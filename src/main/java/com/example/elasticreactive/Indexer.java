package com.example.elasticreactive;



import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.indices.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestClient;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@Component
@Slf4j
@RequiredArgsConstructor
public class Indexer {

    @Value("${indexer.max_bulk_active}")
    private Integer indexerMaxBulkActive;

    @Value("${indexer.min_concurrency}")
    private Integer indexerMinConcurrency;

    @Value("${indexer.max_concurrency}")
    private Integer indexerMaxConcurrency;

    @Value("${indexer.batch_size}")
    private Integer indexerBatchSize;

    @Value("${indexer.batch_count}")
    private Integer indexerBatchCount;

    @Value("${indexer.index_name}")
    private String indexerIndexName;

    @Value("${indexer.index_shards}")
    private Integer indexerIndexShards;

    @Value("${indexer.index_replicas}")
    private Integer indexerIndexReplicas;

    @Value("${indexer.index_refresh_interval}")
    private String indexerIndexRefreshInterval;

    @Autowired
    ReactiveElasticsearchClient elasticsearchClient;

    @Autowired
    RestClient restClient;

    @Autowired
    private PersonGenerator personGenerator;


    private final ObjectMapper objectMapper;

    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private final Timer indexTimer = registry.timer("es.timer");
    private final LongAdder concurrent = registry.gauge("es.concurrent", new LongAdder());
    private final Counter successes = registry.counter("es.index", "result", "success");
    private final Counter failures = registry.counter("es.index", "result", "failure");


    // use semaphore as blocker against too high concurrency
    // normally controlling the concurrency would be enough
    // but semaphore is a brute force defence to make sure that no matter how many worker threads try to send data
    // no more than max bulk operations are in progress
    // this way there is less chance they do timeout
    // another limit on asynch concurrency is number of connections in WebClient pool
    // but blocking on this limit may cause timeouts so better control in the application code
    private Semaphore available;

    private Flux<BulkResponse> indexMany(int batchSize, int batchCount, int concurrency) {
        log.info("indexMany concurrency={}", concurrency);
        return personGenerator
                .infinite()
                .take(batchSize)
                .collectList()
                .repeat()
                .take(batchCount)
                // .flatMap(docs -> indexManyDocSwallowErrors(docs), concurrency);
                .flatMap(docs -> countConcurrent(measure(indexManyDocSwallowErrors(docs))), concurrency);
    }

    private Mono<BulkResponse> indexManyDocSwallowErrors(List<Doc> docs) {
        final long startTime = System.currentTimeMillis();
        return indexManyDocs(docs)
                .doOnSuccess(response -> {
                    available.release();
                    successes.increment();
                    long duration = System.currentTimeMillis() - startTime;
                    log.debug("success reactive bulk after {}", duration);
                    if (duration > 1000) {
                        log.warn("success reactive bulk after long time {}", duration);
                    }
                })
                .doOnError(e -> {
                    available.release();
                    log.error("Unable to index after {}", System.currentTimeMillis() - startTime, e);
                })
                .doOnError(e -> failures.increment())
                .onErrorResume(e -> Mono.empty());
    }

    private Mono<BulkResponse> indexManyDocs(List<Doc> docs) {

        try {
            BulkRequest.Builder bulkRequestBuilder = new BulkRequest.Builder();

            docs.stream().forEach(doc ->
                    bulkRequestBuilder.operations(op -> op
                            .index(idx -> {
                                        try {
                                            return idx
                                                    .index(indexerIndexName)
                                                    .id(doc.getUsername())
                                                    .document(objectMapper.readValue(doc.getJson(), JsonNode.class)
                                                    );
                                        } catch (JsonProcessingException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                            )));
            available.acquire();
            log.debug("calling reactive bulk");
            return elasticsearchClient.bulk(bulkRequestBuilder.build());
        } catch (InterruptedException exc) {
            log.error("interrupted ", exc);
        }
        return Mono.empty();
    }

    private <T> Mono<T> countConcurrent(Mono<T> input) {
        return input
                .doOnSubscribe(s -> concurrent.increment())
                .doOnTerminate(concurrent::decrement);
    }

    private <T> Mono<T> measure(Mono<T> input) {
        return Mono
                .fromCallable(System::currentTimeMillis)
                .flatMap(time ->
                        input.doOnSuccess(x -> {
                            long duration = System.currentTimeMillis() - time;
                            log.debug("took {}", duration);
                            indexTimer.record(duration, TimeUnit.MILLISECONDS);
                        })
                );
    }

    private void createIndex() {

        Mono<CreateIndexResponse> createIndexResponseMono = elasticsearchClient.indices().create(indexBuilder ->
                indexBuilder
                        .index(indexerIndexName)
                        .settings(settingsBuilder -> settingsBuilder
                                .numberOfShards(indexerIndexShards.toString())
                                .numberOfReplicas(indexerIndexReplicas.toString())
                                .refreshInterval(timeBuilder -> timeBuilder.time(indexerIndexRefreshInterval))
                        ));

        createIndexResponseMono
                .doOnSuccess(consumer ->
                        log.info("index created {}", consumer.acknowledged()))
                .doOnError(consumer ->
                        log.error("create index failed {}", consumer.getMessage())
        );
    }

    private void deleteIndex() {

        Mono<DeleteIndexResponse> deleteIndexResponseMono = elasticsearchClient.indices().delete(indexBuilder ->
                indexBuilder.index(indexerIndexName));

        deleteIndexResponseMono
                .doOnSuccess(consumer ->
                        log.info("index deleted {}", consumer.acknowledged()))
                .doOnError(consumer ->
                        log.error("delete index failed {}", consumer.getMessage())
                );
    }

    @PostConstruct
    void startIndexing() {
        available = new Semaphore(indexerMaxBulkActive, true);

        deleteIndex();
        createIndex();

        long startTime = System.currentTimeMillis();
        Flux
                .range(indexerMinConcurrency, indexerMaxConcurrency)
                .concatMap(concurrency -> indexMany(indexerBatchSize, indexerBatchCount, concurrency))
                .window(Duration.ofSeconds(1))
                .flatMap(Flux::count)
                .subscribe(winSize -> log.info(
                        "Got responses/sec={} elapsed from start sec {}",
                        winSize,
                        (System.currentTimeMillis() - startTime)/1000.0));
    }
}