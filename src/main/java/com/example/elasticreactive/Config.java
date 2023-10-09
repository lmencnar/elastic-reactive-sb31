package com.example.elasticreactive;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.apache.http.HttpHost;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchClients;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchConfiguration;

import java.time.Duration;
import java.util.concurrent.TimeUnit;


@Configuration
public class Config extends ReactiveElasticsearchConfiguration {

    @Value("${es.protocol}")
    private String esProtocol;

    @Value("${es.host}")
    private String esHost;

    @Value("${es.port}")
    private Integer esPort;

    @Value("${es.connect_timeout_millis}")
    private Integer esConnectTimeoutMillis;

    @Value("${es.response_timeout_millis}")
    private Integer esResponseTimeoutMillis;

    @Value("${es.max_per_route_connections}")
    private Integer esMaxPerRouteConnections;

    @Value("${es.max_conn_total}")
    private Integer esMaxConnTotal;

    @Value("${es.max_bulk_size}")
    private Integer esMaxBulkSize;

    @Override
    public ClientConfiguration clientConfiguration() {
        final ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .connectedTo(esHost + ":" + esPort)
                .withConnectTimeout(Duration.ofMillis(esConnectTimeoutMillis))
                .withSocketTimeout(Duration.ofMillis(esConnectTimeoutMillis))
                .withClientConfigurer(ElasticsearchClients.ElasticsearchHttpClientConfigurationCallback.from(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder
                            .setMaxConnPerRoute(esMaxPerRouteConnections)
                            .setMaxConnTotal(esMaxConnTotal)
                            .setConnectionTimeToLive(5, TimeUnit.MINUTES)))
                .build();
        return clientConfiguration;
    }

}
