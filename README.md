# Reactive Elasticsearch Client Concurrency Test

Credits:
- Initial version based on article https://nurkiewicz.com/2018/01/spring-reactor-and-elasticsearch.html
- Initial version based on source code https://github.com/nurkiewicz/elastic-flux

Major dependencies:
- springboot 3.1.4
- elastic data reactive 5.1.4 - using elastic client - not using netty

Requirements:
- elasticsearch 7+ local or remote installation - single node or cluster - tested only with single node
- optional SSL and basic security
- java 17 or similar 

The program generates test data (person objects) and pushes them using 
bulk asynchronous operations in a growing number of concurrent threads.

The concurrency as configured grows from 1 to 30.
The size of the bulk request can be configured.
The number of bulk requests for each of the concurrency tests can be configured.
The number of connections in the pool can be configured.

Additionally, the concurrency can be limited by a semaphore guarding the bulk asynch request alone.

The timeouts can be configured.

It was observed running on 6 core 12 threads desktop class machine with local elastic as downloaded with any settings changes.
With settings as configured the usual bulk executes within 200-300 milliseconds.

But with growing concurrency some operations are stuck and complete after longer than 1 second.

Also occasional timeouts are observed over 20 seconds.
Those are reported as exceptions - normally observed as connection timeouts.

Observed "tuning" effects of index settings:
- lower number of shards - ideally 1 - improve indexing performance - too high number of shards, above physical and os capacity (like 10) significantly 
  lower the indexing performance - like from 270 seconds total execution to 600 seconds and even leads to response timeouts
- higher number of shards (up to a point) should improve concurrent search performance - not tested
- high refresh interval - slightly improves indexing performance - like total execution takes 250 seconds vs 270 seconds,
  conclusion is that increasing this time from 1 second is good but it does not make much of a difference for it to be more than 30 or 60 seconds

Comments on connection timeouts and keep alive:
- HTTP REST normally would open new connection for every request, protocol is stateless
- as configured 30 max connections per route
- connection pooling implemented by elastic library with apache asynch client
- When using SSL the overhead of creating a connection is significant - in the order of 10-50 milliseconds
- When using bulk requests which take hundreds of milliseconds or several seconds the role of http connection pooling is lower

Important:
- this test program was created to compare performance and stability with older elastic reactive library 4.4.15 based on netty
- the overall observed execution time with library 5.1.4 is about 5-10% longer (slower)
- no obvious differences in stability, but when timeout occurs it is on connection time while with 4.4.15 it was normally on response time

Closing notes:
- tested with 6 core client and 8 core server - desktop class machines
- below comments assume similar hosting and configuration of volume and timeouts as by default
- no exception timeouts but reaching 90% of CPU utilization on server
- the concurrency above 10 hardly helps increase the rate of processed requests
- when response timeout configured around 5 seconds - some requests do timeout when server is under stress
- with enabled SSL the total execution time is about 20% longer (145 seconds vs 115 seconds)