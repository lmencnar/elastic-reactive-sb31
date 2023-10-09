# Reactive Elasticsearch Client Concurrency Test

Version:
- springboot 3.1.4
- elastic data reactive 5.1.4 - using elastic client - not using netty

Requirements:
- elasticsearch 7+ local or remote installation - single node or cluster without SSL
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
- When using SSL the overhead of creating a connection is significant - in the order of 10-50 milliseconds
- the program as is does not handle SSL yet
- When using bulk requests which take hundreds of milliseconds or several seconds the role of http connection pool is lower

Important:
- this test program was created to compare performance and stability with older elastic reactive library 4.4.15 based on netty
- the overall observed execution time with library 5.1.4 is about 5-10% longer (slower)
- no obvious differences in stability, but when timeout occurs it is on connection time while with 4.4.15 it was normally on response time

Open points:
- the client concurrency seems to reach the limit of requests per seconds already at concurrency 3-4
- with that concurrency the more powerful servers (more cores - like 6-8, threads 12-16) still seem to be underutilized - like 10-20% busy
- as of now - not clear what is the bottleneck