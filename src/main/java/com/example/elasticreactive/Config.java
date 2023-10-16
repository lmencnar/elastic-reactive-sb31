package com.example.elasticreactive;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchClients;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchConfiguration;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;


@Configuration
@Slf4j
public class Config extends ReactiveElasticsearchConfiguration {

    @Value("${es.protocol:http}")
    private String esProtocol;

    @Value("${es.host}")
    private String esHost;

    @Value("${es.port}")
    private Integer esPort;

    @Value("${es.trust_store_file:#{null}}")
    private String esTrustStoreFile;

    @Value("${es.trust_store_password_file:#{null}}")
    private String esTrustStorePasswordFile;

    @Value("${es.user_name:#{null}}")
    private String esUserName;

    @Value("${es.user_password_file:#{null}}")
    private String esUserPasswordFile;

    @Value("${es.connect_timeout_millis}")
    private Integer esConnectTimeoutMillis;

    @Value("${es.response_timeout_millis}")
    private Integer esResponseTimeoutMillis;

    @Value("${es.max_per_route_connections}")
    private Integer esMaxPerRouteConnections;

    @Value("${es.max_conn_total}")
    private Integer esMaxConnTotal;

    @Value("${es.io_reactor_thread_count}")
    private Integer esIOReactorThreadCount;

    @Value("${es.max_bulk_size}")
    private Integer esMaxBulkSize;

    @Override
    public ClientConfiguration clientConfiguration() {

        ClientConfiguration.TerminalClientConfigurationBuilder configurationBuilder;
        if ("http".equals(esProtocol)) {
            configurationBuilder = ClientConfiguration.builder()
                    .connectedTo(esHost + ":" + esPort);
        } else {
            configurationBuilder = ClientConfiguration.builder()
                    .connectedTo(esHost + ":" + esPort)
                    .usingSsl();
        }

        return configurationBuilder.withConnectTimeout(Duration.ofMillis(esConnectTimeoutMillis))
                .withSocketTimeout(Duration.ofMillis(esResponseTimeoutMillis))
                .withClientConfigurer(ElasticsearchClients.ElasticsearchHttpClientConfigurationCallback.from(httpAsyncClientBuilder -> {
                    httpAsyncClientBuilder
                            .setMaxConnPerRoute(esMaxPerRouteConnections)
                            .setMaxConnTotal(esMaxConnTotal)
                            .setConnectionTimeToLive(5, TimeUnit.MINUTES)
                            .setDefaultIOReactorConfig(
                                    buildIOReactorConfig()
                            );
                    if(esTrustStoreFile != null && esTrustStorePasswordFile != null) {
                        try {
                            httpAsyncClientBuilder.setSSLContext(createSSLContext());
                        } catch (Exception exc) {
                            log.error("failed to build SSLContext", exc);
                        }
                    }

                    if(esUserName != null && esUserPasswordFile != null) {
                        try {
                            final CredentialsProvider credentialsProvider =
                                    new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY,
                                    new UsernamePasswordCredentials(esUserName, loadPassword(esUserPasswordFile)));
                            httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        } catch (Exception exc) {
                            log.error("failed to set up basic auth with user password", exc);
                        }
                    }
                    return httpAsyncClientBuilder;
                }))
                .build();
    }

    protected IOReactorConfig buildIOReactorConfig() {

        return IOReactorConfig
                .copy(IOReactorConfig.DEFAULT)
                .setIoThreadCount(esIOReactorThreadCount)
                /*
                .setSoTimeout(60000)
                .setConnectTimeout(0)
                .setTcpNoDelay(true)
                .setSoLinger(-1)
                .setSoReuseAddress(false)
                .setInterestOpQueued(false)
                .setSelectInterval(1000)
                 */
                .build();
    }

    private SSLContext createSSLContext()
            throws IOException, KeyStoreException, NoSuchAlgorithmException,
            CertificateException, KeyManagementException {

        KeyStore truststore = KeyStore.getInstance("pkcs12");
        String trustStorePassword = loadPassword(esTrustStorePasswordFile);
        try (InputStream is = Files.newInputStream(Paths.get(esTrustStoreFile))) {
            truststore.load(is, trustStorePassword.toCharArray());
        }
        SSLContextBuilder sslBuilder = SSLContexts.custom()
                .loadTrustMaterial(truststore, null);

        return sslBuilder.build();
    }

    private String loadPassword(String fileName) throws IOException {

        return Files.readAllLines(Paths.get(fileName)).get(0);
    }
}
