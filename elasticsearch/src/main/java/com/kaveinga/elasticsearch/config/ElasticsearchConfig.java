package com.kaveinga.elasticsearch.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AbstractFactoryBean;

import lombok.extern.slf4j.Slf4j;

/**
 * ElasticSearchConfig
 * 
 * @author folaukaveinga
 */

@Slf4j
@Configuration
public class ElasticsearchConfig extends AbstractFactoryBean<RestHighLevelClient> {

    private RestHighLevelClient restHighLevelClient;

    @Value("${elasticsearch.host}")
    private String              clusterNode;

    @Value("${elasticsearch.httptype}")
    private String              clusterHttpType;

    @Value("${elasticsearch.username}")
    private String              username;

    @Value("${elasticsearch.password}")
    private String              password;

    @Value("${elasticsearch.port}")
    private int                 clusterHttpPort;

    @Override
    public void destroy() {
        try {
            if (restHighLevelClient != null) {
                restHighLevelClient.close();
            }
            log.info("ElasticSearch rest high level client closed");
        } catch (final Exception e) {
            log.error("Error closing ElasticSearch client: ", e);
        }
    }

    @Override
    public Class<RestHighLevelClient> getObjectType() {
        return RestHighLevelClient.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public RestHighLevelClient createInstance() {
        return buildClient();
    }

    private RestHighLevelClient buildClient() {
        try {

            // @formatter:off
 
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, 
                       new UsernamePasswordCredentials(username, password));

            // port: 443
            // http: http
            RestClientBuilder restClientBuilder = RestClient
            .builder(new HttpHost(clusterNode, clusterHttpPort, clusterHttpType));
            
            restClientBuilder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
            
            restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {

                @Override
                public Builder customizeRequestConfig(Builder requestConfigBuilder) {
                    // TODO Auto-generated method stub
                    return requestConfigBuilder.setConnectTimeout(1000000).setSocketTimeout(6000000).setConnectionRequestTimeout(300000);
                }

            });
            
            // @formatter:on

            restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return restHighLevelClient;
    }

}
