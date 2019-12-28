package com.kaveinga.elasticsearch.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * ElasticSearchConfig
 * 
 * @author folaukaveinga Elastic Search bean is configured in properties files
 *         but basePackage is required
 */
@Configuration
public class LocalElasticSearchConfig extends AbstractFactoryBean<RestHighLevelClient> {

	private Logger log = LoggerFactory.getLogger(this.getClass());
	
	private RestHighLevelClient restHighLevelClient;
	
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
			restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		return restHighLevelClient;
	}

}
