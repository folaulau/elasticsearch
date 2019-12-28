package com.kaveinga.elasticsearch.service;

import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SearchService {

	private Logger log = LoggerFactory.getLogger(this.getClass());
	
	@Autowired
	private RestHighLevelClient restHighLevelClient;
	
	
}
