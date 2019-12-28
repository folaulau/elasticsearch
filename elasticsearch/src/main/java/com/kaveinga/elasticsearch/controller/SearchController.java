package com.kaveinga.elasticsearch.controller;

import java.net.URI;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.BufferingClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kaveinga.elasticsearch.utility.HttpRequestInterceptor;

import io.swagger.annotations.Api;

@Api(value = "searchs", produces = "Rest API for search operations", tags = "Search Controller")
@RestController
@RequestMapping("/searchs")
public class SearchController {

	private Logger log = LoggerFactory.getLogger(this.getClass());
	
	private RestTemplate restTemplate = new RestTemplate(
			new BufferingClientHttpRequestFactory(new SimpleClientHttpRequestFactory()));
	
	@PostConstruct
	public void init() {
		restTemplate.getInterceptors().add(new HttpRequestInterceptor());
	}
	
	@GetMapping("/doctors")
	public ResponseEntity<ObjectNode> searchDoctors(){
		
		
		HttpHeaders headers = new HttpHeaders();

		headers.setContentType(MediaType.APPLICATION_JSON);

		HttpEntity<String> entity = new HttpEntity<>(new String(), headers);

		ResponseEntity<ObjectNode> response = null;
		
		try {
			
			response = restTemplate.exchange(
					new URI("http://localhost:9200/doctors/_search"), HttpMethod.GET, entity,
					ObjectNode.class);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new ResponseEntity<>(response.getBody(), HttpStatus.OK);
	}
}
