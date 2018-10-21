package com.es.esdemo.controller;

import org.elasticsearch.client.transport.TransportClient;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.es.esdemo.utils.EsUtils;

@RestController
public class IndexController {
	private static final String INDEX = "web";
	
	@PostMapping("/index/add")
	public Object add() {
		TransportClient client = EsUtils.getClient();
		client.admin().indices().prepareCreate(INDEX).get();
		
		return 1;
	}
	
	@DeleteMapping("/index/delete")
	public Object delete() {
		TransportClient client = EsUtils.getClient();
		client.admin().indices().prepareDelete(INDEX).get();
		
		return 1;
	}
}
