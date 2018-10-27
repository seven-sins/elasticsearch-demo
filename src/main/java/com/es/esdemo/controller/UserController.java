package com.es.esdemo.controller;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

import com.es.esdemo.utils.EsUtils;

@RestController
public class UserController {
	private static final String INDEX = "web";

	@GetMapping("/user")
	public Object query() throws UnknownHostException {
        TransportClient client = EsUtils.getClient();
        // 搜索数据
        GetResponse response = client.prepareGet(INDEX, "user", "10").execute().actionGet();
        // 输出结果
        String result = response.getSourceAsString();
        System.out.println(result);
        // 关闭client
        client.close();

		return result;
	}
	
	@PostMapping("/user")
	public Object add() throws IOException {
		TransportClient client = EsUtils.getClient();
		XContentBuilder doc = XContentFactory.jsonBuilder()
								.startObject()
								.field("id", "1")
								.field("username", "king")
								.field("age", 18)
								.field("sex", 1)
								.field("pwd", "123456")
								.endObject();
		// 添加文档
		IndexResponse response = client.prepareIndex(INDEX, "user", "10").setSource(doc).get();
		
		return response.status();
	}
	
	@DeleteMapping("/user")
	public Object delete() throws IOException {
		TransportClient client = EsUtils.getClient();
		
		// 删除文档
		DeleteResponse response = client.prepareDelete(INDEX, "user", "10").get();
		
		return response.status();
	}
	
	@PutMapping("/user")
	public Object update() throws IOException, InterruptedException, ExecutionException {
		TransportClient client = EsUtils.getClient();
		
		// 更新文档
		UpdateRequest request = new UpdateRequest();
		request.index(INDEX).type("user").id("10").doc(XContentFactory.jsonBuilder().startObject().field("age", 19).endObject());
		
		UpdateResponse response = client.update(request).get();
		
		return response.status();
	}

	@PostMapping("/user/upsert")
	public Object insert() throws IOException, InterruptedException, ExecutionException {
		TransportClient client = EsUtils.getClient();
		
		// 数据存在更新， 不存在创建
		IndexRequest request = new IndexRequest();
		request.index(INDEX).type("user").id("10").source(XContentFactory.jsonBuilder()
								.startObject()
								.field("id", "1")
								.field("username", "king")
								.field("age", 18)
								.field("sex", 1)
								.field("pwd", "123456")
								.endObject());
		
		UpdateRequest request2 = new UpdateRequest(INDEX, "user", "10")
							.doc(XContentFactory.jsonBuilder().field("sex", 0).endObject()).upsert(request);
		
		UpdateResponse response = client.update(request2).get();
		
		return response.status();
	}
	
	/**
	 * 批量查询
	 * @return
	 */
	@GetMapping("/user/find")
	public Object find() {
		TransportClient client = EsUtils.getClient();
		MultiGetResponse response = client.prepareMultiGet()
									.add(INDEX, "user", "10", "7", "3")
									.add(INDEX, "role", "1")
									.get();
		for(MultiGetItemResponse item: response) {
			GetResponse gr = item.getResponse();
			if(gr != null && gr.isExists()) {
				System.out.println(gr.getSourceAsString());
			}
		}
		
		return response.getResponses();
	}
	
	@PostMapping("/user/batch")
	public Object batchInsert() throws IOException {
		TransportClient client = EsUtils.getClient();
		BulkRequestBuilder bulkBuild = client.prepareBulk();
		
		bulkBuild.add(client.prepareIndex(INDEX, "user", "3")
				.setSource(XContentFactory.jsonBuilder()
						.startObject()
						.field("id", "3")
						.field("username", "girl")
						.field("age", 17)
						.endObject()
					));
		bulkBuild.add(client.prepareIndex(INDEX, "user", "7")
				.setSource(XContentFactory.jsonBuilder()
						.startObject()
						.field("id", "7")
						.field("username", "bee")
						.field("age", 17)
						.endObject()
					));
		
		BulkResponse response = bulkBuild.get();
		if(response.hasFailures()) {
			System.out.println("failures");
		}
		
		return response.status();
	}
	
	@DeleteMapping("/user/batch")
	public Object batchDelete() throws IOException {
		TransportClient client = EsUtils.getClient();
		
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
										.newRequestBuilder(client)
										.filter(QueryBuilders.matchQuery("username", "be"))
										.source(INDEX)
										.get();
		
		long counts = response.getDeleted();
		
		return counts;
	}
	
	@GetMapping("/user/query1")
	public Object query1() throws IOException {
		TransportClient client = EsUtils.getClient();
		
		QueryBuilder qb = QueryBuilders.matchAllQuery();
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(qb)
							.setSize(3)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	@GetMapping("/user/query2")
	public Object query2() throws IOException {
		TransportClient client = EsUtils.getClient();
		
		QueryBuilder qb = QueryBuilders.matchQuery("username", "bee");
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(qb)
							.setSize(3)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	@GetMapping("/user/query3")
	public Object query3() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 查询username和pwd字段含有bee的数据(必须与bee一致)
		QueryBuilder qb = QueryBuilders.multiMatchQuery("bee", "username", "pwd");
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(qb)
							.setSize(3)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	/**
	 * 范围查询
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/user/query4")
	public Object query4() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 查询
		// QueryBuilder qb = QueryBuilders.rangeQuery("birthday").from("1990-01-01").to("2000-01-01").format("yyyy-MM-dd);
		QueryBuilder qb = QueryBuilders.rangeQuery("age").from("10").to("21");
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(qb)
							.setSize(3)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	/**
	 * 前缀查询
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/user/query5")
	public Object query5() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 查询
		QueryBuilder qb = QueryBuilders.prefixQuery("username", "b");
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(qb)
							.setSize(3)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	/**
	 * 模糊查询
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/user/query6")
	public Object query6() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 查询
		QueryBuilder qb = QueryBuilders.wildcardQuery("username", "*e*");
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(qb)
							.setFrom(1)
							.setSize(3)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	/**
	 * 模糊查询
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/user/query7")
	public Object query7() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 查询
		QueryBuilder qb = QueryBuilders.fuzzyQuery("username", "bey"); // 不用太精确， 跟目标差不多也能查到
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(qb)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	/**
	 * 类型查询
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/user/query8")
	public Object query8() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 查询
		QueryBuilder qb = QueryBuilders.typeQuery("user");
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(qb)
							.setFrom(0)
							.setSize(3)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	/**
	 * 根据ID查询
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/user/query9")
	public Object query9() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 根据ID查询
		QueryBuilder qb = QueryBuilders.idsQuery().addIds("3", "7");
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(qb)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	@GetMapping("/user/query10")
	public Object query10() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 
		QueryBuilder qb = QueryBuilders.termQuery("username", "king");
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(qb)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	/**
	 * 多条件 and
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/user/query11")
	public Object query11() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		
		boolQueryBuilder.must(QueryBuilders.wildcardQuery("username", "kin*"));
		boolQueryBuilder.must(QueryBuilders.wildcardQuery("pwd", "*456"));
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(boolQueryBuilder)
							.setFrom(0)
							.setSize(3)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	/**
	 * 多条件模糊查询
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/user/query12")
	public Object query12() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		
		boolQueryBuilder.should(QueryBuilders.wildcardQuery("username", "kin*"));
		boolQueryBuilder.should(QueryBuilders.wildcardQuery("username", "be*"));
		
		SearchResponse sr = client.prepareSearch(INDEX)
							.setQuery(boolQueryBuilder)
							.setFrom(0)
							.setSize(3)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
	
	@GetMapping("/user/query13")
	public Object query13() throws IOException {
		TransportClient client = EsUtils.getClient();
		// 
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		
		boolQueryBuilder.should(QueryBuilders.matchPhraseQuery("username", "*be*"));
		boolQueryBuilder.should(QueryBuilders.matchPhraseQuery("pwd", "*123*"));
		
		SearchResponse sr = client.prepareSearch("index1", "index2")
							.setQuery(boolQueryBuilder)
							.setTypes("type1", "type2")
							.setFrom(0)
							.setSize(3)
							.get();
		SearchHits hits = sr.getHits();
		for(SearchHit hit: hits) {
			System.out.println(hit.getSourceAsString());
		}
		
		return hits.getTotalHits();
	}
}





















