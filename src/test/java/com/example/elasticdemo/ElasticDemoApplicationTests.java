package com.example.elasticdemo;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class ElasticDemoApplicationTests {

    @Autowired
    RestHighLevelClient client;
    private final String NEW_INDEX = "new-index";

    /**
     * 이미 해당 인덱스가 존재한다는 가정하에 만들어진 API
     *
     * @throws IOException
     */
    @DisplayName("모든 작업을 시작하기 전 인덱스를 조회 후 삭제하고 결과를 출력한다.")
    @BeforeEach
    void beforeEach() throws IOException {
        //== 인덱스 존재 여부 확인 ==//
        GetIndexRequest getIndexRequest = new GetIndexRequest(NEW_INDEX);
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        //== 조회 후 해당 인덱스가 조회한다면 ==//
        if (exists) {
            System.out.println("new-index 존재");
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(NEW_INDEX);
            boolean acknowledged = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT)
                    .isAcknowledged();
            System.out.println("삭재 완료 여부 : " + acknowledged);
        } else {
            System.out.println("new-index 존재하지 않음.");
        }
    }

    @DisplayName("ElasticSearch 의 cat API 를 이용하여 모든 인덱스 목록을 가져온다.")
    @Test
    void catAPI_indices() throws IOException {
        assertNotNull(client);
        GetIndexRequest allIndexRequest = new GetIndexRequest("*");
        GetIndexResponse response = client.indices().get(allIndexRequest, RequestOptions.DEFAULT);

        Arrays.stream(response.getIndices())
                .forEach(System.out::println);
    }

    @DisplayName("ElasticSearch 에 새로운 Index 생성")
    @Test
    void create_new_index() throws Exception {

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(NEW_INDEX);

        boolean acknowledged = client.indices().create(createIndexRequest, RequestOptions.DEFAULT)
                .isAcknowledged();
        System.out.println("new-index 생성 성공 여부 : " + acknowledged);

        //== INDEX 생성 후 Document 삽입 ==//
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "hope");
        jsonMap.put("Job", "Tech Unit");
        IndexRequest indexRequest = new IndexRequest(NEW_INDEX)
                .id("1")
                .source(jsonMap);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        DocWriteResponse.Result result = indexResponse.getResult();

        System.out.println("Document 삽입 성공 여부 = " + result);

    }

    @DisplayName("ElasticSearch 에서 해당 Index를 통해 Document를 가져온다.")
    @Test
    void get_document() throws Exception {
        //== 인덱스 생성 후 문서 삽입 ==//
        create_new_index();

        GetRequest getRequest = new GetRequest(NEW_INDEX, "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> source = getResponse.getSource();
        long version = getResponse.getVersion();
        System.out.println("version = " + version);
        String type = getResponse.getType();
        System.out.println("type = " + type);
        System.out.println("source = " + source);
    }

    @DisplayName("ElasticSearch 에서 특정 Document를 검색")
    @Test
    void search() throws Exception {
        //== 인덱스 생성 후 문서 삽입 ==//ㅓ
        create_new_index();
        Thread.sleep(1000);
        //== 특정 Index 만 Search 범위로 지정 ==//
        SearchRequest newIndexSearchRequest = new SearchRequest(NEW_INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(
                        matchAllQuery()
                );

        newIndexSearchRequest.source(searchSourceBuilder);

        List<Map<String, Object>> collect = Arrays.stream(client.search(newIndexSearchRequest, RequestOptions.DEFAULT)
                        .getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .collect(Collectors.toList());

        for (Map<String, Object> stringObjectMap : collect) {
            System.out.println(stringObjectMap);
        }
    }

}
