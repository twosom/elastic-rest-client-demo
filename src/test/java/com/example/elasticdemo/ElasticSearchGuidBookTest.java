package com.example.elasticdemo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@ActiveProfiles("home")
@SpringBootTest
public class ElasticSearchGuidBookTest {

    @Value("${elastic.host}")
    private String HOST;

    @Value("${elastic.port}")
    private int PORT;

    RestHighLevelClient client;


    @BeforeEach
    void beforeEach() {
        client = createClient(HOST, PORT);
    }


    @AfterEach
    void afterEach() {
        try {
            client.close();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private RestHighLevelClient createClient(String host, int port) {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port, "http")
                )
        );
    }


    @DisplayName("search API 학습을 위한 데이터 Bulk API")
    @Test
    void bulk_api_for_search_api() throws Exception {

        GetIndexRequest getIndexRequest = new GetIndexRequest("test_data");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (exists) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("test_data");
            boolean result = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT).isAcknowledged();
            assertTrue(result);
        }

        BulkRequest bulkRequest = new BulkRequest();
        addSampleDataToBulkRequest(bulkRequest);
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    @DisplayName("간단한 Search API 사용")
    @Test
    void simple_search_api() throws Exception {
        GetIndexRequest getIndexRequest = new GetIndexRequest("test_data");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (!exists) {
            bulk_api_for_search_api();
        }

        SearchRequest searchRequest = new SearchRequest("test_data");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                QueryBuilders.termQuery("title", "elasticsearch")
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponseSections internalResponse = client.search(searchRequest, RequestOptions.DEFAULT)
                .getInternalResponse();

        List<Map<String, Object>> result = Arrays.stream(internalResponse
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .collect(Collectors.toList());

        assertFalse(result.isEmpty());
        for (Map<String, Object> stringObjectMap : result) {
            System.out.println(stringObjectMap);
        }
    }







    private void addSampleDataToBulkRequest(BulkRequest bulkRequest) {
        bulkRequest.add(addData("{ \"title\": \"ElasticSearch Training Book\", \"publisher\": \"insight\", \"ISBN\": \"9788966264849\", \"release_date\": \"2020/09/30\", \"description\": \"ElasticSearch is cool open source search engine\" }\n", "1"));
        bulkRequest.add(addData("{ \"title\" : \"Kubernetes: Up and Running\", \"publisher\": \"O'Reilly Media, Inc.\", \"ISBN\": \"9781491935675\", \"release_date\": \"2017/09/03\", \"description\" : \"What separates the traditional enterprise from the likes of Amazon, Netflix, and Etsy? Those companies have refined the art of cloud native development to maintain their competitive edge and stay well ahead of the competition. This practical guide shows Java/JVM developers how to build better software, faster, using Spring Boot, Spring Cloud, and Cloud Foundry.\" }\n", "2"));
        bulkRequest.add(addData("{ \"title\" : \"Cloud Native Java\", \"publisher\": \"O'Reilly Media, Inc.\", \"ISBN\": \"9781449374648\", \"release_date\": \"2017/08/04\", \"description\" : \"What separates the traditional enterprise from the likes of Amazon, Netflix, and Etsy? Those companies have refined the art of cloud native development to maintain their competitive edge and stay well ahead of the competition. This practical guide shows Java/JVM developers how to build better software, faster, using Spring Boot, Spring Cloud, and Cloud Foundry.\" }\n", "3"));
        bulkRequest.add(addData("{ \"title\" : \"Learning Chef\", \"publisher\": \"O'Reilly Media, Inc.\", \"ISBN\": \"9781491944936\", \"release_date\": \"2014/11/08\", \"description\" : \"Get a hands-on introduction to the Chef, the configuration management tool for solving operations issues in enterprises large and small. Ideal for developers and sysadmins new to configuration management, this guide shows you to automate the packaging and delivery of applications in your infrastructure. You’ll be able to build (or rebuild) your infrastructure’s application stack in minutes or hours, rather than days or weeks.\" }\n", "4"));
        bulkRequest.add(addData("{ \"title\" : \"Elasticsearch Indexing\", \"publisher\": \"Packt Publishing\", \"ISBN\": \"9781783987023\", \"release_date\": \"2015/12/22\", \"description\" : \"Improve search experiences with ElasticSearch’s powerful indexing functionality – learn how with this practical ElasticSearch tutorial, packed with tips!\" }\n", "5"));
        bulkRequest.add(addData("{ \"title\" : \"Hadoop: The Definitive Guide, 4th Edition\", \"publisher\": \"O'Reilly Media, Inc.\", \"ISBN\": \"9781491901632\", \"release_date\": \"2015/04/14\", \"description\" : \"Get ready to unlock the power of your data. With the fourth edition of this comprehensive guide, you’ll learn how to build and maintain reliable, scalable, distributed systems with Apache Hadoop. This book is ideal for programmers looking to analyze datasets of any size, and for administrators who want to set up and run Hadoop clusters.\" }\n", "6"));
        bulkRequest.add(addData("{ \"title\": \"Getting Started with Impala\", \"publisher\": \"O'Reilly Media, Inc.\", \"ISBN\": \"9781491905777\", \"release_date\": \"2014/09/14\", \"description\" : \"Learn how to write, tune, and port SQL queries and other statements for a Big Data environment, using Impala—the massively parallel processing SQL query engine for Apache Hadoop. The best practices in this practical guide help you design database schemas that not only interoperate with other Hadoop components, and are convenient for administers to manage and monitor, but also accommodate future expansion in data size and evolution of software capabilities. Ideal for database developers and business analysts, the latest revision covers analytics functions, complex types, incremental statistics, subqueries, and submission to the Apache incubator.\" }\n", "7"));
        bulkRequest.add(addData("{ \"title\": \"NGINX High Performance\", \"publisher\": \"Packt Publishing\", \"ISBN\": \"9781785281839\", \"release_date\": \"2015/07/29\", \"description\": \"Optimize NGINX for high-performance, scalable web applications\" }\n", "8"));
        bulkRequest.add(addData("{ \"title\": \"Mastering NGINX - Second Edition\", \"publisher\": \"Packt Publishing\", \"ISBN\": \"9781782173311\", \"release_date\": \"2016/07/28\", \"description\": \"An in-depth guide to configuring NGINX for your everyday server needs\" }\n", "9"));
        bulkRequest.add(addData("{ \"title\" : \"Linux Kernel Development, Third Edition\", \"publisher\": \"Addison-Wesley Professional\", \"ISBN\": \"9780672329463\", \"release_date\": \"2010/06/09\", \"description\" : \"Linux Kernel Development details the design and implementation of the Linux kernel, presenting the content in a manner that is beneficial to those writing and developing kernel code, as well as to programmers seeking to better understand the operating system and become more efficient and productive in their coding.\" }\n", "10"));
        bulkRequest.add(addData("{ \"title\" : \"Linux Kernel Development, Second Edition\", \"publisher\": \"Sams\", \"ISBN\": \"9780672327209\", \"release_date\": \"2005/01/01\", \"description\" : \"The Linux kernel is one of the most important and far-reaching open-source projects. That is why Novell Press is excited to bring you the second edition of Linux Kernel Development, Robert Love's widely acclaimed insider's look at the Linux kernel. This authoritative, practical guide helps developers better understand the Linux kernel through updated coverage of all the major subsystems as well as new features associated with the Linux 2.6 kernel. You'll be able to take an in-depth look at Linux kernel from both a theoretical and an applied perspective as you cover a wide range of topics, including algorithms, system call interface, paging strategies and kernel synchronization. Get the top information right from the source in Linux Kernel Development.\" }", "11"));
    }

    private IndexRequest addData(String s, String id) {
        return createIndexRequestForBulkRequest(id)
                .source(s,
                        XContentType.JSON);
    }

    private IndexRequest createIndexRequestForBulkRequest(String id) {
        return new IndexRequest("test_data")
                .id(id);
    }


}
