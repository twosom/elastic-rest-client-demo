package com.example.elasticdemo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@ActiveProfiles("home")
@SpringBootTest
public class ElasticSearchGuidBookTest {

    public static final String CCTV_DATA = "cctv-data";
    public static final String NEW_CCTV_DATA = "new-cctv-data";
    public static final String TEST_DATA = "test_data";
    @Value("${elastic.host}")
    private String HOST;

    @Value("${elastic.port}")
    private int PORT;

    RestHighLevelClient client;

    private static AtomicLong id = new AtomicLong(1);


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

    @DisplayName("검색 학습을 위한 BulkAPI 사용1")
    @Test
    void create_index_using_bulk_index_api1() throws Exception {
        removeIndexIfExists(CCTV_DATA);
        createCctvIndex();
        List<Map<String, Object>> records = getObjectFromResourceFile();
        BulkRequest bulkRequest = new BulkRequest();


        int i = 0;
        for (Map<String, Object> record : records) {
            IndexRequest indexRequest = new IndexRequest(CCTV_DATA);
            indexRequest.id(String.valueOf(id.getAndAdd(1)))
                    .source(record);
            bulkRequest.add(indexRequest);
            i++;
            if (i >= 10000) {
                i = 0;
                long count = Arrays.stream(client.bulk(bulkRequest, RequestOptions.DEFAULT)
                                .getItems())
                        .filter(e -> e.getOpType().equals(DocWriteRequest.OpType.INDEX))
                        .count();
                System.out.println(count + " 개의 문서가 인덱싱 되었습니다.");
                bulkRequest = new BulkRequest();
            }
        }

        long count = Arrays.stream(client.bulk(bulkRequest, RequestOptions.DEFAULT)
                        .getItems())
                .filter(e -> e.getOpType().equals(DocWriteRequest.OpType.INDEX))
                .count();
        System.out.println(count + " 개의 문서가 인덱싱 되었습니다.");
        id.set(1);
    }


    @DisplayName("geo_point 사용을 위한 재인덱싱")
    @Test
    void reindex_for_geo_point() throws Exception {
        removeIndexIfExists(NEW_CCTV_DATA);
        // TODO 만약 인덱스가 없으면 생성
        if (!isExistIndex(CCTV_DATA)) create_index_using_bulk_index_api1();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(NEW_CCTV_DATA);


        //TODO 기존의 longitude, latitude 에서 geo_point 를 사용허기 위한 사전 Mapping 작업
        XContentBuilder builder = XContentFactory.jsonBuilder();
        mappingLocationForReindex(builder);
        createIndexRequest.mapping(builder);
        boolean isCreated = client.indices().create(createIndexRequest, RequestOptions.DEFAULT).isAcknowledged();
        Assertions.assertTrue(isCreated);


        ReindexRequest reindexRequest = new ReindexRequest();
        Script script = new Script(ScriptType.INLINE, "painless",
                "ctx._source.location = ['lat' : ctx._source.latitude, 'lon' : ctx._source.longitude];" +
                        "ctx._source.remove('longitude');" +
                        "ctx._source.remove('latitude');", new HashMap<>());

        reindexRequest.setSourceIndices(CCTV_DATA)
                .setDestIndex(NEW_CCTV_DATA)
                .setScript(script);

        BulkByScrollResponse reindexResponse = client.reindex(reindexRequest, RequestOptions.DEFAULT);
        System.out.println("reindexResponse = " + reindexResponse);
    }


    @DisplayName("검색 학습을 위한 BulkAPI 사용2")
    @Test
    void create_index_using_bulk_api2() throws Exception {

        removeIndexIfExists(TEST_DATA);


        InputStream inputStream = new ClassPathResource("sample09-1.json").getInputStream();
        List<Map<String, Object>> sampleDataList = new ObjectMapper().readValue(inputStream, new TypeReference<List<Map<String, Object>>>() {
        });

        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, Object> source : sampleDataList) {
            IndexRequest indexRequest = new IndexRequest(TEST_DATA)
                    .id(String.valueOf(id.getAndAdd(1)))
                    .source(source);

            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        Assertions.assertTrue(Arrays.stream(bulkResponse.getItems()).findAny().isPresent());

    }


    @DisplayName("from/size 를 이용한 검색")
    @Test
    void search_with_from_size() throws Exception {
        if (isExistIndex(TEST_DATA)) create_index_using_bulk_api2();

        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                        termQuery("publisher", "media")
                )
                .from(0)
                .size(3);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
    }


    @DisplayName("sort 옵션을 활용한 검색")
    @Test
    void search_with_sort() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(
                        termQuery("title", "nginx")
                )
                //TODO sort 옵션은 text 필드가 아닌 keyword 나 integer 와 같이 not analyzed 가 기본인 필드를 기준으로 해야 한다.
                .sort("ISBN.keyword", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
    }


    @DisplayName("source 옵션을 활용한 검색")
    @Test
    void search_with_source() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                        termQuery("title", "nginx")
                )
                .fetchSource(new String[]{"title", "description"}, new String[]{"publisher", "ISBN", "release_date"});

        searchRequest.source(searchSourceBuilder);


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
    }

    @DisplayName("highlighting 옵션을 활용한 검색")
    @Test
    void search_with_highlighting() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //TODO 검색 결과 중에 어떤 부분이 쿼리문과 일치하여 검색되었는지 궁금할 경우 사용.
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitle =
                new HighlightBuilder.Field("title");
        highlightBuilder.field(highlightTitle);



        searchSourceBuilder.query(
                termQuery("title", "nginx")
        ).highlighter(highlightBuilder);

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }

    }

    @DisplayName("boost 옵션을 활용한 검색")
    @Test
    void search_with_boost() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest1 = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
        searchSourceBuilder1.query(
                matchQuery("title", "nginx")
                        .boost(4)
        );

        searchRequest1.source(searchSourceBuilder1);

        SearchResponse searchResponse1 = client.search(searchRequest1, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult1 = extractResultList(searchResponse1);
        for (Map<String, Object> result : searchResult1) {
            System.out.println(result);
        }


        SearchRequest searchRequest2 = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        searchSourceBuilder2.query(
                termQuery("title", "nginx")
                        .boost(4)
        );


        searchRequest2.source(searchSourceBuilder2);
        SearchResponse searchResponse2 = client.search(searchRequest2, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult2 = extractResultList(searchResponse2);
        for (Map<String, Object> result : searchResult2) {
            System.out.println(result);
        }
    }


    @DisplayName("scroll api를 통한 예제")
    @Test
    void search_with_scroll() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                matchQuery("title", "nginx")
        ).size(1);

        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1));

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        System.out.println("scrollID : " + scrollId);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }


        //== 위에서 발급받은 scrollId 로 페이징 처리 ==//
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
        searchScrollRequest.scroll(TimeValue.timeValueMinutes(1));

        SearchResponse scrollResponse = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
        scrollId = scrollResponse.getScrollId();
        System.out.println("scrollId : " + scrollId);
        searchResult = extractResultList(scrollResponse);
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
    }


    @DisplayName("match 쿼리 사용")
    @Test
    void search_with_match_query() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // TODO /match 쿼리는 queryContext 중에서도 가장 많이 사용되는 쿼리.
        // TODO /검색어로 들어온 문자열을 analyzer 를 통해 분석한 후 inverted index 에서 해당 문자열의 토큰을 가지고 있는 문서를 검색.
        // TODO /문서의 해당 필드에 설정해 놓은 analyzer 를 기본으로 사용하며, 별도의 analyzer 를 사용할 때는 직접 명시해 주면 된다.
        searchSourceBuilder.query(
                matchQuery("description", "nginx guide")
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
    }

    private List<Map<String, Object>> extractResultList(SearchResponse searchResponse) {
        return Arrays.stream(searchResponse.getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .collect(Collectors.toList());
    }


    private void mappingLocationForReindex(XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("location");
                {
                    builder.field("type", "geo_point");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
    }


    private void createCctvIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(CCTV_DATA);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        //TODO 사전 MAPPING 작업
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("longitude");
                {
                    builder.field("type", "double");
                }
                builder.endObject();
                builder.startObject("latitude");
                {
                    builder.field("type", "double");
                }
                builder.endObject();
                builder.startObject("카메라대수");
                {
                    builder.field("type", "long");
                }
                builder.endObject();
                builder.startObject("카메라화소수");
                {
                    builder.field("type", "double");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        createIndexRequest.mapping(builder);
        boolean isCreated = client.indices().create(createIndexRequest, RequestOptions.DEFAULT).isAcknowledged();
        Assertions.assertTrue(isCreated);
        System.out.println(CCTV_DATA + " 인덱스가 생성되었습니다.");
    }

    private List<Map<String, Object>> getObjectFromResourceFile() throws IOException {
        InputStream inputStream = new ClassPathResource("cctv-data.json").getInputStream();
        return new ObjectMapper().readValue(inputStream, new TypeReference<List<Map<String, Object>>>() {
                }).stream()
                .filter(e -> StringUtils.hasText(String.valueOf(e.get("longitude"))))
                .filter(e -> StringUtils.hasText(String.valueOf(e.get("latitude"))))
                .collect(Collectors.toList());
    }


    private void removeIndexIfExists(String indexName) throws IOException {
        boolean exists = isExistIndex(indexName);
        if (exists) {
            System.out.println(indexName + " 인덱스가 이미 존재합니다.");
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            boolean acknowledged = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT).isAcknowledged();
            Assertions.assertTrue(acknowledged);
            System.out.println(indexName + " 인덱스가 삭제되었습니다.");
        }
    }

    private boolean isExistIndex(String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        return exists;
    }


}
