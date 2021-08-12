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
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.rankeval.EvaluationMetric;
import org.elasticsearch.index.rankeval.PrecisionAtK;
import org.elasticsearch.index.rankeval.RatedDocument;
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

import java.awt.print.Pageable;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;

@ActiveProfiles("gravylab")
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
                .setScript(script)
                .setTimeout(TimeValue.timeValueMinutes(2));

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

    //== QueryContext ==//

    @DisplayName("match 쿼리 사용")
    @Test
    void search_with_match_query() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // TODO /match 쿼리는 queryContext 중에서도 가장 많이 사용되는 쿼리.
        //  검색어로 들어온 문자열을 analyzer 를 통해 분석한 후 inverted index 에서 해당 문자열의 토큰을 가지고 있는 문서를 검색.
        //  문서의 해당 필드에 설정해 놓은 analyzer 를 기본으로 사용하며, 별도의 analyzer 를 사용할 때는 직접 명시해 주면 된다.
        //  match 쿼리는 analyzer 를 통해서 분석한 토큰을 기준으로 검색하며 이때 어떤 토큰이 먼저 있는지에 대한 순서는 고려하지 않는다.
        //  즉, 검색어에 "nginx guide" 가 들어오든, "guid nginx" 가 들어오든 같은 결과를 보여준다.

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

    @DisplayName("match_phrase 쿼리")
    @Test
    void search_with_match_phrase_query() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // TODO 기존 match 쿼리의 경우, kernel 과 linux 라는 두개의 토큰을 만들고 두 개의 단어 중 "하나라도" 포함되어 있다면 검색 결과 보여줌.
        searchSourceBuilder.query(
                matchQuery("description", "Kernel Linux")
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        System.out.println("기존 matchQuery 결과 : ");
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }


        // TODO match_phrase 쿼리의 경우 kernel 과 linux 두 개의 토큰을 만드는 것은 동일하지만
        //  kernel linux 라는 정확한 두 개의 단어 중 단어 하나만 포함되어 있다면 검색 결과를 보여주지 못한다.

        searchRequest = new SearchRequest(TEST_DATA);
        searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(
                matchPhraseQuery("description", "Kernel Linux")
        );

        searchRequest.source(searchSourceBuilder);
        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        searchResult = extractResultList(searchResponse);

        // TODO match_phrase 쿼리로 Kernel Linux 문자열을 검색하면 아무것도 나오지 않는다.
        System.out.println("matchPhraseQuery 결과(검색어 : Kernel Linux) : ");
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }


        // TODO match_phrase 쿼리로 Linux Kernel 라는 순서로 검색


        searchRequest = new SearchRequest(TEST_DATA);
        searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(
                matchPhraseQuery("description", "Linux Kernel")
        );

        searchRequest.source(searchSourceBuilder);
        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        searchResult = extractResultList(searchResponse);
        System.out.println("matchPhraseQuery 결과(검색어 : Linux Kernel) : ");
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }

        // TODO 검색어의 순서가 중요할 경우에는 match_phrase 를, 순서에 구애받지 않는 경우에는 match 를 사용하면 된다.

    }


    @DisplayName("multi_match 쿼리")
    @Test
    void search_with_multi_match_query() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        // TODO multi_match 는 match 와 동일하지만 두 개 이상의 필드에 match 쿼리를 날릴 수 있다.
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // TODO multiMatchQuery 파라미터에는 찾고자 하는 키워드, 찾을 위치(필드명) 들을 입력하면 된다.
        //  아래에는 title 과 description 필드를 대상으로 kernel 이라는 문자열을 검색하였다.
        searchSourceBuilder.query(
                multiMatchQuery("kernel", "title", "description")
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);

        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
    }


    @DisplayName("query_string 쿼리")
    @Test
    void search_with_query_string() throws Exception {
        // TODO query_string 은 and 와 or 같은 검색어 간 연산이 필요한 경우에 사용한다.
        //  경우에 따라서 match 쿼리나 multi_match 와 동일하게 동작할 수도 있고 regular expression 기반의 쿼리가 될 수도 있다.
        //  아래와 같은 query_string 을 만들면 match 쿼리와 같은 의미를 만들 수 있다.
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // TODO 기존의 match 쿼리
        searchSourceBuilder.query(
                matchQuery("title", "Linux")
        );
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        System.out.println("기존의 match 쿼리 사용 : ");
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }

        // TODO query_string(위의 match 쿼리와 같은 역할을 한다.)
        searchRequest = new SearchRequest(TEST_DATA);
        searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(
                // TODO 검색할 키워드를 입력한다.
                queryStringQuery("Linux")
                        // TODO 검색할 필드를 입력한다.
                        .field("title")
        );
        searchRequest.source(searchSourceBuilder);

        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        searchResult = extractResultList(searchResponse);
        System.out.println("query_string 사용 : ");
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }

        // TODO 또는 query_string 을 통한 와일드카드 검색을 할 수도 있다.
        //  query_string 을 통한 와일드카드 검색
        searchRequest = new SearchRequest(TEST_DATA);
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                queryStringQuery("*nux")
                        .field("title")
        );

        searchRequest.source(searchSourceBuilder);

        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        searchResult = extractResultList(searchResponse);
        System.out.println("query_string 를 통한 와일드카드 검색 : ");
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
        // TODO query_string 을 통한 와일드카드 검색은 스코어링을 하지 않을뿐더러,
        //  검색 성능에 좋지 않기 때문에 경우에 따라서는 다른 쿼리로 수정해서 사용하는 것이 좋다.
    }


    //== FilterContext ==//
    //TODO 이름에서 알 수 있듯이 해당 문서에 대한 필터링에 사용되는 쿼리이다.
    // QueryContext 가 검색어가 문서에 얼마나 매칭되는지 계산하고 찾는다면,
    // FilterContext 는 검색어의 포함 여부를 찾는 형태다. 둘 사이의 가장 큰 차이점은 검색어를 analyze 하느냐의 여부다.


    @DisplayName("term 쿼리")
    @Test
    void search_with_term_query() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //TODO term 쿼리는 정확하게 일치되는 단어를 찾을 때 사용한다. analyze 를 하지 않기 때문에 "당연히" 대소문자를 구분한다.
        // 대문자로 시작하는 Linux 로 검색 시
        searchSourceBuilder.query(
                termQuery("title", "Linux")
        );
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        // TODO 검색 결과가 않나온다.
        System.out.println("Linux 검색 결과 : ");
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }


        searchRequest = new SearchRequest(TEST_DATA);
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                termQuery("title", "linux")
        );
        searchRequest.source(searchSourceBuilder);
        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        searchResult = extractResultList(searchResponse);
        System.out.println("linux 검색 결과 : ");
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
        // TODO 둘의 결과는 완전히 다르다. 그래서 보통 text 타입의 필드를 대상으로 할 때는 term 쿼리보다 match 쿼리를 사용하는 것이 일반적이다.
    }

    @DisplayName("range 쿼리")
    @Test
    void search_with_range_query() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        // TODO range 쿼리는 범위를 지정하여 특정 값의 범위 이내에 있는 경우를 검색할 때 사용한다.
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //TODO release_date 를 기준으로 2015년 1월 1일부터 2015년 12월 13일까지 발행된 책들을 검색하는 쿼리이다.
        searchSourceBuilder.query(
                rangeQuery("release_date")
                        .gte("2015/01/01")
                        .lte("2015/12/31")
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
    }

    @DisplayName("wildcard 쿼리")
    @Test
    void search_with_wildcard_query() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        //TODO 이름에서 알 수 있듯이 와일드카드 특수문자를 이용한 일종의 Full-Scan 검색이 가능한 쿼리이다.
        // 이 쿼리도 text 필드가 아닌 keyword 타입의 쿼리에 사용해야 한다.
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //TODO publisher 중에 Media 라는 단어가 포함된 모든 책을 검색해 준다. 하지만 wildcard query 는
        // 모든 inverted index 를 하나하나 확인하기 때문에 검색 속도가 매우 느리다. 특히 아래와 같이 시작부터 * 를 포함하는
        // 쿼리는 문서의 개수가 늘어날수록 검색 결과도 선형적으로 늘어나기 때문에 주의해야 한다.
        // 사실 wildcard 쿼리를 사용할 때는 꼭 wildcard 쿼리를 써야 하는지를 먼저 재고해야 한다.
        // 아래처럼 Media 라는 단어가 포함된 책을 찾은 경우에는 match 쿼리를 활용하는 편이 더 빠르다.
        // 실제로 두 쿼리를 실행해 보면 match 쿼리가 wildcard 쿼리에 비해 훨씬 속도가 빠르다.


        // TODO wildcard 활용
        searchSourceBuilder.query(
                wildcardQuery("publisher.keyword", "*Media*")
        );
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        System.out.println("wildcard 를 활용한 검색");
        System.out.println("검색에 걸린 시간 : " + searchResponse.getTook().toString());
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }


        searchRequest = new SearchRequest(TEST_DATA);
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                matchQuery("publisher", "Media")
        );
        searchRequest.source(searchSourceBuilder);

        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        searchResult = extractResultList(searchResponse);
        System.out.println("match 를 활용한 검색");
        System.out.println("검색에 걸린 시간 : " + searchResponse.getTook().toString());
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
    }


    //TODO bool query 를 이용해 쿼리 조합하기
    // 지금까지 QueryContext 와 FilterContext 를 살펴보았다. 사실 이 두가지 쿼리만 가지고는 검색 조건을 맞추기가 불가능하다.
    // 예를 들어 특정 시기 동안 발행된 nginx 관련 도서를 검색하기 위해서는 range 쿼리와 match 쿼리 두 가지가 함께 필요하기 때문이다.
    // 이번에는 두 가지 이상의 쿼리를 조합해서 사용하는 방법을 살펴볼 것이다. 그 중에서도 가장 대표적이고 많이 사용되는 bool query 에 대해서 살펴보자.
    // 아래는 bool query 에서 사용할 수 있는 항목을 정리한 것이다.
    // must: 항목 내 쿼리에 일치하는 문서를 검색, 스코어링 O, 캐싱 X
    // filter: 항목 내 쿼리에 일치하는 문서를 검색, 스코어링 X, 캐싱 O
    // should: 항목 내 쿼리에 일치하는 문서를 검색, 스코어링 O, 캐싱 X
    // must_not: 항목 내 쿼리에 잋리하지 않는 문서를 검색, 스코어링 X, 캐싱 O


    //TODO 쿼리의 문서 일치/불일치 여부와 검색된 문서의 스코어링 계산 여부, 캐싱 여부에 따라 항목이 나뉜다.
    // 위 특징들을 기준으로 bool query 의 must, should 는 QueryContext 에서 실행되고, filter, must_not 은
    // FilterContext 에서 실행된다. 앞으로는 쿼리별로 QueryContext 와 FilterContext 를 개별적으로 사용했다면,
    // bool query 는  Query/FilterContext 를 혼합해서 사용할 수 있다.


    @DisplayName("bool query 예제")
    @Test
    void search_with_bool_query() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //TODO title 이 nginx 와 매칭되면서 동시에 release_date 가 2016/01/01 ~ 2017/12/31 사이 를 검색
        // 여기서 한 가지 주의할 점은 filterContext 에 포함되는 쿼리들은 filter 절에 넣는 것이 좋다.
        // release_date 필드에 대한 검색 쿼리를 filter 절이 아닌 must 절에 포함시켜도 동일한 검색 겨로가가 나오지만, 실제로 실행해 보면
        // filter 절에 포함시킨 결과가 더 빠르다.
        searchSourceBuilder.query(
                boolQuery()
                        .must(
                                matchQuery("title", "nginx")
                        )
                        .filter(
                                rangeQuery("release_date")
                                        .gte("2016/01/01")
                                        .lte("2017/12/31")
                        )
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        System.out.println("filter 절에 포함시킨 쿼리의 검색 소요 시간 : " + searchResponse.getTook().toString());
        System.out.println("title 에 nginx 가 들어가며 release_date 가 2016/01/01 ~ 2017/12/31 인 결과 조회 : ");
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }

        searchRequest = new SearchRequest(TEST_DATA);
        searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(
                boolQuery()
                        .must(
                                matchQuery("title", "nginx")

                        )
                        .must(
                                rangeQuery("release_date")
                                        .gte("2014/01/01")
                                        .lte("2017/12/31")
                        )
        );

        searchRequest.source(searchSourceBuilder);
        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        searchResult = extractResultList(searchResponse);
        System.out.println("must 절에 포함시킨 쿼리의 검색 소요 시간 : " + searchResponse.getTook().toString());
        System.out.println("title 에 nginx 가 들어가며 release_date 가 2014/01/01 ~ 2017/12/31 인 결과 조회 : ");
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }

        //TODO 이와 같은 현상이 발생하는 이유는 must 절에 포함된 FilterContext 들은 score 를 계산하는데 활용되기 때문에(must 절은 QueryContext)
        // 불필요한 연산이 들어가지만, filter 절에 포함되면 FilterContext 에 맞게 score 계산이 되지 않기 때문이다.
        // 또한 filter 절에서 실행된 range 쿼리는 캐싱의 대상이 되기 때문에 결과를 빠르게 응답 받을 가능성이 높다.
        // 하지만 title 에 nginx 검색은 match 쿼리이며 내부적인 룰에 의해서 점수가 더 높은 책이 먼저 검색되어야 할 필요가 있기 대문에
        // must 절에 포함시켜야 한다. 따라서 검색 조건이 yes 또는 no 만을 포함하는 경우라면 filter 절에 넣어 FilterContext 에서 실행되도록
        // 해야 하고, 매칭의 정도가 중요한 조건이라면 must 혹은 should 절에 포함시켜서 QueryContext 에서 실행되도록 해야 한다.
        // 반면에 must_not 절은 쿼리에 일치하지 않는 문서를 검색하는 항목이다.
    }


    @DisplayName("must_not 절")
    @Test
    void search_with_must_not() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //TODO must_not 절은 사용자가 검색 쿼리를 통해 원치 않는 문서를 제외할 수 있는 항목이다.
        // 이 must_not 절은 앞서 살펴본 filter 절과 마찬가지로 FilterContext 에서 실행되어
        // 마찬가지로 score 계산을 하지 않으며 문서 캐싱의 대상이 된다.
        searchSourceBuilder.query(
                boolQuery()
                        .must(
                                matchQuery("title", "nginx")
                        )
                        .filter(
                                rangeQuery("release_date")
                                        .gte("2014/01/01")
                                        .lte("2017/12/31")
                        )
                        .mustNot(
                                matchQuery("description", "performance")
                        )
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> searchResult = extractResultList(searchResponse);
        System.out.println("검색 소요 시간 : " + searchResponse.getTook().toString());
        for (Map<String, Object> result : searchResult) {
            System.out.println(result);
        }
    }


    //TODO should 는 내용만으로는 must 절과 큰 차이가 없어 보이지만, should 절은 minimum_should_match 라는 옵션을 제공한다.
    // 이 옵션은 should 절을 사용할 때 꼭 써야만 하는 옵션은 아니다.

    @DisplayName("minimum_should_match 를 사용하지 않은 should 절")
    @Test
    void search_without_minimum_should_match() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(
                boolQuery()
                        .must(
                                matchQuery("title", "nginx")
                        )
                        .filter(
                                rangeQuery("release_date")
                                        .gte("2014/01/01")
                                        .lte("2017/12/31")
                        )
                        //TODO 이렇게 should 와 같이 같은 조건에 여러개의 matchQuery 가 들어가야 할 경우에는
                        // 메소드 체이닝으로 이어준다.
                        .should(
                                matchQuery("title", "performance")
                        )
                        .should(
                                matchQuery("description", "scalable web")
                        )
        );
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("검색 소요 시간 : " + searchResponse.getTook().toString());
        for (SearchHit hit : searchResponse.getInternalResponse().hits().getHits()) {
            System.out.println(hit.toString());
        }

        //TODO should 절 내의 조건과 일치하는 부분이 있는 문서는 스코어가 올라가게 된다.
        // 즉, 하나의 쿼리 내에서 스코어를 올려주는 boost 같은 기능 외에 검색을 통해 문서의 스코어를 올려줄 때 사용할 수 있다.
    }


    @DisplayName("minimum_should_match 를 사용한 should 절")
    @Test
    void search_with_minimum_should_match() throws Exception {
        if (!isExistIndex(TEST_DATA)) create_index_using_bulk_api2();
        SearchRequest searchRequest = new SearchRequest(TEST_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(
                boolQuery()
                        .must(
                                matchQuery("title", "nginx")
                        )
                        .filter(
                                rangeQuery("release_date")
                                        .gte("2014/01/01")
                                        .lte("2017/12/31")
                        )
                        //TODO 1번
                        .should(
                                matchQuery("title", "performance")
                        )
                        //TODO 2번
                        .should(
                                matchQuery("description", "scalable web")
                        )
                        .minimumShouldMatch(1)
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("검색 소요 시간 : " + searchResponse.getTook().toString());
        for (SearchHit hit : searchResponse.getInternalResponse().hits().getHits()) {
            System.out.println(hit.toString());
        }
        //TODO 위와 같이 minimum_should_match 옵션을 사용한 쿼리는 전혀 다른 결과를 보여줄 수 있다.
        // 1과 2의 쿼리 중 "적어도" 하나는 일치해야 결과를 리턴하는 옵션이다.
        // 같은 should 절을 사용한 코드와는 결과가 크게 상이해질 수 있다.
    }


    @DisplayName("search API 총 복습")
    @Test
    void search_api_review() throws Exception {
        //TODO 실제 검색을 위해 13 만건 정도 되는 CCTV 데이터 사용(Analyzer 적용 완료)
        if (!isExistIndex(NEW_CCTV_DATA)) reindex_for_geo_point();
        //TODO 검색 조건 : 관리기관명이나 소 재지번주소에 "서울"이 들어가며 "시설물"과 관련된 설치구분 중 설치년월이 2014년 이후인 것을 검색
        SearchRequest searchRequest = new SearchRequest(NEW_CCTV_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(
                boolQuery()
                        .must(
                                multiMatchQuery("서울", "관리기관명.nori", "소재지지번주소.nori")
                        )
                        .must(
                                matchQuery("설치목적구분.nori", "시설물")
                        )
                        .filter(
                                rangeQuery("설치년월")
                                        .gte("2014-01")
                        )
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("검색 소요 시간 : " + searchResponse.getTook().toString());
        System.out.println("총 " + searchResponse.getHits().getTotalHits().value + " 건이 검색되었습니다.");
        for (SearchHit hit : searchResponse.getInternalResponse().hits().getHits()) {
            System.out.println(hit.toString());
        }
    }

    @DisplayName("geo distance 를 이용한 검색")
    @Test
    void search_with_geo_distance_api1() throws Exception {
        //TODO 실제 검색을 위해 13 만건 정도 되는 CCTV 데이터 사용(Analyzer 적용 완료)
        if (!isExistIndex(NEW_CCTV_DATA)) reindex_for_geo_point();
        SearchRequest searchRequest = new SearchRequest(NEW_CCTV_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        //TODO 현재 위치(인천)에서 반경 5KM 안에 2014년 1월 이후에 설치되었으며 보관일수가 30일 이상이며 설치목적구분이 방범인 것,
        searchSourceBuilder.query(
                boolQuery()
                        .filter(
                                geoDistanceQuery("location")
                                        .distance(5, DistanceUnit.KILOMETERS)
                                        //TODO 인천 좌표
                                        .point(37.50471, 126.73972)
                        )
                        .filter(
                                rangeQuery("보관일수")
                                        .gte(30)
                        )
                        .filter(
                                rangeQuery("설치년월")
                                        .gte("2014-01")
                        )
                        .must(
                                matchQuery("설치목적구분.nori", "방범")
                        )
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println("검색 소요 시간 : " + searchResponse.getTook().toString());
        System.out.println("총 " + searchResponse.getHits().getTotalHits().value + " 건이 검색되었습니다.");
        for (SearchHit hit : searchResponse.getInternalResponse().hits().getHits()) {
            System.out.println(hit.toString());
        }
    }

    @DisplayName("geo distance 를 이용한 검색")
    @Test
    void search_with_geo_distance_api2() throws Exception {
        if (!isExistIndex(NEW_CCTV_DATA)) reindex_for_geo_point();
        // TODO 촬영방면정보가 360도 전방면인 반경 10KM 안에 있으면서 설치목적구분이 어린이보호인 모든 CCTV 조회
        SearchRequest searchRequest = new SearchRequest(NEW_CCTV_DATA);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                boolQuery()
                        .must(
                                matchQuery("촬영방면정보", "360도 전방면")
                        )
                        .must(
                                matchQuery("설치목적구분", "어린이보호")
                        )
                        .filter(
                                geoDistanceQuery("location")
                                        .point(37.504794, 126.739541)
                                        .distance(5, DistanceUnit.KILOMETERS)
                        )
        );
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("검색 소요 시간 : " + searchResponse.getTook().toString());
        System.out.println("총 " + searchResponse.getHits().getTotalHits().value + " 건이 검색되었습니다.");
        for (SearchHit hit : searchResponse.getInternalResponse().hits().getHits()) {
            System.out.println(hit.toString());
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
                mappingFieldWithAnalyzer(builder, "관리기관명", "text");
                mappingFieldWithAnalyzer(builder, "설치목적구분", "text");
                mappingFieldWithAnalyzer(builder, "소재지도로명주소", "text");
                mappingFieldWithAnalyzer(builder, "소재지지번주소", "text");
                mappingFieldWithAnalyzer(builder, "제공기관명", "text");
                mappingFieldWithAnalyzer(builder, "촬영방면정보", "text");
            }
            builder.endObject();
        }
        builder.endObject();
    }

    private void mappingFieldWithAnalyzer(XContentBuilder builder, String field, String type) throws IOException {
        builder.startObject(field);
        {
            builder.field("type", type);
            builder.startObject("fields");
            {
                builder.startObject("nori");
                {
                    builder.field("type", "text");
                    builder.field("analyzer", "nori");
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
