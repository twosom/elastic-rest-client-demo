package com.example.elasticdemo;

import com.example.elasticdemo.model.ElasticRecruitModel;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.MultiTermVectorsResponse;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;


@SpringBootTest
public class GravylabSaraminIndexTest {

    public static final String SARAMIN = "saramin";
    public static final String NEW_SARAMIN = "new_saramin";
    public static final String MYTEMPLATE_1 = "mytemplate_1";
    public static final String STANDARD_ANALYZER = "standard";
    public static final String CREATED = "CREATED";
    RestHighLevelClient client;

    @Value("${elastic.host}")
    private String HOST;

    @Value("${elastic.port}")
    private int PORT;

    private final static String GRAVYLAB_NORI_ANALYZER = "gravylab-nori-analyzer";

    @Autowired
    ModelMapper mapper;


    private RestHighLevelClient createClient(String host, int port) {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port, "http")
                )
        );
    }

    @BeforeEach
    @Test
    void beforeEach() {
        //== client 생성 ==//
        client = createClient(HOST, PORT);
    }


    @AfterEach
    @Test
    void afterEach() throws IOException {
        //== 사용 완료한 client 닫기 ==//
        client.close();
    }

    //=========================== SARAMIN =============================//
    @DisplayName("ElasticSearch 에서 모든 구직공고 가져오기")
    @Test
    void get_recruit_data() throws IOException {
        client = createClient(HOST, PORT);
        //== 생성자 안에 조회할 인덱스 지정 가능(String) 아무것도 안넣고 할 시 모든 INDEX 에서 검색 ==//
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //== 모든 Document 조회 ==//
        searchSourceBuilder.query(
                        QueryBuilders.matchAllQuery()
                )
                .size(50);

        searchRequest.source(searchSourceBuilder);
        List<ElasticRecruitModel> modelList = Arrays.stream(client.search(searchRequest, RequestOptions.DEFAULT)
                        .getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .map(e -> mapper.map(e, ElasticRecruitModel.class))
                .collect(Collectors.toList());

        for (ElasticRecruitModel elasticRecruitModel : modelList) {
            System.out.println(elasticRecruitModel);
        }
    }


    @DisplayName("검색어를 통해 ElasticSearch 에서 가져오기")
    @ParameterizedTest(name = "{index} 번 검색어 : {0}")
    @ValueSource(strings = {"개발자", "개발", "인사", "회계"})
    void search_by_keyword(String keyword) throws IOException {
        //== SearchRequest 생성. 생성자에 특정 인덱스를 지정하여 해당 인덱스(복수 가능) 범위 내에서만 검색 가능하도록 할 수 있다. ==//
        SearchRequest searchRequest = new SearchRequest();
        //== SearchSourceBuilder 생성. 실질적인 검색 쿼리를 하는 Body 라고 생각하면 된다. ==//
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                //== boolQuery 는 안의 조건들이 모두 참일 경우 가져온다. ==//
                boolQuery()
                        //== should 는 OR 와 같다. ==//
                        //== etc, company, subject 중에서 하나라도 매칭되는 결과를 가져온다. ==//
                        .should(
                                matchQuery("etc", keyword)
                        )
                        .should(
                                matchQuery("company", keyword)
                        )
                        .should(
                                matchQuery("subject", keyword)
                        )
        );

        //== 생성한 searchRequest 에 검색어 쿼리가 담긴 searchSourceBuilder 를 설정한다. ==//
        searchRequest.source(searchSourceBuilder);

        //== searchRequest 로 client 에 검색을 요청 후, 검색 결과를 객체로 변환하는 작업 ==//
        List<ElasticRecruitModel> modelList = Arrays.stream(client.search(searchRequest, RequestOptions.DEFAULT)
                        .getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .map(this::converSourceMapToModel)
                .collect(Collectors.toList());

        //== 검색 결과 출력 ==//
        for (ElasticRecruitModel elasticRecruitModel : modelList) {
            System.out.println(elasticRecruitModel);
        }
    }


    @DisplayName("사용자가 구현한 분석기 사용")
    @ParameterizedTest(name = "{index} 번 텍스트 : {0}")
    @ValueSource(strings = {"안녕하세요. 그레이비랩입니다.", "아버지가 방에 들어가신다.", "아버지 가방에 들어가신다", "[체외진단의료기기 전문기업] 면역진단 마케팅 및 PM (7년 이상)"})
    void analyse_with_custom_analyzer(String text) throws IOException {
        //== 분석 요청을 위한 AnalyzeRequest 생성. withIndexAnalyzer 는 특정 인덱스 지정, withGlobalAnalyzer 는 모든 인덱스에 해당된다.  ==//
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(SARAMIN, GRAVYLAB_NORI_ANALYZER, text);

        //== client 에 analyzerRequest 를 전달하여 분석을 요청한다. ==//
        AnalyzeResponse response = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        //== 결과를 받아서 token 을 List 로 추출한다. ==//
        List<String> tokens = extractTokensFromResponse(response);

        System.out.println("tokens = " + tokens);
    }

    @DisplayName("Standard 분석기 사용")
    @Test
    void analyze_with_standard_analyzer() throws IOException {
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(SARAMIN, STANDARD_ANALYZER, "I am a boy");
        AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> tokens = extractTokensFromResponse(analyzeResponse);
        System.out.println("tokens = " + tokens);
    }

    @DisplayName("Standard Tokenizer 사용")
    @Test
    void analyze_with_standard_tokenizer() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.buildCustomAnalyzer("standard")
                .build("I am a boy");
        AnalyzeResponse response = client.indices().analyze(request, RequestOptions.DEFAULT);
        List<String> tokens = extractTokensFromResponse(response);
        System.out.println("tokens = " + tokens);
    }


    @DisplayName("문서의 특정 field Analyze 결과 보기")
    @Test
    void term_vectors() throws IOException {


        //== 검색을 위한 SearchRequest 생성 ==//
        //TODO 자동 완성 검색어 목록을 어떻게 추려낼 지
        long start = new Date().getTime();
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                        QueryBuilders.matchAllQuery()
                ).fetchSource(new String[]{"positionId"}, new String[]{"etc", "subject", "pageUrl", "company", "finished", "source", "pageId", "recruitSupportType"})
                .size(50);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        List<String> idList = Arrays.stream(response.getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .map(this::converSourceMapToModel)
                .map(ElasticRecruitModel::getPositionId)
                .collect(Collectors.toList());

        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(SARAMIN, "fake_id");
        termVectorsRequest.setFields("etc", "subject", "company");

        MultiTermVectorsRequest multiTermVectorsRequest = new MultiTermVectorsRequest(idList.toArray(new String[0]), termVectorsRequest);
        MultiTermVectorsResponse mtermvectors = client.mtermvectors(multiTermVectorsRequest, RequestOptions.DEFAULT);
        System.out.println("mtermvectors = " + mtermvectors);


        List<String> tokenList = mtermvectors
                .getTermVectorsResponses()
                .stream()
                .map(TermVectorsResponse::getTermVectorsList)
                .flatMap(Collection::stream)
                .map(TermVectorsResponse.TermVector::getTerms)
                .flatMap(Collection::stream)
                .map(TermVectorsResponse.TermVector.Term::getTerm)
                .map(String::trim)
                .distinct()
                .filter(e -> e.length() > 1)
                .filter(e -> !e.matches("[+-]?\\d*(\\.\\d+)?"))
                .collect(Collectors.toList());
        long end = new Date().getTime();
        System.out.println(end - start);

        for (String token : tokenList) {
            System.out.println(token);
        }
    }

    @DisplayName("Reindex API 사용")
    @Test
    void re_index() throws IOException {

        //== 인덱스 존재 여부 확인 ==//
        GetIndexRequest getIndexRequest = new GetIndexRequest(NEW_SARAMIN);
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        //== 인덱스가 존재하면 삭제 ==//
        if (exists) {
            System.out.println("새로운 인덱스가 이미 존재합니다.");
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(NEW_SARAMIN);
            boolean result = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT)
                    .isAcknowledged();
            System.out.println("새로운 인덱스 삭제 결과 : " + result);
        }

        //== 기존의 saramin 인덱스를 new_saramin 이라는 인덱스로 복사 요청 ==//
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(SARAMIN)
                .setDestIndex(NEW_SARAMIN);
        BulkByScrollResponse response = client.reindex(reindexRequest, RequestOptions.DEFAULT);
        System.out.println(response.getCreated() + "개의 데이터가 추가됨.");
    }

    @DisplayName("Template API 를 이용한 인덱스 템플릿 생성")
    @Test
    void create_index_template() throws IOException {

        //== 해당 index template 이 이미 있는지 확인 ==//
        GetIndexTemplatesRequest getTemplateRequest = new GetIndexTemplatesRequest(MYTEMPLATE_1);
        GetIndexTemplatesResponse getTemplateResponse = client.indices().getIndexTemplate(getTemplateRequest, RequestOptions.DEFAULT);
        //== 이미 존재하는 인덱스 템플릿이라면 ==//
        if (!getTemplateResponse.getIndexTemplates().isEmpty()) {
            //== 해당 인덱스 템플릿 삭제 요청 ==//
            System.out.println(MYTEMPLATE_1 + " 템플릿 이미 존재");
            DeleteIndexTemplateRequest deleteTemplateRequest = new DeleteIndexTemplateRequest(MYTEMPLATE_1);
            AcknowledgedResponse deleteTemplateResponse = client.indices().deleteTemplate(deleteTemplateRequest, RequestOptions.DEFAULT);
            boolean result = deleteTemplateResponse.isAcknowledged();
            System.out.println(MYTEMPLATE_1 + " 템플릿 삭제 성공 여부 : " + result);
        }

        PutIndexTemplateRequest request = new PutIndexTemplateRequest("mytemplate_1");
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 1);


        request.patterns(List.of("test*"))
                .order(1)
                .settings(settings)
                .mapping("{\n" +
                        "    \"properties\" : {\n" +
                        "           \"test\": {\n" +
                        "               \"type\" : \"text\" \n" +
                        "           }\n" +
                        "       }\n" +
                        "}", XContentType.JSON)
                .alias(new Alias("alias1"));


        AcknowledgedResponse putTemplateResponse = client.indices().putTemplate(request, RequestOptions.DEFAULT);
        System.out.println(MYTEMPLATE_1 + " 템플릿 생성 성공 여부 : " + putTemplateResponse.isAcknowledged());

    }

    @DisplayName("테스트용 인덱스 만들기")
    @Test
    void create_test_index() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("docs");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (exists) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("docs");
            boolean acknowledged = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT)
                    .isAcknowledged();
            System.out.println("인덱스 삭제 성공 여부 : " + acknowledged);
        }
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("docs");
        createIndexRequest
                .mapping("{\n" +
                                "           \"properties\" : {\n" +
                                "                   \"title\" : { \"type\" : \"text\" },\n" +
                                "                   \"content\" : { \"type\" : \"keyword\" }\n" +
                                "             }\n" +
                                "}",
                        XContentType.JSON
                );
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        boolean result = createIndexResponse.isAcknowledged();
        System.out.println("인덱스 생성 성공 여부 : " + result);
    }

    @DisplayName("테스트용 인덱스에 문서 입력")
    @Test
    void insert_docs_to_test_index() throws IOException {
        IndexRequest indexRequest = new IndexRequest("docs");
        Map<String, Object> source = new HashMap<>();
        source.put("title", "ElasticSearch Training Book");
        source.put("content", "ElasticSearch is cool open source search engine");
        indexRequest
                .source(source);


        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("indexResponse = " + indexResponse);
        Assertions.assertEquals(indexResponse.getResult().name(), CREATED);
    }


    private List<String> extractTokensFromResponse(AnalyzeResponse response) {
        return response
                .getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .distinct()
                .collect(Collectors.toList());
    }


    private ElasticRecruitModel converSourceMapToModel(Map<String, Object> e) {
        return mapper.map(e, ElasticRecruitModel.class);
    }
}
