package com.example.elasticdemo;

import com.example.elasticdemo.model.ElasticRecruitModel;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.MultiTermVectorsResponse;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;


@SpringBootTest
public class GravylabSaraminIndexTest {

    RestHighLevelClient client;
    private final String NEW_INDEX = "new-index";

    @Value("${elastic.host}")
    private String HOST;

    @Value("${elastic.port}")
    private int PORT;

    private final static String ANALYZER = "gravylab-nori-analyzer";

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


    @DisplayName("분석 기능 구현")
    @ParameterizedTest(name = "{index} 번 텍스트 : {0}")
    @ValueSource(strings = {"안녕하세요. 그레이비랩입니다.", "아버지가 방에 들어가신다.", "아버지 가방에 들어가신다", "[체외진단의료기기 전문기업] 면역진단 마케팅 및 PM (7년 이상)"})
    void analyse(String text) throws IOException {
        //== 분석 요청을 위한 AnalyzeRequest 생성. withIndexAnalyzer 는 특정 인덱스 지정, withGlobalAnalyzer 는 모든 인덱스에 해당된다.  ==//
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer("saramin", ANALYZER, text);

        //== client 에 analyzerRequest 를 전달하여 분석을 요청한다. ==//
        AnalyzeResponse response = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        //== 결과를 받아서 token 을 List 로 추출한다. ==//
        List<String> tokens = response
                .getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .distinct()
                .collect(Collectors.toList());

        System.out.println("tokens = " + tokens);

    }

    @DisplayName("문서의 특정 field Analyze 결과 보기")
    @Test
    void term_vectors() throws IOException {


        //== 검색을 위한 SearchRequest 생성 ==//
        //TODO 자동 완성 검색어 목록을 어떻게 추려낼 지
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

        TermVectorsRequest termVectorsRequest = new TermVectorsRequest("saramin", "fake_id");
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

        for (String token : tokenList) {
            System.out.println(token);
        }
    }


    private ElasticRecruitModel converSourceMapToModel(Map<String, Object> e) {
        return mapper.map(e, ElasticRecruitModel.class);
    }
}
