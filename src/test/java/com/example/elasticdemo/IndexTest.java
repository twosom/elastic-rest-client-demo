package com.example.elasticdemo;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@ActiveProfiles("gravylab")
@SpringBootTest
public class IndexTest {
    public static final String NUMBER_OF_SHARDS = "number_of_shards";
    public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
    public static final String GRAVYLAB_NORI_TOKENIZER = "gravylab_nori_tokenizer";
    public static final String GRAVYLAB_NORI_ANALYZER = "gravylab_nori_analyzer";
    public static final String GRAVYLAB_NORI_POSTFILTER = "gravylab-nori-postfilter";
    public static final String GRAVYLAB_SYNONYM_FILTER = "gravylab-synonym-filter";
    public static final String GRAVYLAB_STOP_FILTER = "gravylab-stop-filter";
    public static final String TEST_RECRUIT_DATA = "test_recruit_data";

    @Value("${elastic.host}")
    private String HOST;

    @Value("${elastic.port}")
    private int PORT;

    RestHighLevelClient client;

    @Autowired
    ModelMapper modelMapper;

    String[] filter = {
            GRAVYLAB_NORI_POSTFILTER
            , "nori_readingform"
            , GRAVYLAB_SYNONYM_FILTER
            , GRAVYLAB_STOP_FILTER
    };


    private RestHighLevelClient createClient(String host, int port) {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port, "http")
                )
        );
    }

    @BeforeEach
    void beforeEach() {
        client = createClient(HOST, PORT);
    }

    @AfterEach
    void afterEach() throws IOException {
        client.close();
    }


    private static AtomicLong id = new AtomicLong(1);
    String[] onlyNouns = {"E", "J", "IC", "MAG", "MAJ", "NA", "SC",
            "SE", "SH", "SP", "SSC", "SSO", "UNA", "UNKNOWN",
            "VCP", "VCN", "VSV", "XPN", "XSA", "XSV", "SY", "VA", "VV", "VX"};

    @Test
    void create_index_for_recruit_test() throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("classpath:test-json/**/*.json");

        List<Map<String,Object>> collect = new ArrayList<>();
        for (Resource resource : resources) {
            InputStream inputStream = resource.getInputStream();
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
            try {
                Map<String, Object> mappedObject = objectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {
                });
                Map<String, Object> map = (Map<String, Object>) ((Map<String, Object>) mappedObject
                        .get("result")).get("detail");
                collect.add(map);

            } catch (Exception e) {
                System.out.println("error file" + resource.getFilename());
                e.printStackTrace();
            }
        }


        // TODO index 만들기
        if (isExistsIndex(TEST_RECRUIT_DATA)) {
            createIndex(TEST_RECRUIT_DATA);
        }


        collect = collect
                .stream()
                .limit(500)
                .filter(e -> !((List<String>) e.get("description")).isEmpty())
                .filter(e -> !((List<String>) e.get("hashtag")).isEmpty())
                .filter(e -> !((List<String>) e.get("welfare")).isEmpty())
                .filter(e -> !((List<String>) e.get("employmentType")).isEmpty())
                .filter(e -> !((List<String>) e.get("companyInformation")).isEmpty())
                .collect(Collectors.toList());

        // TODO BulkRequest 만들기
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, Object> source : collect) {
            IndexRequest indexRequest = new IndexRequest(TEST_RECRUIT_DATA);
            String jsonString = new ObjectMapper()
                    .writeValueAsString(source);
            System.out.println("jsonString = " + jsonString);
            indexRequest.source(jsonString,XContentType.JSON);
            bulkRequest.add(indexRequest);

        }

        long count = Arrays.stream(client.bulk(bulkRequest, RequestOptions.DEFAULT)
                        .getItems())
                .count();
        System.out.println(count + " 개의 문서가 인덱싱 되었습니다.");
        id.set(1);

    }

    private void createIndex(String index) throws IOException {

        removeIndexIfExists(index);

        XContentBuilder settingBuilder = makeSettingBuilder();
        XContentBuilder mappingBuilder = makeMappingBuilder();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(settingBuilder);
        createIndexRequest.mapping(mappingBuilder);
        boolean isCreated = this.client.indices().create(createIndexRequest, RequestOptions.DEFAULT).isAcknowledged();
        Assertions.assertTrue(isCreated);
        System.out.println(TEST_RECRUIT_DATA + " 인덱스가 생성되었습니다.");
    }


    private void removeIndexIfExists(String indexName) throws IOException {
        boolean exists = isExistsIndex(indexName);
        if (exists) {
            System.out.println(indexName + " 인덱스가 이미 존재합니다.");
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            boolean acknowledged = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT).isAcknowledged();
            Assertions.assertTrue(acknowledged);
            System.out.println(indexName + " 인덱스가 삭제되었습니다.");
        }
    }



    private boolean isExistsIndex(String index) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }

    private XContentBuilder makeMappingBuilder() throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();

        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                defineWithAnalyzer(mappingBuilder, "searchWord", "text", GRAVYLAB_NORI_ANALYZER);
                defineWithAnalyzer(mappingBuilder, "source", "text", GRAVYLAB_NORI_ANALYZER);
                defineWithAnalyzer(mappingBuilder, "companyName", "text", GRAVYLAB_NORI_ANALYZER);
                defineWithAnalyzer(mappingBuilder, "companyAddress", "text", GRAVYLAB_NORI_ANALYZER);
                defineWithAnalyzer(mappingBuilder, "description", "text", GRAVYLAB_NORI_ANALYZER);
                defineWithAnalyzer(mappingBuilder, "position", "text", GRAVYLAB_NORI_ANALYZER);
                defineWithAnalyzer(mappingBuilder, "workArea", "text", GRAVYLAB_NORI_ANALYZER);
                defineWithAnalyzer(mappingBuilder, "employmentType", "text", GRAVYLAB_NORI_ANALYZER);
                defineWithAnalyzer(mappingBuilder, "welfare", "text", GRAVYLAB_NORI_ANALYZER);
                defineWithAnalyzer(mappingBuilder, "hashtag", "text", GRAVYLAB_NORI_ANALYZER);
                defineWithoutAnalyzer(mappingBuilder, "created", "text");

            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        return mappingBuilder;
    }

    private void defineWithoutAnalyzer(XContentBuilder mappingBuilder, String field, String type) throws IOException {
        mappingBuilder.startObject(field);
        {
            mappingBuilder.field("type", type);
        }
        mappingBuilder.endObject();
    }

    private void defineWithAnalyzer(XContentBuilder mappingBuilder, String field, String type, String analyzer) throws IOException {
        mappingBuilder.startObject(field);
        {
            mappingBuilder.field("type", type);
            mappingBuilder.field("analyzer", analyzer);
        }
        mappingBuilder.endObject();
    }

    private XContentBuilder makeSettingBuilder() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field(NUMBER_OF_SHARDS, 5);
            builder.field(NUMBER_OF_REPLICAS, 1);

            builder.startObject("analysis");
            {
                builder.startObject("tokenizer");
                {
                    builder.startObject(GRAVYLAB_NORI_TOKENIZER);
                    {
                        builder.field("type", "nori_tokenizer");
                        builder.field("decompound_mode", "none");
                        builder.field("user_dictionary", "user_dictionary.txt");
                    }
                    builder.endObject();
                }
                builder.endObject();

                builder.startObject("analyzer");
                {
                    builder.startObject(GRAVYLAB_NORI_ANALYZER);
                    {
                        builder.field("type", "custom");
                        builder.field("tokenizer", GRAVYLAB_NORI_TOKENIZER);
                        builder.array("filter", filter);
                    }
                    builder.endObject();
                }
                builder.endObject();

                // 쿼리 실행 후 실행되는 필터
                builder.startObject("filter");
                {
                    builder.startObject(filter[0]);
                    {
                        builder.field("type", "nori_part_of_speech");
                        builder.array("stoptags", onlyNouns);
                    }
                    builder.endObject();
                    builder.startObject(filter[2]);
                    {
                        // 동의어 필터
                        builder.field("type", "synonym");
                        builder.field("synonyms_path", "synonymsFilter.txt");
                    }
                    builder.endObject();
                    builder.startObject(filter[3]);
                    {
                        builder.field("type", "stop");
                        builder.field("stopwords_path", "stopFilter.txt");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }


    private InputStream getInputStream(Resource e) {
        try {
            return e.getInputStream();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
