package com.abasecode.opencode.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.HistogramBucket;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.abasecode.opencode.es.annotation.EnableCodeEs;
import com.abasecode.opencode.es.config.AutoConfiguration;
import com.abasecode.opencode.es.config.ElasticsearchConfig;
import com.abasecode.opencode.es.entity.Product;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author Jon
 * e-mail: ijonso123@gmail.com
 * url: <a href="https://jon.wiki">Jon's blog</a>
 * url: <a href="https://github.com/abasecode">project github</a>
 * url: <a href="https://abasecode.com">AbaseCode.com</a>
 */
public class ElasticsearchBaseClientTest {

    ElasticsearchBaseClient<Product> baseClient;
    String INDEX_NAME = "product";

    //    @BeforeEach
    public void begin() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "Es789456"));

        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("192.168.3.230", 9200),
                        new HttpHost("192.168.3.230", 9200)
                )
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider));

        RestClient restClient = builder.build();
        ElasticsearchClient client;
        RestClientTransport transport;
        transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        client = new ElasticsearchClient(transport);
        baseClient = new ElasticsearchBaseClient<>(client, transport);
    }

    @BeforeEach
    public void start() {
        ElasticsearchConfig.EsConfig esConfig = new ElasticsearchConfig.EsConfig();
        esConfig.setPassword("Es789456");
        esConfig.setUsername("elastic");
        esConfig.setUris(Arrays.asList("http://192.168.3.230:9200"));
        baseClient = new ElasticsearchBaseClient<>(esConfig);
    }


    @Test
    void hasIndexExist() throws IOException {
        boolean r = baseClient.hasIndexExist(INDEX_NAME);
        Assertions.assertEquals(true, r);
    }

    @Test
    void createIndex() throws IOException {
        String index = "t01";
        String json = "{" +
                "   \"mappings\" : {" +
                "      \"properties\" : {" +
                "        \"id\":{\n" +
                "          \"type\":\"keyword\"" +
                "        },\n" +
                "        \"title\" : {" +
                "          \"type\" : \"text\"," +
                "          \"analyzer\":\"ik_smart\"," +
                "          \"search_analyzer\":\"ik_max_word\"" +
                "        }" +
                "      }" +
                "   }" +
                "}";
        if (baseClient.hasIndexExist(index)) {
            baseClient.deleteIndex(index);
        }
        boolean r = baseClient.createIndex(index, json);
        Assertions.assertEquals(true, r);
    }

    @Test
    void createIndexByJsonFile() throws IOException {
        if (baseClient.hasIndexExist(INDEX_NAME)) {
            baseClient.deleteIndex(INDEX_NAME);
        }
        String file = "D:\\temp\\es-product\\product.json";
        boolean r = baseClient.createIndexByJsonFile("product", file);
        Assertions.assertEquals(true, r);
    }

    @Test
    void deleteIndex() throws IOException {
        String index = "t01";
        boolean result = false;
        if (baseClient.hasIndexExist(index)) {
            result = true;
        }
        boolean r = baseClient.deleteIndex(index);
        Assertions.assertEquals(result, r);
    }

    @Test
    void hasDocExist() throws IOException {
        String id = "001";
        baseClient.deleteDoc(INDEX_NAME, id);
        boolean r = baseClient.hasDocExist(INDEX_NAME, id);
        Assertions.assertEquals(false, r);
    }

    @Test
    void deleteDoc() throws IOException {
        String id = "7";
        String r = baseClient.deleteDoc(INDEX_NAME, id);
        System.out.println(r);
        Assertions.assertEquals("not_found", r);
    }

    @Test
    void deleteBulkWithList() throws IOException {
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        BulkResponse bulkResponse = baseClient.deleteBulkWithList(INDEX_NAME, list);
        System.out.println(bulkResponse.errors());
        System.out.println(bulkResponse.items().size());
    }

    @Test
    void saveOrUpdateDocByJson() throws IOException {
        String json = "{\"id\":\"1\",\"category\":\"图书\",\"brand\":\"南山出版社\",\"price\":79.80,\"title\":\"没有立正何来稍息\"}";
        String result = "created";
        boolean force = true;
        if (baseClient.hasDocExist(INDEX_NAME, "1")) {
            if (force) {
                result = "updated";
            } else {
                result = "exists";
            }
        }
        String r = baseClient.saveOrUpdateDocByJson(INDEX_NAME, json, "1", force);
        Assertions.assertEquals(result, r);
    }

    @Test
    void saveOrUpdateDocByJsonFile() throws IOException {
        String file = "D:\\temp\\es-product\\product-02.json";
        String result = "created";
        boolean force = false;
        if (baseClient.hasDocExist(INDEX_NAME, "3")) {
            if (force) {
                result = "updated";
            } else {
                result = "exists";
            }
        }
        String r = baseClient.saveOrUpdateDocByJsonFile(INDEX_NAME, file, "2", force);
        Assertions.assertEquals(result, r);
    }

    @Test
    void saveOrUpdateDoc() throws IOException {
        Product product = (Product) new Product()
                .setBrand("Apple")
                .setCategory("笔记本")
                .setPrice(BigDecimal.valueOf(26800.00))
                .setTitle("MacBook Pro 16S 2T-32G蜂窝版")
                .setId("3");
        String result = "created";
        boolean force = true;
        if (baseClient.hasDocExist(INDEX_NAME, "3")) {
            if (force) {
                result = "updated";
            } else {
                result = "exists";
            }
        }
        String r = baseClient.saveOrUpdateDoc(INDEX_NAME, product, force);
        Assertions.assertEquals(result, r);
    }

    @Test
    void saveOrUpdateDocBulkWithJsonFiles() throws IOException {
        String path = "D:\\temp\\es-product\\files";
        BulkResponse bulkResponse = baseClient.saveOrUpdateDocBulkWithJsonFiles(INDEX_NAME, path);
        Assertions.assertEquals(false, bulkResponse.errors());
        Assertions.assertEquals(8, bulkResponse.items().size());
    }

    @Test
    void saveOrUpdateDocBulkWithJson() throws IOException {
        List<String> list = new ArrayList<>();
        list.add("{\"id\":\"11\",\"category\":\"饮料\",\"brand\":\"雀巢\",\"price\":22.8,\"title\":\"奶香拿铁\"}");
        list.add("{\"id\":\"12\",\"category\":\"饮料\",\"brand\":\"瑞幸\",\"price\":12.9,\"title\":\"经典拿铁\"}");
        list.add("{\"id\":\"13\",\"category\":\"饮料\",\"brand\":\"星巴克\",\"price\":38.5,\"title\":\"拿铁\"}");
        BulkResponse bulkResponse = baseClient.saveOrUpdateDocBulkWithJson(INDEX_NAME, list);
        Assertions.assertEquals(false, bulkResponse.errors());
        Assertions.assertEquals(3, bulkResponse.items().size());
    }

    @Test
    void saveOrUpdateDocBulkWithList() throws IOException {
        List<Product> products = new ArrayList<>();
        products.add((Product) new Product().setPrice(BigDecimal.valueOf(12.8)).setCategory("零食").setBrand("亿滋").setTitle("奥利奥6片装").setId("14"));
        products.add((Product) new Product().setPrice(BigDecimal.valueOf(20.8)).setCategory("零食").setBrand("亿滋").setTitle("奥利奥12片装").setId("15"));
        products.add((Product) new Product().setPrice(BigDecimal.valueOf(68.8)).setCategory("零食").setBrand("亿滋").setTitle("奥利奥桶装").setId("16"));
        products.add((Product) new Product().setPrice(BigDecimal.valueOf(15.0)).setCategory("零食").setBrand("豪士").setTitle("红豆面包3片装").setId("17"));
        products.add((Product) new Product().setPrice(BigDecimal.valueOf(29.0)).setCategory("零食").setBrand("豪士").setTitle("红豆面包6片装").setId("18"));
        products.add((Product) new Product().setPrice(BigDecimal.valueOf(36.0)).setCategory("零食").setBrand("三只松鼠").setTitle("坚果分享装").setId("19"));
        products.add((Product) new Product().setPrice(BigDecimal.valueOf(108.0)).setCategory("零食").setBrand("三只松鼠").setTitle("坚果桶装").setId("20"));
        BulkResponse bulkResponse = baseClient.saveOrUpdateDocBulkWithList(INDEX_NAME, products);
        Assertions.assertEquals(false, bulkResponse.errors());
        Assertions.assertEquals(7, bulkResponse.items().size());
    }

    @Test
    void getSearchResponse() throws IOException {
        String json = "{" +
                "  \"size\": 20, " +
                "  \"query\": {" +
                "    \"match_all\": {}" +
                "  }" +
                "}";
        SearchResponse<Product> searchResponse = baseClient.getSearchResponse(INDEX_NAME, json, null, Product.class);
        System.out.println(searchResponse);
    }

    @Test
    void getSearchRequest() {
        String json = "{" +
                "  \"size\": 20, " +
                "  \"query\": {" +
                "    \"match_all\": {}" +
                "  }" +
                "}";
        SearchRequest searchRequest = baseClient.getSearchRequest(INDEX_NAME, json, null);
        System.out.println(searchRequest);
    }

    @Test
    void queryById() throws IOException {
        Product product = baseClient.queryById(INDEX_NAME, "1", Product.class);
        System.out.println(product);
        Assertions.assertEquals("图书", product.getCategory());
    }

    @Test
    void queryByKeywordSimple() throws IOException {
        List<Product> products = baseClient.queryByKeywordSimple(INDEX_NAME, "title", "苹果", Product.class);
        products.stream().forEach(product -> System.out.println(product));
    }

    @Test
    void queryByJson() throws IOException {
        String json = "{" +
                "  \"query\": {" +
                "    \"match\": {" +
                "      \"title\": \"红豆\"" +
                "    }\n" +
                "  }\n" +
                "}";
        List<Product> products = baseClient.queryByJson(INDEX_NAME, json, Product.class);
        products.stream().forEach(product -> System.out.println(product));
    }

    @Test
    void queryByJsonWithPage() throws IOException {
        String json = "{" +
                "  \"query\": {" +
                "    \"match_all\": {}" +
                "  }" +
                "}";
        int page = 2;
        int size = 10;
        Page<Product> p = baseClient.queryByJsonWithPage(INDEX_NAME, json, page, size, Product.class);
        p.getContent().stream().forEach(a -> System.out.println(a.getId()));
    }


    @Test
    void getStringTermsBucketByJson() throws IOException {
        String json = "{" +
                "  \"size\": 0," +
                "  \"aggs\": {" +
                "    \"brands\": {" +
                "      \"terms\": {" +
                "        \"field\": \"brand\"," +
                "        \"size\": 10" +
                "      }" +
                "    }" +
                "  }" +
                "}";
        List<StringTermsBucket> brands = baseClient.getStringTermsBucketByJson(INDEX_NAME, json, "brands");
        brands.stream().forEach(a -> System.out.println(a.key() + " 有 " + a.docCount() + " 件"));
    }

    @Test
    void getSuggestionsByJson() throws IOException {
        String json = "{" +
                "  \"suggest\": {" +
                "    \"titles\": {" +
                "      \"text\": \"m\"," +
                "      \"completion\": {" +
                "        \"field\": \"title.suggest_pinyin_first\"" +
                "      }" +
                "    }" +
                "  }" +
                "}";
        Set<String> titles = baseClient.getSuggestionsByJson(INDEX_NAME, json, "titles");
        titles.stream().forEach(title -> System.out.println(title));
    }

    @Test
    void getHistogramBucketByJson() throws IOException {
        String json = "{" +
                "  \"size\": 0," +
                "  \"aggs\": {" +
                "    \"prices\": {" +
                "      \"histogram\": {" +
                "        \"field\": \"price\"," +
                "        \"interval\": 50," +
                "        \"min_doc_count\": 1" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        List<HistogramBucket> prices = baseClient.getHistogramBucketByJson(INDEX_NAME, json, "prices");
        prices.stream().forEach(p -> System.out.println(p.key() + "-" + (p.key() + 50) + " 有 " + p.docCount() + "件"));
    }

    @Test
    void createQueryScript() throws IOException {
        String script = "{" +
                "  \"script\":{" +
                "    \"lang\": \"painless\", " +
                "    \"source\": \"ctx._source.price += params.value\"" +
                "  }" +
                "}";
        String scriptId = "add_price";
        String lang = "painless";
        boolean force = true;
        baseClient.createQueryScript(scriptId, script, lang, force);
    }

    @Test
    void hasScriptExist() throws IOException {
        String scriptId = "add_price";
        boolean r = baseClient.hasScriptExist(scriptId);
        System.out.println(r);
    }

    @Test
    void deleteScriptById() throws IOException {
        String scriptId = "add_price";
        boolean r = baseClient.deleteScriptById(scriptId);
        System.out.println(r);
    }

    @Test
    void queryBySimpleTemplate() throws IOException {
        List<Product> products = baseClient.queryBySimpleTemplate("product", "category", "笔记本", Product.class);
        products.stream().forEach(a -> System.out.println(a.toString()));
    }

    @Test
    void queryByScriptTemplate() throws IOException {
        String scriptId = "es-simple-script";
        Map<String, JsonData> map = new HashMap<>();
        map.put("field", JsonData.of("title"));
        map.put("value", JsonData.of("大米"));
        List<Product> products = baseClient.queryByScriptTemplate(INDEX_NAME, scriptId, map, Product.class);
        products.stream().forEach(a -> System.out.println(a.toString()));
    }

    @Test
    void testQueryByScriptTemplate() throws IOException {
        String scriptId = "es-simple-script";
        List<Product> products = baseClient.queryByScriptTemplate(a -> a.index(INDEX_NAME)
                        .id(scriptId)
                        .params("field", JsonData.of("title"))
                        .params("value", JsonData.of("大米"))
                , Product.class);
        products.stream().forEach(a -> System.out.println(a.toString()));
    }

    @Test
    void updateByQueryWithJson() throws IOException {
        String script = "ctx._source.price += params.value";
        String scriptId = "add_price";
        baseClient.createQueryScript(scriptId, script, "painless", true);
        String json = "{" +
                "  \"query\": {" +
                "    \"match_all\": {}" +
                "  }," +
                "  \"script\": {" +
                "    \"id\": \"price_add\"," +
                "    \"params\": {" +
                "      \"value\":0.05" +
                "    }" +
                "  }" +
                "}";
        Long r = baseClient.updateByQueryWithJson(INDEX_NAME, json);
        System.out.println(r);
    }
}