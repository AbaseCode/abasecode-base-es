package com.abasecode.opencode.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.HistogramBucket;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.ObjectBuilder;
import com.abasecode.opencode.es.config.ElasticsearchConfig;
import com.abasecode.opencode.es.entity.BaseT;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.abasecode.opencode.es.util.ElasticsearchUtil.getList;
import static com.abasecode.opencode.es.util.ElasticsearchUtil.readJson;

/**
 * @author Jon
 * e-mail: ijonso123@gmail.com
 * url: <a href="https://jon.wiki">Jon's blog</a>
 * url: <a href="https://github.com/abasecode">project github</a>
 * url: <a href="https://abasecode.com">AbaseCode.com</a>
 */
@Component
public class ElasticsearchBaseClient<T extends BaseT> {

    @Autowired
    private ElasticsearchConfig.EsConfig esConfig;

    private final static String SIMPLE_SCRIPT_ID = "es-simple-script";
    private final static String DEFAULT_SCRIPT_LANG = "painless";

    private final static Integer PAGE_ONE = 1;
    private final static Integer PAGE_SIZE = 10;

    private ElasticsearchClient client;
    private RestClientTransport transport;

    /**
     * Instantiation
     */
    @Autowired
    public ElasticsearchBaseClient(ElasticsearchConfig.EsConfig esConfig) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(esConfig.getUsername(), esConfig.getPassword()));
        RestClientBuilder builder = RestClient
                .builder(getList(esConfig.getUris())
                        .stream()
                        .toArray(Node[]::new))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider));
        RestClient restClient = builder.build();
        this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
    }

    /**
     * Instantiation
     *
     * @param client    ElasticsearchClient
     * @param transport RestClientTransport
     */
    public ElasticsearchBaseClient(ElasticsearchClient client, RestClientTransport transport) {
        this.client = client;
        this.transport = transport;
    }

    /**
     * check if the index exists
     *
     * @param index index
     * @return boolean
     * @throws IOException
     */
    public boolean hasIndexExist(String index) throws IOException {
        if (null != index && !index.isEmpty()) {
            return client.indices()
                    .exists(a -> a.index(index))
                    .value();
        }
        return false;
    }

    /**
     * create index by json
     *
     * @param index
     * @param indexJson
     * @return boolean
     * @throws IOException
     */
    public boolean createIndex(String index, String indexJson) throws IOException {
        CreateIndexRequest request = CreateIndexRequest.of(a -> a
                .index(index)
                .withJson(new StringReader(indexJson)));
        return client.indices()
                .create(request)
                .acknowledged();
    }

    /**
     * create index by json file
     *
     * @param index
     * @param jsonPath
     * @return boolean
     * @throws IOException
     */
    public boolean createIndexByJsonFile(String index, String jsonPath) throws IOException {
        if (null == jsonPath || jsonPath.isEmpty()) {
            throw new IOException("File is not exist!");
        }
        File file = new File(jsonPath);
        FileInputStream stream = new FileInputStream(file);
        CreateIndexRequest request = CreateIndexRequest
                .of(a -> a.index(index)
                        .withJson(stream));
        return client.indices()
                .create(request)
                .acknowledged();
    }

    /**
     * delete index
     *
     * @param index
     * @return boolean
     * @throws IOException
     */
    public boolean deleteIndex(String index) throws IOException {
        if (null != index && !index.isEmpty()) {
            if (hasIndexExist(index)) {
                return client.indices()
                        .delete(a -> a.index(index))
                        .acknowledged();
            }
        }
        return false;
    }

    /**
     * Check if the doc exists
     *
     * @param index index
     * @param id    doc id
     * @return boolean
     * @throws IOException
     */
    public boolean hasDocExist(String index, String id) throws IOException {
        if (null != index && !index.isEmpty()) {
            return client.exists(a -> a.index(index).id(id)).value();
        }
        return false;
    }

    /**
     * delete doc by id
     *
     * @param index index
     * @param id    doc id
     * @return string : deleted or not_found
     * @throws IOException
     */
    public String deleteDoc(String index, String id) throws IOException {
        DeleteResponse r = client.delete(DeleteRequest.of(a -> a.index(index).id(id)));
        return r.result().jsonValue();
    }

    /**
     * delete doc by list<Id>
     *
     * @param index index
     * @param ids   list<String>
     * @return BulkResponse
     * @throws IOException
     */
    public BulkResponse deleteBulkWithList(String index, List<String> ids) throws IOException {
        BulkRequest.Builder builder = new BulkRequest.Builder();
        ids.stream().forEach(id -> {
            builder.operations(o -> o.delete(d -> d.index(index).id(id)));
        });
        return client.bulk(builder.build());
    }

    /**
     * save or update doc
     *
     * @param index    index
     * @param json     json
     * @param id       id
     * @param hasForce has force
     * @return String，updated or created
     * @throws IOException
     */
    public String saveOrUpdateDocByJson(String index, String json, String id, Boolean hasForce) throws IOException {
        boolean b = hasDocExist(index, id);
        if (b) {
            if (hasForce) {
                return saveOrUpdateDocByJson(index, json, id);
            }
            return "exists";
        }
        return saveOrUpdateDocByJson(index, json, id);
    }

    /**
     * save or update doc
     *
     * @param index index
     * @param json  json
     * @param id    doc id
     * @return String，updated or created
     * @throws IOException
     */
    private String saveOrUpdateDocByJson(String index, String json, String id) throws IOException {
        IndexResponse r = client.index(IndexRequest.of(a -> a
                .index(index)
                .id(id)
                .withJson(new StringReader(json))));
        return r.result().jsonValue();
    }

    /**
     * save or update doc by json file
     *
     * @param index    index
     * @param file     file
     * @param id       doc id
     * @param hasForce has force
     * @return String，updated or created
     * @throws IOException
     */
    public String saveOrUpdateDocByJsonFile(String index, String file, String id, Boolean hasForce) throws IOException {
        boolean b = hasDocExist(index, id);
        if (b) {
            if (hasForce) {
                return saveOrUpdateDocByJsonFile(index, file, id);
            }
            return "exists";
        }
        return saveOrUpdateDocByJsonFile(index, file, id);
    }

    /**
     * save or update doc by json file
     *
     * @param index index
     * @param file  file
     * @param id    doc id
     * @return String，updated or created
     * @throws IOException
     */
    private String saveOrUpdateDocByJsonFile(String index, String file, String id) throws IOException {
        FileInputStream stream = new FileInputStream(file);
        IndexResponse r = client.index(IndexRequest.of(a -> a
                .index(index)
                .id(id)
                .withJson(stream)));
        return r.result().jsonValue();
    }

    /**
     * save or update by T
     *
     * @param index    index
     * @param t        T
     * @param hasForce
     * @return String，updated or created
     * @throws IOException
     */
    public String saveOrUpdateDoc(String index, T t, boolean hasForce) throws IOException {
        boolean b = hasDocExist(index, t.getId());
        if (b) {
            if (hasForce) {
                return saveOrUpdateDoc(index, t);
            }
            return "exists";
        }
        return saveOrUpdateDoc(index, t);
    }

    /**
     * save or upate by T
     *
     * @param index index
     * @param t     T
     * @return String，updated or created
     * @throws IOException
     */
    private String saveOrUpdateDoc(String index, T t) throws IOException {
        IndexResponse r = client.index(IndexRequest.of(a -> a
                .index(index)
                .id(t.getId())
                .document(t)));
        return r.result().jsonValue();
    }

    /**
     * save or update doc by bulk mode
     * Note: json needs to be configured with id
     *
     * @param index index
     * @param path  path
     * @return BulkResponse
     * @throws IOException
     */
    public BulkResponse saveOrUpdateDocBulkWithJsonFiles(String index, String path) throws IOException {
        if (null == path || path.isEmpty()) {
            throw new IOException("path cannot be empty！");
        }
        File[] files = new File(path).listFiles(f -> f.getName().matches(".*\\.json"));
        BulkRequest.Builder builder = new BulkRequest.Builder();
        for (File file : files) {
            JsonData json = readJson(new FileInputStream(file), client);
            builder.operations(o -> o
                    .index(i -> i.index(index)
                            .id(json.toJson().asJsonObject().getString("id"))
                            .document(json)));
        }
        return client.bulk(builder.build());
    }

    /**
     * save or update doc by list<json>
     *
     * @param index index
     * @param jsons json list
     * @return BulkResponse
     * @throws IOException
     */
    public BulkResponse saveOrUpdateDocBulkWithJson(String index, List<String> jsons) throws IOException {
        if (null == jsons || jsons.isEmpty()) {
            throw new IOException("jsons cannot be empty！");
        }
        BulkRequest.Builder builder = new BulkRequest.Builder();
        jsons.stream().forEach(a -> {
            JsonData json = readJson(new ByteArrayInputStream(a.getBytes()), client);
            builder.operations(o -> o
                    .index(i -> i.index(index)
                            .id(json.toJson().asJsonObject().getString("id"))
                            .document(json)));
        });
        return client.bulk(builder.build());
    }

    /**
     * save or update doc by List<T>
     *
     * @param index index
     * @param list  lsit T
     * @return BulkResponse
     * @throws IOException
     */
    public BulkResponse saveOrUpdateDocBulkWithList(String index, List<T> list) throws IOException {
        if (null == list || list.isEmpty()) {
            throw new IOException("list cannot be empty！");
        }
        BulkRequest.Builder builder = new BulkRequest.Builder();
        list.stream().forEach(t -> {
            builder.operations(o -> o.index(i -> i
                    .index(index)
                    .id(t.getId())
                    .document(t)));
        });
        return client.bulk(builder.build());
    }

    /**
     * get searchResponse
     *
     * @param index index
     * @param json  json
     * @return SearchResponse
     * @throws IOException
     */
    private SearchResponse<Void> getSearchResponse(String index, String json, PageRequest pageRequest) throws IOException {
        return client.search(getSearchRequest(index, json, pageRequest), Void.class);
    }

    /**
     * get searchResponse by json
     *
     * @param index       index
     * @param json        json
     * @param pageRequest pageRequest, nullable
     * @param clazz       class
     * @return SearchResponse<T>
     * @throws IOException
     */
    public SearchResponse<T> getSearchResponse(String index, String json, PageRequest pageRequest, Class<T> clazz) throws IOException {
        return client.search(getSearchRequest(index, json, pageRequest), clazz);
    }

    /**
     * get searchRequest by json
     *
     * @param index       index
     * @param json        json
     * @param pageRequest pageRequest, nullable
     * @return SearchRequest
     */
    public SearchRequest getSearchRequest(String index, String json, PageRequest pageRequest) {
        if (pageRequest == null) {
            return SearchRequest.of(a -> a
                    .index(index)
                    .withJson(new StringReader(json))
                    .ignoreUnavailable(true)
            );
        }
        int pageSize = pageRequest.getPageSize() <= 0 ? PAGE_SIZE : pageRequest.getPageSize();
        int pageNumber = pageRequest.getPageNumber() <= 0 ? PAGE_ONE : pageRequest.getPageNumber();
        int fromNum = pageSize * (pageNumber - 1);
        return SearchRequest.of(a -> a
                .index(index)
                .withJson(new StringReader(json))
                .ignoreUnavailable(true)
                .size(pageSize)
                .from(fromNum));
    }

    /**
     * query by id
     *
     * @param index index
     * @param id    id
     * @param clazz class
     * @return T
     * @throws IOException
     */
    public T queryById(String index, String id, Class<T> clazz) throws IOException {
        GetResponse<T> response = client.get(g -> g
                .index(index)
                .id(id), clazz);
        if (response.found()) {
            return response.source();
        }
        return null;
    }

    /**
     * query by keyword simple match
     *
     * @param index   index
     * @param field   field
     * @param keyword keyword
     * @param clazz   class
     * @return List<T>
     * @throws IOException
     */
    public List<T> queryByKeywordSimple(String index, String field, String keyword, Class<T> clazz) throws IOException {
        SearchResponse<T> response = client.search(s -> s
                .index(index)
                .query(q -> q.match(t -> t
                        .field(field)
                        .query(keyword))), clazz);
        List<T> list = new ArrayList<>();
        response.hits().hits().stream().forEach(t -> list.add(t.source()));
        return list;
    }

    /**
     * query by json
     *
     * @param index index
     * @param json  json
     * @param clazz class
     * @return List<T>
     * @throws IOException
     */
    public List<T> queryByJson(String index, String json, Class<T> clazz) throws IOException {
        SearchResponse<T> response = getSearchResponse(index, json, null, clazz);
        List<T> list = new ArrayList<>();
        response.hits().hits().stream().forEach(t -> list.add(t.source()));
        return list;
    }

    /**
     * query fields by json.
     * notice: Json must specify the fields to be returned
     * @param index index
     * @param json json
     * @return List<Map<String, JsonData>>
     * @throws IOException
     */
    public List<Map<String, JsonData>> queryFieldsByJson(String index, String json) throws IOException {
        SearchResponse<Object> response = client.search(getSearchRequest(index, json, null), Object.class);
        List<Map<String, JsonData>> list = new ArrayList<>();
        response.hits().hits().stream().forEach(t -> list.add(t.fields()));
        return list;
    }

    /**
     * query by json with page
     *
     * @param index       index
     * @param json        json
     * @param pageRequest pageRequest
     * @param clazz       class
     * @return Page<T>
     * @throws IOException
     */
    public Page<T> queryByJsonWithPage(String index, String json, PageRequest pageRequest, Class<T> clazz) throws IOException {
        SearchResponse<T> response = getSearchResponse(index, json, pageRequest, clazz);
        List<T> list = new ArrayList<>();
        response.hits().hits().stream().forEach(t -> list.add(t.source()));
        return new PageImpl<>(list, pageRequest, response.hits().total().value());
    }

    /**
     * query by json with page
     *
     * @param index    index
     * @param json     json
     * @param pageNum  pageNum
     * @param pageSize pageSize
     * @param clazz    class
     * @return Page<T>
     * @throws IOException
     */
    public Page<T> queryByJsonWithPage(String index, String json, Integer pageNum, Integer pageSize, Class<T> clazz) throws IOException {
        PageRequest pageRequest = PageRequest.of(pageNum, pageSize);
        return queryByJsonWithPage(index, json, pageRequest, clazz);
    }

    /**
     * query fields by json with page
     * notice: Json must specify the fields to be returned
     * @param index index
     * @param json json
     * @param pageRequest pageRequest
     * @return Page<T>
     * @throws IOException
     */
    public Page<Map<String, JsonData>> queryFieldsByJsonWithPage(String index, String json, PageRequest pageRequest) throws IOException {
        SearchResponse<Object> response = client.search(getSearchRequest(index, json, pageRequest), Object.class);
        List<Map<String, JsonData>> list = new ArrayList<>();
        response.hits().hits().stream().forEach(t -> list.add(t.fields()));
        return new PageImpl<>(list, pageRequest, response.hits().total().value());
    }

    /**
     * query by json with page
     * notice: Json must specify the fields to be returned
     * @param index    index
     * @param json     json
     * @param pageNum  pageNum
     * @param pageSize pageSize
     * @return Page<T>
     * @throws IOException
     */
    public Page<Map<String, JsonData>> queryFieldsByJsonWithPage(String index, String json, Integer pageNum, Integer pageSize) throws IOException {
        PageRequest pageRequest = PageRequest.of(pageNum, pageSize);
        return queryFieldsByJsonWithPage(index, json, pageRequest);
    }

    /**
     * get stringTermsBucket by json
     *
     * @param index           index
     * @param aggregationJson json
     * @param aggName         agg name
     * @return set<String>
     * @throws IOException
     */
    public List<StringTermsBucket> getStringTermsBucketByJson(String index, String aggregationJson, String aggName) throws IOException {
        SearchResponse<Void> response = getSearchResponse(index, aggregationJson, null);
        return response.aggregations()
                .get(aggName)
                .sterms()
                .buckets()
                .array();
    }

    /**
     * get suggestion sets by json
     *
     * @param index          index
     * @param suggestionJson suggestion json
     * @param suggestName    suggest name
     * @return Set<String>
     * @throws IOException
     */
    public Set<String> getSuggestionsByJson(String index, String suggestionJson, String suggestName) throws IOException {
        SearchResponse<Void> searchResponse = getSearchResponse(index, suggestionJson, null);
        return searchResponse
                .suggest()
                .get(suggestName)
                .get(0)
                .completion()
                .options()
                .stream()
                .map(a -> a.text())
                .collect(Collectors.toSet());
    }

    /**
     * get histogramBucket by json
     *
     * @param index           index
     * @param aggregationJson aggregationJson
     * @param aggName         agg name
     * @return List<HistogramBucket>
     * @throws IOException
     */
    public List<HistogramBucket> getHistogramBucketByJson(String index, String aggregationJson, String aggName) throws IOException {
        SearchResponse<Void> response = getSearchResponse(index, aggregationJson, null);
        List<HistogramBucket> list = response.aggregations()
                .get(aggName)
                .histogram()
                .buckets()
                .array();
        return list;
    }

    /**
     * create script
     *
     * @param scriptId     script id
     * @param scriptSource script source, Content of source
     * @param language     script language
     * @param force        has force
     * @throws IOException
     */
    public void createQueryScript(String scriptId, String scriptSource, String language, boolean force) throws IOException {
        if (hasScriptExist(scriptId)) {
            if (force) {
                deleteScriptById(scriptId);
                createQueryScript(scriptId, scriptSource, language);
            }
        } else {
            createQueryScript(scriptId, scriptSource, language);
        }
    }

    /**
     * create script
     *
     * @param scriptId     script id
     * @param scriptSource script template
     * @param language     language
     * @throws IOException
     */
    private void createQueryScript(String scriptId, String scriptSource, String language) throws IOException {
        if (language == null || language.isEmpty()) {
            language = DEFAULT_SCRIPT_LANG;
        }
        String finalLanguage = language;
        client.putScript(r -> r.id(scriptId)
                .script(s -> s
                        .lang(finalLanguage)
                        .source(scriptSource)));
    }

    /**
     * create simple script template
     */
    private void createSimpleScriptTemplate() throws IOException {
        if (!hasScriptExist(SIMPLE_SCRIPT_ID)) {
            String scriptTemplate = "{\"query\":{\"match\":{\"{{field}}\":\"{{value}}\"}}}";
            createQueryScript(SIMPLE_SCRIPT_ID, scriptTemplate, "mustache");
        }
    }

    /**
     * check script exist
     *
     * @param scriptId scriptId
     * @return boolean
     * @throws IOException
     */
    public boolean hasScriptExist(String scriptId) throws IOException {
        GetScriptResponse script = client.getScript(GetScriptRequest.of(a -> a.id(scriptId)));
        return script.found();
    }

    /**
     * delete script template
     *
     * @param scriptId
     * @return boolean
     * @throws IOException
     */
    public boolean deleteScriptById(String scriptId) throws IOException {
        if (hasScriptExist(scriptId)) {
            DeleteScriptResponse deleteScriptResponse = client.deleteScript(DeleteScriptRequest.of(a -> a.id(scriptId)));
            return deleteScriptResponse.acknowledged();
        }
        return false;
    }

    /**
     * query by simple script template
     * <p>
     * DSL example:
     * GET /product/_search/template
     * {
     * "id":"es-simple-script",
     * "params": {
     * "field":"title",
     * "value":"大米"
     * }
     * }
     *
     * @param index index
     * @param field field
     * @param value value
     * @param clazz class
     * @return list<T>
     * @throws IOException
     */
    public List<T> queryBySimpleTemplate(String index, String field, String value, Class<T> clazz) throws IOException {
        createSimpleScriptTemplate();
        return queryByScriptTemplate(a -> a
                .index(index)
                .id(SIMPLE_SCRIPT_ID)
                .params("field", JsonData.of(field))
                .params("value", JsonData.of(value)), clazz);
    }

    /**
     * query by script template
     *
     * @param fn    fn
     * @param clazz class
     * @return list<T>
     * @throws IOException
     */
    public List<T> queryByScriptTemplate(Function<SearchTemplateRequest.Builder, ObjectBuilder<SearchTemplateRequest>> fn,
                                         Class<T> clazz) throws IOException {
        SearchTemplateResponse<T> response = client.searchTemplate(fn, clazz);
        List<T> list = new ArrayList<>();
        response.hits().hits().stream().forEach(a -> list.add(a.source()));
        return list;
    }

    /**
     * query by script template
     *
     * @param index    index
     * @param scriptId scriptId
     * @param map      map
     * @param clazz    class
     * @return List<T>
     * @throws IOException
     */
    public List<T> queryByScriptTemplate(String index, String scriptId, Map<String, JsonData> map, Class<T> clazz) throws IOException {
        return queryByScriptTemplate(s -> s.index(index)
                .id(SIMPLE_SCRIPT_ID)
                .params(map), clazz);
    }

    /**
     * update by query with json
     * <p>
     * DSL example
     * first: create script
     * PUT /_scripts/price_add
     * {
     * "script":{
     * "lang": "painless",
     * "source": "ctx._source.price += params.value"
     * }
     * }
     * and then:
     * POST /product/_update_by_query
     * {
     * "query": {
     * "match_all": {}
     * },
     * "script": {
     * "id": "price_add",
     * "params": {
     * "value": 5
     * }
     * }
     * }
     * all price will be increased by 5
     * java code reference test
     *
     * @param index index
     * @param json  json
     * @return Long
     * @throws IOException
     */
    public Long updateByQueryWithJson(String index, String json) throws IOException {
        return updateByQuery(index, json).updated();
    }

    /**
     * get UpdateByQueryResponse
     *
     * @param index index
     * @param json  json
     * @return UpdateByQueryResponse
     * @throws IOException
     */
    private UpdateByQueryResponse updateByQuery(String index, String json) throws IOException {
        return client.updateByQuery(a -> a.index(index)
                .withJson(new StringReader(json)));
    }
}
