package com.abasecode.opencode.es.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpMapper;
import jakarta.json.spi.JsonProvider;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jon
 * e-mail: ijonso123@gmail.com
 * url: <a href="https://jon.wiki">Jon's blog</a>
 * url: <a href="https://github.com/abasecode">project github</a>
 * url: <a href="https://abasecode.com">AbaseCode.com</a>
 */
public class ElasticsearchUtil {

    /**
     * get list<Node> from string list
     *
     * @param uris list<String>
     * @return List<Node>
     */
    public static List<Node> getList(List<String> uris) {
        if (uris == null) {
            return null;
        }
        if (uris.isEmpty()) {
            return null;
        }
        List<Node> list = new ArrayList<>();
        for (int i = 0; i < uris.size(); i++) {
            String[] s = uris.get(i).toString().split("://");
            String[] hosts = s[1].split(":");
            HttpHost httpHost = new HttpHost(hosts[0].replace("//", ""), Integer.parseInt(hosts[1]), s[0]);
            list.add(new Node(httpHost));
        }
        return list;
    }

    /**
     * read json from stream
     *
     * @param input    InputStream
     * @param esClient ElasticsearchClient
     * @return JsonData
     */
    public static JsonData readJson(InputStream input, ElasticsearchClient esClient) {
        JsonpMapper jsonpMapper = esClient._transport().jsonpMapper();
        JsonProvider jsonProvider = jsonpMapper.jsonProvider();

        return JsonData.from(jsonProvider.createParser(input), jsonpMapper);
    }
}
