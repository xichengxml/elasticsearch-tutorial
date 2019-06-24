package com.xicheng.elasticsearch.demo.controller;

import com.google.gson.Gson;
import com.xicheng.elasticsearch.demo.config.EsConfig;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class SuggestController {

    // 相当于数据库名
    private static final String INDEXNAME = "es_suggest";
    // 相当于表名
    private static final String TYPENAME = "xicheng";

    private static final Gson GSON = new Gson();

    @Autowired
    private EsConfig esConfig;

    /**
     * 添加一条数据
     */
    @RequestMapping("/add")
    public String addData() throws Exception {
        TransportClient client = esConfig.getConnection();
        Map<String, Object> json = new HashMap<>();
        json.put("user", "测试");
        json.put("blog", "这是elasticsearch的测试案例");
        json.put("motoo", "to be better me");
        IndexResponse response = client.prepareIndex(INDEXNAME, TYPENAME)
                .setSource(json)
                .get();
        System.out.println("添加成功" + response.getResult());
        client.close();
        return "success";
    }

    @RequestMapping("/get")
    public String getAllData(String key, String value) throws Exception {
        TransportClient client = esConfig.getConnection();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery(key, value));
        SearchResponse response = client.prepareSearch(INDEXNAME)
                .setTypes(TYPENAME)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder)
                .execute()
                .actionGet();
        SearchHit[] hits = response.getHits().getHits();
        List<Map<String, Object>> dataList = new ArrayList<>();
        for (int i = 0; i < hits.length; i++) {
            Map<String, Object> stringObjectMap = hits[i].sourceAsMap();
            dataList.add(stringObjectMap);
        }
        client.close();
        return GSON.toJson(dataList);
    }

    @RequestMapping("/t-sug")
    public String termSuggest(String key, String value) throws Exception {
        TransportClient client = esConfig.getConnection();
        SearchResponse searchResponse = client.prepareSearch(INDEXNAME).suggest(new SuggestBuilder().addSuggestion("foo",
                SuggestBuilders.termSuggestion(key).text(value))).get();
        Suggest suggest = searchResponse.getSuggest();
        List<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> foo = suggest.getSuggestion("foo").getEntries();
        return GSON.toJson(responseToList(foo));
    }
    @RequestMapping("/p-sug")
    public String phraseSuggest(String key, String value) throws Exception {
        TransportClient client = esConfig.getConnection();
        SearchResponse searchResponse = client.prepareSearch(INDEXNAME).suggest(new SuggestBuilder().addSuggestion("foo",
                SuggestBuilders.phraseSuggestion(key).text(value))).get();
        Suggest suggest = searchResponse.getSuggest();
        List<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> foo = suggest.getSuggestion("foo").getEntries();
        return GSON.toJson(responseToList(foo));
    }
    @RequestMapping("/c-sug")
    public String completionSuggest(String key, String value) throws Exception {
        TransportClient client = esConfig.getConnection();
        SearchResponse searchResponse = client.prepareSearch(INDEXNAME).suggest(new SuggestBuilder().addSuggestion("foo",
                SuggestBuilders.completionSuggestion(key).text(value))).get();
        Suggest suggest = searchResponse.getSuggest();
        List<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> foo = suggest.getSuggestion("foo").getEntries();
        return GSON.toJson(responseToList(foo));
    }

    private List<String> responseToList(List<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> foo) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < foo.size(); i++) {
            Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option> entry = foo.get(i);
            List<? extends Suggest.Suggestion.Entry.Option> options = entry.getOptions();
            for (int j = 0; j < options.size(); j++) {
                String data = options.get(j).toString();
                result.add(data);
            }
        }
        return result;
    }
}
