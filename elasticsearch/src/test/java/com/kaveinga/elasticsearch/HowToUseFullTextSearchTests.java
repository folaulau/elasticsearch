package com.kaveinga.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.kaveinga.elasticsearch.entity.User;
import com.kaveinga.elasticsearch.utils.ObjectUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootTest
class HowToUseFullTextSearchTests {

    @Value("${spring.datasource.name}")
    private String              database;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    void searchWithMatch() {

        /**
         * include two first names to illustrate contain
         */
        String firstName = "Leland Isabell";

        int pageNumber = 0;
        int pageSize = 10;

        SearchRequest searchRequest = new SearchRequest(database);
        searchRequest.allowPartialSearchResults(true);
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(pageNumber * pageSize);
        searchSourceBuilder.size(pageSize);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        /**
         * fetch only a few fields
         */
        // searchSourceBuilder.fetchSource(new String[]{ "id", "firstName", "lastName", "cards" }, new String[]{""});

        /**
         * Query
         */

        /**
         * Filter<br>
         * match query is like contain in mysql<br>
         * if firstName is a phrase like John the second. Any records with firstName contain John or second will return
         */

        searchSourceBuilder.query(QueryBuilders.matchQuery("firstName", firstName));

        searchRequest.source(searchSourceBuilder);

        if (searchSourceBuilder.sorts() != null && searchSourceBuilder.sorts().size() > 0) {
            log.info("\n{\n\"query\":{}, \"sort\":{}\n}", searchSourceBuilder.query().toString(), searchSourceBuilder.sorts().toString());
        } else {
            log.info("\n{\n\"query\":{}\n}", searchSourceBuilder.query().toString());
        }

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            log.info("totalShards={}, totalHits={}", searchResponse.getTotalShards(), searchResponse.getHits().getTotalHits().value);

            List<User> users = getResponseResult(searchResponse.getHits());

            /**
             * Result<br>
             * 
             */

            log.info("results={}", ObjectUtils.toJson(users));

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Test
    void searchWithMatchPhrase() {

        String description = "His biggest fear";

        int pageNumber = 0;
        int pageSize = 10;

        SearchRequest searchRequest = new SearchRequest(database);
        searchRequest.allowPartialSearchResults(true);
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(pageNumber * pageSize);
        searchSourceBuilder.size(pageSize);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        /**
         * fetch only a few fields
         */
        // searchSourceBuilder.fetchSource(new String[]{ "id", "firstName", "lastName", "cards" }, new String[]{""});

        /**
         * Query
         */

        /**
         * Filter<br>
         * match query is like contain in mysql
         */
        searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("description", description));

        searchRequest.source(searchSourceBuilder);

        if (searchSourceBuilder.sorts() != null && searchSourceBuilder.sorts().size() > 0) {
            log.info("\n{\n\"query\":{}, \"sort\":{}\n}", searchSourceBuilder.query().toString(), searchSourceBuilder.sorts().toString());
        } else {
            log.info("\n{\n\"query\":{}\n}", searchSourceBuilder.query().toString());
        }

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            log.info("totalShards={}, totalHits={}", searchResponse.getTotalShards(), searchResponse.getHits().getTotalHits().value);

            List<User> users = getResponseResult(searchResponse.getHits());

            log.info("results={}", ObjectUtils.toJson(users));

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Test
    void searchWithMultiMatchAllFields() {

        int pageNumber = 0;
        int pageSize = 10;

        SearchRequest searchRequest = new SearchRequest(database);
        searchRequest.allowPartialSearchResults(true);
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(pageNumber * pageSize);
        searchSourceBuilder.size(pageSize);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        /**
         * fetch only a few fields
         */
        // searchSourceBuilder.fetchSource(new String[]{ "id", "firstName", "lastName", "cards" }, new String[]{""});

        /**
         * Query
         */

        /**
         * Filter<br>
         * match query is like contain in mysql<br>
         * * means all fields<br>
         * Isabell - firstName of a diff user <br>
         * 3102060312 - phoneNumber of a diff user<br>
         * biggest fear - description of a diff user<br>
         */

        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("Isabell 3102060312 biggest fear", "*"));

        /**
         * query against swipe(nested object) merchantName but did not return anything which it should.<br>
         * * does not work with nested fields
         */
        // searchSourceBuilder.query(QueryBuilders.multiMatchQuery("Best Buy", "*"));

        searchRequest.preference("multi-fields");

        searchRequest.source(searchSourceBuilder);

        if (searchSourceBuilder.sorts() != null && searchSourceBuilder.sorts().size() > 0) {
            log.info("\n{\n\"query\":{}, \"sort\":{}\n}", searchSourceBuilder.query().toString(), searchSourceBuilder.sorts().toString());
        } else {
            log.info("\n{\n\"query\":{}\n}", searchSourceBuilder.query().toString());
        }

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            log.info("totalShards={}, totalHits={}", searchResponse.getTotalShards(), searchResponse.getHits().getTotalHits().value);

            List<User> users = getResponseResult(searchResponse.getHits());

            log.info("results={}", ObjectUtils.toJson(users));

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html<br>
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax<br>
     * You can use the query_string query to create a complex search that includes wildcard characters, searches across
     * multiple fields, and more.<br>
     * 
     * Because it returns an error for any invalid syntax, we don’t recommend using the query_string query for search
     * boxes.<br>
     * If you don’t need to support a query syntax, consider using the match query. If you need the features of a query
     * syntax, use the simple_query_string query, which is less strict.<br>
     * 
     * Note that query string can be written using bool query<br>
     */
    @Test
    void searchWithQueryString() {

        int pageNumber = 0;
        int pageSize = 10;

        SearchRequest searchRequest = new SearchRequest(database);
        searchRequest.allowPartialSearchResults(true);
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(pageNumber * pageSize);
        searchSourceBuilder.size(pageSize);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        /**
         * fetch only a few fields
         */
        searchSourceBuilder.fetchSource(new String[]{"id", "firstName", "lastName", "dateOfBirth", "description", "status"}, new String[]{""});

        /**
         * Query<br>
         */

        /**
         * any documents that contain "description" in any fields
         */
        // searchSourceBuilder.query(QueryBuilders.queryStringQuery("His biggest fear").minimumShouldMatch("3"));

        /**
         * dateOfBirth before 1990-01-01
         */
        // searchSourceBuilder.query(QueryBuilders.queryStringQuery("dateOfBirth:{* TO 1990-01-01}"));

        /**
         * The preferred operators are + (this term must be present) and - (this term must not be present). All other
         * terms are optional.<br>
         * quick brown +fox -news<br>
         * The familiar boolean operators AND, OR and NOT (also written &&, || and !) are also supported but beware that
         * they do not honor the usual precedence rules, so parentheses should be used whenever multiple operators are
         * used together.<br>
         */

        // OR is optional as it's the default operator
        // searchSourceBuilder.query(QueryBuilders.queryStringQuery("description:(greedy NOT haula) OR status:(ACTIVE OR
        // SUSPENDED)"));

        // using AND
        // searchSourceBuilder.query(QueryBuilders.queryStringQuery("description:(greedy NOT haula) AND status:(ACTIVE
        // OR SUSPENDED)"));

        // using minimumShouldMatch
        searchSourceBuilder.query(QueryBuilders.queryStringQuery("description:(greedy NOT haula) OR status:(ACTIVE OR SUSPENDED) OR dateOfBirth:(1985-09-26)").minimumShouldMatch("3"));

        searchRequest.source(searchSourceBuilder);

        if (searchSourceBuilder.sorts() != null && searchSourceBuilder.sorts().size() > 0) {
            log.info("\n{\n\"query\":{}, \"sort\":{}\n}", searchSourceBuilder.query().toString(), searchSourceBuilder.sorts().toString());
        } else {
            log.info("\n{\n\"query\":{}\n}", searchSourceBuilder.query().toString());
        }

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            log.info("totalShards={}, totalHits={}", searchResponse.getTotalShards(), searchResponse.getHits().getTotalHits().value);

            List<User> users = getResponseResult(searchResponse.getHits());

            log.info("results={}", ObjectUtils.toJson(users));

        } catch (IOException e) {
            log.warn("IOException, msg={}", e.getLocalizedMessage());
            e.printStackTrace();
        } catch (Exception e) {
            log.warn("Exception, msg={}", e.getLocalizedMessage());
            e.printStackTrace();
        }

    }

    @Test
    void searchWithExtraRequestConfigs() {

        String firstName = "Ayla";

        int pageNumber = 0;
        int pageSize = 10;

        SearchRequest searchRequest = new SearchRequest(database);
        searchRequest.allowPartialSearchResults(true);
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(pageNumber * pageSize);
        searchSourceBuilder.size(pageSize);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        /**
         * fetch only a few fields
         */
        // searchSourceBuilder.fetchSource(new String[]{ "id", "firstName", "lastName", "cards" }, new String[]{""});

        /**
         * Query
         */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        /**
         * Filter<br>
         * match query is like contain in mysql
         */
        boolQueryBuilder.filter(QueryBuilders.matchQuery("firstName", firstName));

        searchSourceBuilder.query(boolQueryBuilder);

        /**
         * The timeout parameter tells the coordinating node how long it should wait before giving up and just returning
         * the results that it already has. It can be better to return some results than none at all.
         */
        searchSourceBuilder.timeout(TimeValue.timeValueMinutes(2));

        searchRequest.source(searchSourceBuilder);

        searchRequest.searchType(SearchType.DEFAULT);

        /**
         * preference parameter allows you to control which shards or nodes are used to handle the search request.<br>
         * https://www.elastic.co/guide/en/elasticsearch/reference/current/search-shard-routing.html<br>
         * 
         */
        searchRequest.preference("firstName");

        if (searchSourceBuilder.sorts() != null && searchSourceBuilder.sorts().size() > 0) {
            log.info("\n{\n\"query\":{}, \"sort\":{}\n}", searchSourceBuilder.query().toString(), searchSourceBuilder.sorts().toString());
        } else {
            log.info("\n{\n\"query\":{}\n}", searchSourceBuilder.query().toString());
        }

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            log.info("isTimedOut={}, totalShards={}, totalHits={}", searchResponse.isTimedOut(), searchResponse.getTotalShards(), searchResponse.getHits().getTotalHits().value);

            List<User> users = getResponseResult(searchResponse.getHits());

            log.info("results={}", ObjectUtils.toJson(users));

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private List<User> getResponseResult(SearchHits searchHits) {

        Iterator<SearchHit> it = searchHits.iterator();

        List<User> searchResults = new ArrayList<>();

        while (it.hasNext()) {
            SearchHit searchHit = it.next();
            // log.info("sourceAsString={}", searchHit.getSourceAsString());
            try {

                User obj = ObjectUtils.getObjectMapper().readValue(searchHit.getSourceAsString(), new TypeReference<User>() {});
                // log.info("obj={}", ObjectUtils.toJson(obj));

                searchResults.add(obj);
            } catch (IOException e) {
                log.warn("IOException, msg={}", e.getLocalizedMessage());
            }
        }

        return searchResults;

    }

}
