package com.kaveinga.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.kaveinga.elasticsearch.entity.Address;
import com.kaveinga.elasticsearch.entity.User;
import com.kaveinga.elasticsearch.utils.ObjectUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootTest
class HowToUseNestedQueryTests {

    @Value("${spring.datasource.name}")
    private String              database;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/7.x/query-dsl-nested-query.html
     */
    @Test
    void searchWithNestedQuery() {

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
        searchSourceBuilder.fetchSource(new String[]{"id", "firstName", "lastName", "addresses"}, new String[]{""});

        /**
         * Query with bool
         */
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        boolQuery.must(QueryBuilders.matchQuery("addresses.street", "304 W Stillwater Dr"));
        boolQuery.must(QueryBuilders.matchQuery("addresses.state", "UT"));
        boolQuery.must(QueryBuilders.matchQuery("addresses.city", "Saratoga Springs"));
        boolQuery.must(QueryBuilders.matchQuery("addresses.zipcode", "84045"));

        boolQuery.mustNot(QueryBuilders.matchQuery("state", "CA"));

        searchSourceBuilder.query(QueryBuilders.nestedQuery("addresses", boolQuery, ScoreMode.None));

        /**
         * Query with query string<br>
         * not working as "304 W Stillwater Dr" are tokenized which they should not be<br>
         */
        // StringBuilder stringQuery = new StringBuilder();
        // stringQuery.append("addresses.street:(304 W Stillwater Dr)");
        // QueryStringQueryBuilder queryStringQuery = QueryBuilders.queryStringQuery(stringQuery.toString());
        //
        // searchSourceBuilder.query(QueryBuilders.nestedQuery("addresses", queryStringQuery, ScoreMode.None));

        searchRequest.source(searchSourceBuilder);

        searchRequest.preference("nested-address");

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

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/7.x/query-dsl-nested-query.html<br>
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-distance-query.html
     */
    @Test
    void searchWithNestedQueryAndGeolocation() {

        int pageNumber = 0;
        int pageSize = 3;

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
        searchSourceBuilder.fetchSource(new String[]{"*"}, new String[]{"cards"});
        // searchSourceBuilder.fetchSource(new FetchSourceContext(true, new String[]{"*"}, new String[]{"cards"}));
        /**
         * Query with bool
         */
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        /**
         * Lehi skate park: 40.414897, -111.881186<br>
         * get locations/addresses close to skate park(from a radius).<br>
         * The geo_distance filter can work with multiple locations / points per document. Once a single location /
         * point matches the filter, the document will be included in the filter.
         */
        boolQuery.filter(QueryBuilders.geoDistanceQuery("addresses.location").point(40.414897, -111.881186).distance(1, DistanceUnit.MILES));

        searchSourceBuilder.query(QueryBuilders.nestedQuery("addresses", boolQuery, ScoreMode.None));

        searchRequest.source(searchSourceBuilder);

        searchRequest.preference("nested-address");

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
            log.warn("IOException, msg={}", e.getLocalizedMessage());
            e.printStackTrace();
        } catch (Exception e) {
            log.warn("Exception, msg={}", e.getLocalizedMessage());
            e.printStackTrace();
        }

    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/7.x/query-dsl-nested-query.html<br>
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-distance-query.html<br>
     * Query against nested objects and only retrieve nested objects<br>
     */
    @Test
    void searchWithNestedQueryAndRetrieveOnlyNestedObjects() {

        int pageNumber = 0;
        int pageSize = 3;

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
        searchSourceBuilder.fetchSource(new String[]{"*"}, new String[]{"cards"});
        // searchSourceBuilder.fetchSource(new FetchSourceContext(true, new String[]{"*"}, new String[]{"cards"}));
        /**
         * Query with bool
         */
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        /**
         * Lehi skate park: 40.414897, -111.881186<br>
         * get locations/addresses close to skate park(from a radius).<br>
         * The geo_distance filter can work with multiple locations / points per document. Once a single location /
         * point matches the filter, the document will be included in the filter.
         */
        boolQuery.filter(QueryBuilders.geoDistanceQuery("addresses.location").point(40.414897, -111.881186).distance(1, DistanceUnit.MILES));

        String innerObjName = "nestedAddresses";

        searchSourceBuilder.query(QueryBuilders.nestedQuery("addresses", boolQuery, ScoreMode.None).innerHit(new InnerHitBuilder(innerObjName)));

        searchRequest.source(searchSourceBuilder);

        searchRequest.preference("nested-address");

        if (searchSourceBuilder.sorts() != null && searchSourceBuilder.sorts().size() > 0) {
            log.info("\n{\n\"query\":{}, \"sort\":{}\n}", searchSourceBuilder.query().toString(), searchSourceBuilder.sorts().toString());
        } else {
            log.info("\n{\n\"query\":{}\n}", searchSourceBuilder.query().toString());
        }

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            log.info("isTimedOut={}, totalShards={}, totalHits={}", searchResponse.isTimedOut(), searchResponse.getTotalShards(), searchResponse.getHits().getTotalHits().value);

            List<Address> addresses = new ArrayList<Address>();
            Iterator<SearchHit> it = searchResponse.getHits().iterator();
            int rootCount = 1;
            while (it.hasNext()) {
                SearchHit searchHit = it.next();
                log.info("searchHit={}", ObjectUtils.toJson(searchHit));
                Map<String, SearchHits> innerHit = searchHit.getInnerHits();

                if (innerHit != null) {
                    SearchHits innerSearchHits = innerHit.get(innerObjName);

                    for (SearchHit innerSearchHit : innerSearchHits.getHits()) {
                        // log.info("rootCount={},
                        // innerSearchHit={}",rootCount,ObjectUtils.toJson(innerSearchHit.getSourceAsMap()));
                        Address obj = ObjectUtils.getObjectMapper().readValue(innerSearchHit.getSourceAsString(), new TypeReference<Address>() {});
                        addresses.add(obj);
                    }

                }
                rootCount++;
            }

            log.info("addresses={}", ObjectUtils.toJson(addresses));

        } catch (IOException e) {
            log.warn("IOException, msg={}", e.getLocalizedMessage());
            e.printStackTrace();
        } catch (Exception e) {
            log.warn("Exception, msg={}", e.getLocalizedMessage());
            e.printStackTrace();
        }

    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/7.x/query-dsl-nested-query.html
     */
    @Test
    void searchAllFieldsWithNestedQuery() {

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
        searchSourceBuilder.fetchSource(new String[]{"id", "firstName", "lastName", "description","cards.cardNumber","cards.swipes.merchantName", "dateOfBirth", "addresses.street", "addresses.city", "addresses.state", "addresses.zipcode"}, new String[]{""});

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
        
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        /**
         * Lehi skate park: 40.414897, -111.881186<br>
         * get locations/addresses close to skate park(from a radius).<br>
         * The geo_distance filter can work with multiple locations / points per document. Once a single location /
         * point matches the filter, the document will be included in the filter.
         */
        boolQuery.filter(QueryBuilders.nestedQuery("addresses", QueryBuilders.multiMatchQuery("Lehi", "*"), ScoreMode.None));
        
        boolQuery.filter(QueryBuilders.nestedQuery("cards.swipes", QueryBuilders.multiMatchQuery("Best Buy", "*"), ScoreMode.None));
        
        boolQuery.filter(QueryBuilders.multiMatchQuery("strongly dislikes sheep", "*"));

        searchSourceBuilder.query(boolQuery);

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

    private List<User> getResponseResult(SearchHits searchHits) {

        Iterator<SearchHit> it = searchHits.iterator();

        List<User> searchResults = new ArrayList<>();

        while (it.hasNext()) {
            SearchHit searchHit = it.next();
            //log.info("sourceAsString={}", searchHit.getSourceAsString());
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
