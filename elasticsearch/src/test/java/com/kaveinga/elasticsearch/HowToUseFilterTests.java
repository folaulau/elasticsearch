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
import org.elasticsearch.common.geo.GeoPoint;
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
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
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
class HowToUseFilterTests {

    @Value("${spring.datasource.name}")
    private String              database;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/filter-search-results.html
     */
    @Test
    void filterQuery() {

        int pageNumber = 0;
        int pageSize = 5;

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
        searchSourceBuilder.fetchSource(new String[]{"id", "firstName", "lastName", "rating", "dateOfBirth"}, new String[]{""});

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        boolQuery.filter(QueryBuilders.termQuery("rating", 5));

        searchSourceBuilder.query(boolQuery);

        searchRequest.source(searchSourceBuilder);

        searchRequest.preference("rating");

        if (searchSourceBuilder.query() != null && searchSourceBuilder.sorts() != null && searchSourceBuilder.sorts().size() > 0) {
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
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
     */
    @Test
    void filterQueryWithRange() {

        int pageNumber = 0;
        int pageSize = 5;

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
        searchSourceBuilder.fetchSource(new String[]{"id", "firstName", "lastName", "rating", "dateOfBirth"}, new String[]{""});

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        boolQuery.filter(QueryBuilders.rangeQuery("rating").gte(2).lte(4));

        searchSourceBuilder.query(boolQuery);

        searchRequest.source(searchSourceBuilder);

        searchRequest.preference("rating");

        if (searchSourceBuilder.query() != null && searchSourceBuilder.sorts() != null && searchSourceBuilder.sorts().size() > 0) {
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
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
     */
    @Test
    void filterQueryWithExists() {

        int pageNumber = 0;
        int pageSize = 5;

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
        searchSourceBuilder.fetchSource(new String[]{"id", "firstName", "lastName", "rating", "dateOfBirth"}, new String[]{""});

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        boolQuery.filter(QueryBuilders.existsQuery("lastLoggedInAt"));

        searchSourceBuilder.query(boolQuery);

        searchRequest.source(searchSourceBuilder);

        searchRequest.preference("rating");

        if (searchSourceBuilder.query() != null && searchSourceBuilder.sorts() != null && searchSourceBuilder.sorts().size() > 0) {
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
