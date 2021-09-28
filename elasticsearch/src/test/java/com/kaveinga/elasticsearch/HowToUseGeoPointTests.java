package com.kaveinga.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
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
class HowToUseGeoPointTests {

    @Value("${spring.datasource.name}")
    private String              database;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/7.x/query-dsl-nested-query.html<br>
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-distance-query.html<br>
     */
    @Test
    void searchWithGeopoint() {

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
         * point matches the filter, the document will be included in the filter.<br>
         */
        boolQuery.filter(QueryBuilders.geoDistanceQuery("addresses.location").point(40.414897, -111.881186)
                .distance(1, DistanceUnit.MILES).geoDistance(GeoDistance.ARC));

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
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-distance-query.html
     */
    @Test
    void searchWithGeoBoundingBox() {

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
        searchSourceBuilder.fetchSource(new String[]{"id","firstName","lastName","addresses.street","addresses.city","addresses.zipcode"}, new String[]{"cards"});
        /**
         * Query with bool
         */
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        /**
         * topLeft: herriman<br>
         * bottomRight: american folk
         */
        boolQuery.filter(QueryBuilders.geoBoundingBoxQuery("addresses.location")
                .setCorners(new GeoPoint(40.526588, 112.025029), new GeoPoint(0.0, 0.0)));

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
