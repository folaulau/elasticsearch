package com.kaveinga.elasticsearch;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.kaveinga.elasticsearch.dao.UserDAO;
import com.kaveinga.elasticsearch.entity.User;
import com.kaveinga.elasticsearch.utils.ObjectUtils;
import com.kaveinga.elasticsearch.utils.RandomGeneratorUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootTest
class HowToUseDocumentAPITests {

    @Value("${spring.datasource.name}")
    private String              database;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private UserDAO             userDAO;

    @Test
    void indexDocument() {

        Long id = RandomGeneratorUtils.getLongWithin(50, 100);

        User user = new User();
        user.setFirstName(RandomGeneratorUtils.getRandomFirstname());
        user.setDateOfBirth(LocalDate.now().minusYears(RandomGeneratorUtils.getLongWithin(19, 40)));
        user.setId(id);

        String jsonUser = ObjectUtils.toJson(user);

        log.info("user={}", jsonUser);

        IndexRequest request = new IndexRequest(database);
        request.id(id.toString());

        request.source(jsonUser, XContentType.JSON);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");

        try {
            IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);

            log.info("index status={}", indexResponse.status().getStatus());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Test
    void getDocumentById() {

        Long id = 1L;

        GetRequest getRequest = new GetRequest(database, id.toString());

        try {
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);

            log.info("result={}", ObjectUtils.toJson(getResponse.getSource()));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-document-update.html<br>
     * only update fields being passed
     */
    @Test
    void updateWithPartialDocument() {

        Long id = 1L;

        try {

            GetRequest getRequest = new GetRequest(database, id.toString());

            getRequest.fetchSourceContext(new FetchSourceContext(true, new String[]{"id", "firstName", "lastName", "dateOfBirth"}, new String[]{}));

            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);

            log.info("current user={}", ObjectUtils.toJson(getResponse.getSource()));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        /**
         * update only user data not nested data.
         */
        User user = new User();
        user.setFirstName(RandomGeneratorUtils.getRandomFirstname());
        user.setDateOfBirth(LocalDate.now().minusYears(RandomGeneratorUtils.getLongWithin(19, 40)));
        user.setId(id);

        String jsonUser = ObjectUtils.toJson(user);

        log.info("partial user={}", jsonUser);

        UpdateRequest request = new UpdateRequest(database, id.toString());

        request.doc(jsonUser, XContentType.JSON);
        request.timeout(TimeValue.timeValueSeconds(1));
        request.retryOnConflict(3);
        /**
         * fetch source after update
         */
        request.fetchSource(true);
        request.fetchSource(new FetchSourceContext(true, new String[]{"id", "firstName", "lastName", "dateOfBirth"}, new String[]{}));

        log.info("request={}", request.toString());

        request.fetchSource();

        try {
            UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);

            GetResult getResult = updateResponse.getGetResult();

            if (getResult.isExists()) {
                log.info("updated user={}", ObjectUtils.toJson(getResult.getSource()));
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-document-update-by-query.html
     */
    @Test
    void updateWithQuery() {

        UpdateByQueryRequest request = new UpdateByQueryRequest(database);

        /**
         * Query<br>
         * It can be a compound query or a simple query
         */
        request.setQuery(QueryBuilders.matchQuery("description", "His biggest"));

        request.setMaxDocs(100);

        StringBuilder inlineUpdates = new StringBuilder();
        inlineUpdates.append("ctx._source.firstName='Folaulau"+RandomGeneratorUtils.getLongWithin(19, 4000)+"';");
        inlineUpdates.append("ctx._source.lastName='Kaveinga"+RandomGeneratorUtils.getLongWithin(19, 4000)+"';");

        request.setScript(new Script(ScriptType.INLINE, "painless", inlineUpdates.toString(), Collections.emptyMap()));

        try {
            BulkByScrollResponse bulkResponse = restHighLevelClient.updateByQuery(request, RequestOptions.DEFAULT);

            log.info("timedOut={}, updatedDocs={}, batches={}, totalDocs={}", bulkResponse.isTimedOut(), bulkResponse.getUpdated(),  bulkResponse.getBatches(), bulkResponse.getTotal());
        } catch (IOException e) {
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
