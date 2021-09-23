package com.kaveinga.elasticsearch.dao;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import com.kaveinga.elasticsearch.dto.RowDTO;
import com.kaveinga.elasticsearch.entity.User;
import com.kaveinga.elasticsearch.utils.ObjectUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
public class IndexDAOImp implements IndexDAO {

    @Value("${spring.datasource.name}")
    private String              database;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public void insert(List<RowDTO> rows) {
        // TODO Auto-generated method stub

        if (null == rows || rows.size() == 0)

        {
            log.info("empty payload.");
            return;
        }

        // log.info("inserting into {}",database.toLowerCase());
        final BulkRequest bulkRequest = new BulkRequest();

        rows.stream().forEach(row -> {
            IndexRequest request = new IndexRequest(database.toLowerCase()).id(row.getUserId() + "").source(row.getJsonData(), XContentType.JSON);
            bulkRequest.add(request);
        });

        try {
            BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

            log.info("status={}, hasFailures={}, failureMsg={}", response.status().getStatus(), response.hasFailures(), (response.hasFailures() ? response.buildFailureMessage() : ""));

            // for (BulkItemResponse item : Arrays.asList(response.getItems())) {
            // log.info("index={}, id={}, type={}", item.getIndex(), item.getId(), item.getType());
            // }

        } catch (IOException e) {
            log.warn("IOException, msg={}", e.getLocalizedMessage());

            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void insert(RowDTO row) {
        if (null == row) {
            log.info("empty payload.");
            return;
        }

        IndexRequest request = new IndexRequest(database.toLowerCase()).id(row.getUserId() + "").source(row.getJsonData(), XContentType.JSON);

        try {
            IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);

            log.info("status={}", response.status().getStatus());

            // for (BulkItemResponse item : Arrays.asList(response.getItems())) {
            // log.info("index={}, id={}, type={}", item.getIndex(), item.getId(), item.getType());
            // }

        } catch (IOException e) {
            log.warn("IOException, msg={}", e.getLocalizedMessage());

            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void insertUsers(List<User> rows) {
        if (null == rows || rows.size() == 0) {
            log.info("empty payload.");
            return;
        }

        // log.info("inserting into {}",database.toLowerCase());
        final BulkRequest bulkRequest = new BulkRequest();

        rows.stream().forEach(row -> {
            IndexRequest request = new IndexRequest(database.toLowerCase()).id(row.getId() + "").source(ObjectUtils.toJson(row), XContentType.JSON);
            bulkRequest.add(request);
        });

        try {
            BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

            log.info("status={}, hasFailures={}, failureMsg={}", response.status().getStatus(), response.hasFailures(), (response.hasFailures() ? response.buildFailureMessage() : ""));

            // for (BulkItemResponse item : Arrays.asList(response.getItems())) {
            // log.info("index={}, id={}, type={}", item.getIndex(), item.getId(), item.getType());
            // }

        } catch (IOException e) {
            log.warn("IOException, msg={}", e.getLocalizedMessage());

            throw new IllegalArgumentException(e);
        }

    }

    @Override
    public void insertUser(User row) {
        if (null == row) {
            log.info("empty payload.");
            return;
        }

        IndexRequest request = new IndexRequest(database.toLowerCase()).id(row.getId() + "").source(ObjectUtils.toJson(row), XContentType.JSON);

        try {
            IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);

            log.info("status={}", response.status().getStatus());

            // for (BulkItemResponse item : Arrays.asList(response.getItems())) {
            // log.info("index={}, id={}, type={}", item.getIndex(), item.getId(), item.getType());
            // }

        } catch (IOException e) {
            log.warn("IOException, msg={}", e.getLocalizedMessage());

            throw new IllegalArgumentException(e);
        }
    }
}
