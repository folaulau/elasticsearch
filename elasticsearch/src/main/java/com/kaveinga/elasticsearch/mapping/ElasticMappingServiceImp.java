package com.kaveinga.elasticsearch.mapping;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ElasticMappingServiceImp implements ElasticMappingService {

    @Value("${spring.datasource.name}")
    private String              database;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public boolean setupMapping() {
        log.info("setting up mapping for {} index.", database);

        try {
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(database.toLowerCase());
            restHighLevelClient.indices().delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        CreateIndexRequest request = new CreateIndexRequest(database.toLowerCase());

        // @formatter:off
 

        String caseInsensitiveAnalyzer = "analyzer_case_insensitive";

        try {
            request.settings(Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 2)
                    .loadFromSource(Strings.toString(XContentFactory.jsonBuilder()
                            .startObject()
                                .startObject("analysis")
                                    .startObject("analyzer")
                                        .startObject(caseInsensitiveAnalyzer)
                                            .field("tokenizer", "keyword")
                                            .field("filter", "lowercase")
                                             .endObject()
                                        .endObject()
                                    .endObject()
                            .endObject()), XContentType.JSON));
            
        } catch (IOException e) {
            log.warn(e.getLocalizedMessage());
            e.printStackTrace();
        }

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("properties");
                {
                    
                    builder.startObject("dateOfBirth");
                    {
                        builder.field("type", "date");
                        builder.field("format", "yyyy-MM-dd");
                    }
                    builder.endObject();
                    
                    /**
                     * nested object cards
                     */
                    builder.startObject("cards");
                    {
                        builder.field("type", "nested");
                        
                        builder.startObject("properties");
                        {
                            builder.startObject("expirationDate");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                            builder.startObject("activatedDate");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                            builder.startObject("deactivatedDate");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                            /**
                             * nested object swipe
                             */
                            builder.startObject("swipes");
                            {
                                builder.field("type", "nested");
                                
                                builder.startObject("properties");
                                {
                                    builder.startObject("createdAt");
                                    {
                                        builder.field("type", "date");
                                    }
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    
                    /**
                     * nested object addresses
                     */
                    builder.startObject("addresses");
                    {
                        builder.field("type", "nested");
                        
                        builder.startObject("properties");
                        {
                            builder.startObject("location");
                            {
                                builder.field("type", "geo_point");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    

                }
                builder.endObject();
            }
            builder.endObject();

            request.mapping(builder);
            
            // @formatter:on

            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
            log.info("{} index mapping has been set up!, isAcknowledged={}", database.toLowerCase(), createIndexResponse.isAcknowledged());
        } catch (IOException e) {
            log.warn("Error with setting up mapping. IOException, msg={}", e.getLocalizedMessage());
            return false;
        }
        return true;
    }

}
