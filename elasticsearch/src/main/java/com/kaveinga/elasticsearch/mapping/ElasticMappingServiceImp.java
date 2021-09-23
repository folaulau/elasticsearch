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
                    builder.startObject("PropertyType");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("PropertySubType");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("location");
                    {
                        builder.field("type", "geo_point");
                    }
                    builder.endObject();

                    builder.startObject("appraisals");
                    {
                        builder.field("type", "nested");
                        
                        builder.startObject("properties");
                        {
                            builder.startObject("DateDue");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                            builder.startObject("DateBooked");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                            builder.startObject("ReportDate");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("expenses");
                    {
                        builder.field("type", "nested");
                        
                        builder.startObject("properties");
                        {
                            builder.startObject("IEPeriodEnd");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                            builder.startObject("ExpenseDateTimeCreated");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("leases");
                    {
                        builder.field("type", "nested");
                        
                        builder.startObject("properties");
                        {
                            builder.startObject("SaleDateTimeCreated");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                            builder.startObject("ContractDate");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("sales");
                    {
                        builder.field("type", "nested");
                        
                        builder.startObject("properties");
                        {
                            builder.startObject("SurveyDate");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                            builder.startObject("LeaseDateTimeCreated");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                            builder.startObject("LeaseCommenceMYr");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("costs");
                    {
                        builder.field("type", "nested");
                        
                        builder.startObject("properties");
                        {
                            builder.startObject("CostDateTimeCreated");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                            builder.startObject("CostEffectiveDate");
                            {
                                builder.field("type", "date");
                                builder.field("format", "yyyy-MM-dd");
                            }
                            builder.endObject();
                            
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    
                    builder.startObject("photos");
                    {
                        builder.field("type", "nested");
                    }
                    builder.endObject();

                    builder.startObject("EntryDate");
                    {
                        builder.field("type", "date");
                        builder.field("format", "yyyy-MM-dd HH:mm:ss");
                    }
                    builder.endObject();

                    builder.startObject("CreatedOn");
                    {
                        builder.field("type", "date");
                        builder.field("format", "yyyy-MM-dd HH:mm:ss");
                    }
                    builder.endObject();

                    builder.startObject("GBA");
                    {
                        builder.field("type", "long");
                    }
                    builder.endObject();
                    
                    builder.startObject("PropertyName");
                    {
                        builder.field("type", "text");
                        builder.field("analyzer", caseInsensitiveAnalyzer);
                        
                        builder.startObject("fields");
                        {
                            builder.startObject("keyword");
                            {
                                builder.field("type", "keyword");
                                builder.field("ignore_above", 256);
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    
                    builder.startObject("Address");
                    {
                        builder.field("type", "text");
                        builder.field("analyzer", caseInsensitiveAnalyzer);
                        
                        builder.startObject("fields");
                        {
                            builder.startObject("keyword");
                            {
                                builder.field("type", "keyword");
                                builder.field("ignore_above", 256);
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    
                    builder.startObject("County");
                    {
                        builder.field("type", "text");
                        builder.field("analyzer", caseInsensitiveAnalyzer);
                        
                        builder.startObject("fields");
                        {
                            builder.startObject("keyword");
                            {
                                builder.field("type", "keyword");
                                builder.field("ignore_above", 256);
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    
                    builder.startObject("StateFullName");
                    {
                        builder.field("type", "text");
                        builder.field("analyzer", caseInsensitiveAnalyzer);
                        
                        builder.startObject("fields");
                        {
                            builder.startObject("keyword");
                            {
                                builder.field("type", "keyword");
                                builder.field("ignore_above", 256);
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    
                    builder.startObject("StateAbbr");
                    {
                        builder.field("type", "text");
                        builder.field("analyzer", caseInsensitiveAnalyzer);
                        
                        builder.startObject("fields");
                        {
                            builder.startObject("keyword");
                            {
                                builder.field("type", "keyword");
                                builder.field("ignore_above", 256);
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    
                    builder.startObject("StreetName");
                    {
                        builder.field("type", "text");
                        builder.field("analyzer", caseInsensitiveAnalyzer);
                        
                        builder.startObject("fields");
                        {
                            builder.startObject("keyword");
                            {
                                builder.field("type", "keyword");
                                builder.field("ignore_above", 256);
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    
                    builder.startObject("City");
                    {
                        builder.field("type", "text");
                        builder.field("analyzer", caseInsensitiveAnalyzer);
                        
                        builder.startObject("fields");
                        {
                            builder.startObject("keyword");
                            {
                                builder.field("type", "keyword");
                                builder.field("ignore_above", 256);
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
