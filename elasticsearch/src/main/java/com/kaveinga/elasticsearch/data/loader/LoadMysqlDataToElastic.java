package com.kaveinga.elasticsearch.data.loader;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.kaveinga.elasticsearch.dao.ElasticsearchDAO;
import com.kaveinga.elasticsearch.dao.UserDAO;
import com.kaveinga.elasticsearch.entity.User;
import com.kaveinga.elasticsearch.mapping.ElasticMappingService;
import com.kaveinga.elasticsearch.utils.ObjectUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LoadMysqlDataToElastic implements CommandLineRunner {

    @Autowired
    private UserDAO               userDAO;

    @Autowired
    private ElasticsearchDAO      elasticsearchDAO;

    @Autowired
    private ElasticMappingService elasticMappingService;

    @Override
    public void run(String... args) throws Exception {

        
        elasticMappingService.setupMapping();
        
        int pageNumber = 0;
        int pageSize = 10;

        Pageable pageable = PageRequest.of(pageNumber, pageSize);
        Page<User> userPage = null;

        while (true) {
            userPage = userDAO.get(pageable);

            if (userPage.hasContent()) {

                List<User> users = userPage.getContent();

                log.info("users={}", ObjectUtils.toJson(users));

                elasticsearchDAO.insertUsers(users);

                if (!userPage.hasNext()) {
                    break;
                }

                pageNumber++;
                pageable = PageRequest.of(pageNumber, pageSize);

            } else {
                break;
            }
        }

        log.info("done loading data to ES!");

    }
}
