package com.kaveinga.elasticsearch.data.loader;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.kaveinga.elasticsearch.dao.CardRepository;
import com.kaveinga.elasticsearch.dao.ElasticsearchDAO;
import com.kaveinga.elasticsearch.dao.SwipeRepository;
import com.kaveinga.elasticsearch.dao.UserDAO;
import com.kaveinga.elasticsearch.dao.UserRepository;
import com.kaveinga.elasticsearch.dto.RowDTO;
import com.kaveinga.elasticsearch.dto.UserIndex;
import com.kaveinga.elasticsearch.entity.User;
import com.kaveinga.elasticsearch.utils.ObjectUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LoadMysqlDataToElastic implements CommandLineRunner {

    @Autowired
    private UserDAO          userDAO;

    @Autowired
    private ElasticsearchDAO elasticsearchDAO;

    @Override
    public void run(String... args) throws Exception {

        int start = 1;
        int end = 10;

        List<RowDTO> rows = userDAO.get(start, end);
        List<RowDTO> newRows = new ArrayList<>();

        for (RowDTO row : rows) {
            log.info(row.toString());
//            RowDTO newRow = new RowDTO();
//            newRow.setUserId(row.getUserId());

            String jsonString = row.getJsonData();//.replace("\\", "xxx");
            log.info("jsonString={}", ObjectUtils.toJson(jsonString));
//            UserIndex userIndex = ObjectUtils.getObjectMapper().readValue(jsonString, UserIndex.class);
//            log.info("userIndex={}", ObjectUtils.toJson(userIndex));
//            newRow.setJsonData(ObjectUtils.toJson(userIndex));
//            newRows.add(newRow);

        }

        //elasticsearchDAO.insert(newRows);
        
        elasticsearchDAO.insert(rows);

//        Pageable pageable = PageRequest.of(start, end);
//
//        Page<User> users = userRepository.findAll(pageable);
//        
//        log.info("users={}",ObjectUtils.toJson(users));
    }
}
