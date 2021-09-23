package com.kaveinga.elasticsearch.dao;

import java.util.List;

import com.kaveinga.elasticsearch.dto.RowDTO;
import com.kaveinga.elasticsearch.entity.User;

public interface IndexDAO {

    void insert(List<RowDTO> rows);
    
    void insertUsers(List<User> rows);
    
    void insert(RowDTO row);
    
    void insertUser(User row);
}
