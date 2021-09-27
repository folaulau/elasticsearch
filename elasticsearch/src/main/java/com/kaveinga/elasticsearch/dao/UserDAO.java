package com.kaveinga.elasticsearch.dao;

import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.kaveinga.elasticsearch.dto.RowDTO;
import com.kaveinga.elasticsearch.entity.User;

public interface UserDAO {

    List<RowDTO> get(int start, int end);
    
    Page<User> get(Pageable pageable);
    
    User getById(Long id);
}
