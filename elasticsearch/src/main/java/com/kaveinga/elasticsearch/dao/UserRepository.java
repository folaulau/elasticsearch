package com.kaveinga.elasticsearch.dao;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kaveinga.elasticsearch.entity.User;

public interface UserRepository extends JpaRepository<User, Long>{

}
