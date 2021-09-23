package com.kaveinga.elasticsearch.dao;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kaveinga.elasticsearch.entity.Swipe;

public interface SwipeRepository extends JpaRepository<Swipe, Long>{

}
