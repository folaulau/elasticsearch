package com.kaveinga.elasticsearch.dao;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kaveinga.elasticsearch.entity.Card;

public interface CardRepository extends JpaRepository<Card, Long>{

}
