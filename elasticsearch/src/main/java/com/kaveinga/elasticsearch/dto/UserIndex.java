package com.kaveinga.elasticsearch.dto;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Set;

import javax.persistence.Column;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(value = Include.NON_NULL)
public class UserIndex implements Serializable {

    private static final long    serialVersionUID = 1L;

    private Long                 id;

    private String               firstName;

    private String               lastName;

    private String               email;

    private String               phoneNumber;

    private String               gender;

    private LocalDate            dateOfBirth;

    private Set<CardNestedIndex> cards;
}
