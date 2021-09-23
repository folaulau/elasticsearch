package com.kaveinga.elasticsearch.dto;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(value = Include.NON_NULL)
public class CardNestedIndex implements Serializable {

    private static final long     serialVersionUID = 1L;

    private Long                  id;

    private String                cardNumber;

    private LocalDate             expirationDate;

    private Boolean               active;

    private LocalDate             activatedDate;

    private LocalDate             deactivatedDate;

    private Set<SwipeNestedIndex> swipes;
}
