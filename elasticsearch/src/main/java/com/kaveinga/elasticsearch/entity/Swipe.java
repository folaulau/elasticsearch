package com.kaveinga.elasticsearch.entity;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(value = Include.NON_NULL)
@Entity
@Table(name = "swipe")
public class Swipe implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false, unique = true)
    private Long              id;

    @Column(name = "amount")
    private Double            amount;

    /**
     * Merchant
     */
    @Column(name = "merchant_code")
    private String            merchantCode;

    @Column(name = "merchant_name")
    private String            merchantName;

    /**
     * Card
     */

    @Column(name = "card_number")
    private String            cardNumber;

    @JoinColumn(name = "card_id")
    @ManyToOne(cascade = CascadeType.DETACH)
    private Card              card;
    
    @Column(name = "created_at")
    private LocalDateTime createdAt;

}
