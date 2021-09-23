package com.kaveinga.elasticsearch.entity;

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
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
@Table(name = "card")
public class Card implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false, unique = true)
    private Long              id;

    @Column(name = "card_number", unique = true)
    private String            cardNumber;

    @Column(name = "expiration_date")
    private LocalDate         expirationDate;

    @Column(name = "activated_date")
    private LocalDate         activatedDate;

    @Column(name = "deactivated_date")
    private LocalDate         deactivatedDate;

    @Column(name = "active")
    @Type(type = "true_false")
    private Boolean           active;

    @JoinColumn(name = "user_id")
    @ManyToOne(cascade = CascadeType.DETACH)
    private User              user;

    @JsonIgnoreProperties(value = {"card"})
    @OneToMany(mappedBy = "card")
    private Set<Swipe>        swipes;

    public Card(Long id) {
        this.id = id;
    }

}
