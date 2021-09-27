package com.kaveinga.elasticsearch.entity;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.kaveinga.elasticsearch.status.UserStatus;

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
@Table(name = "user")
public class User implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false, unique = true)
    private Long              id;

    @Column(name = "first_name")
    private String            firstName;

    @Column(name = "last_name")
    private String            lastName;

    @Column(name = "email")
    private String            email;

    @Column(name = "phone_number")
    private String            phoneNumber;

    @Column(name = "gender")
    private String            gender;

    @Column(name = "date_of_birth")
    private LocalDate         dateOfBirth;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private UserStatus        status;

    @JsonIgnoreProperties(value = {"user"})
    @OneToMany(mappedBy = "user")
    private Set<Card>         cards;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<Address>      addresses;

    @Lob
    @Column(name = "description")
    private String            description;

    @Column(name = "last_logged_in_at")
    private LocalDateTime     lastLoggedInAt;

    public void addAddress(Address address) {
        if (this.addresses == null) {
            this.addresses = new HashSet<>();
        }
        this.addresses.add(address);
    }

    public User(Long id) {
        this.id = id;
    }

}
