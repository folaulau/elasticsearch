package com.kaveinga.elasticsearch.entity;

import java.io.Serializable;
import java.time.LocalDateTime;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.UpdateTimestamp;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.kaveinga.elasticsearch.dto.GeoPoint;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(value = Include.NON_NULL)
@DynamicUpdate
@Entity
@Table(name = "address")
public class Address implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long              id;

    @Column(name = "street")
    private String            street;

    @Column(name = "street2")
    private String            street2;

    @Column(name = "city")
    private String            city;

    @Column(name = "state")
    private String            state;

    @Column(name = "zipcode")
    private String            zipcode;

    @Column(name = "latitude")
    private Double            latitude;

    @Column(name = "longitude")
    private Double            longitude;

    @Column(name = "timezone")
    private String            timezone;

    @Column(name = "primary_address")
    private Boolean           primary;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime     createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime     updatedAt;

    public Address(String street, String street2, String city, String state, String zipcode) {
        this(street, street2, city, state, zipcode, null, null);
    }

    public Address(String street, String street2, String city, String state, String zipcode, Double latitude, Double longitude) {
        this.street = street;
        this.street2 = street2;
        this.city = city;
        this.state = state;
        this.zipcode = zipcode;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public GeoPoint getLocation() {
        return new GeoPoint(latitude, longitude);
    }


    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(this.id)
                .append(this.street)
                .append(this.street2)
                .append(this.city)
                .append(this.state)
                .append(this.longitude)
                .append(this.latitude)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        Address other = (Address) obj;
        return new EqualsBuilder()
                .append(this.id, other.id)
                .append(this.street, other.street)
                .append(this.street2, other.street2)
                .append(this.city, other.city)
                .append(this.state, other.state)
                .append(this.longitude, other.longitude)
                .append(this.latitude, other.latitude)
                .isEquals();
    }
}
