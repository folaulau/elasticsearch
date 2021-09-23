package com.kaveinga.elasticsearch.dto;

import java.io.Serializable;

import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class RowDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer           userId;
    private String            jsonData;
}
