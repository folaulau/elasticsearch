package com.kaveinga.elasticsearch.utils;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Entity;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResultSetMapperUtils<T> {
    // This method is already implemented in package
    // but as far as I know it accepts only public class attributes
    private void setProperty(Object clazz, String fieldName, Object columnValue) {
        try {
            //log.info("clazz={},fieldName={},columnValue={}", clazz, fieldName, columnValue);
            // get all fields of the class (including public/protected/private)
            Field field = clazz.getClass().getDeclaredField(fieldName);
            // this is necessary in case the field visibility is set at private
            field.setAccessible(true);
            field.set(clazz, columnValue);
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            log.warn("error localMsg={}, msg={}", e.getLocalizedMessage(), e.getMessage());
        }
    }

    public List<T> mapRersultSetToList(ResultSet rs, Class<T> clazz) {
        List<T> outputList = null;
        try {
            // make sure resultset is not null
            if (rs != null) {

                while (rs.next()) {

                    log.info("fields");

                    T bean = mapRersultSetToSingleObject(rs, clazz);

                    if (outputList == null) {
                        outputList = new ArrayList<T>();
                    }
                    outputList.add(bean);
                } // EndOf while(rs.next())
            } else {
                // ResultSet is empty
                log.warn("ResultSet is empty");
                return null;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return outputList;
    }

    public T mapRersultSetToSingleObject(ResultSet rs, Class<T> clazz) {

        T bean = null;

        try {
            // make sure resultset is not null
            if (rs != null) {

                // get the resultset metadata
                ResultSetMetaData rsmd = rs.getMetaData();

                bean = (T) clazz.newInstance();

                for (int _iterator = 0; _iterator < rsmd.getColumnCount(); _iterator++) {
                    // get the SQL column name
                    String columnName = rsmd.getColumnName(_iterator + 1);

                    // get the value of the SQL column
                    Object columnValue = rs.getObject(_iterator + 1);

                    // String columnTypeName = rsmd.getColumnTypeName(_iterator + 1);

                    if (columnValue != null) {

                        this.setProperty(bean, columnName, columnValue);

                        // log.info("columnName={}, columnValue={}, columnTypeName={}", columnName, columnValue,
                        // columnTypeName);
                    }

                } // EndOf for(_iterator...)

            } else {
                // ResultSet is empty
                log.warn("ResultSet is empty");
                return null;
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

        return bean;
    }
}
