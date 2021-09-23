package com.kaveinga.elasticsearch.dao;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;
import com.kaveinga.elasticsearch.dto.RowDTO;
import com.kaveinga.elasticsearch.entity.User;
import com.kaveinga.elasticsearch.utils.ResultSetMapperUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
public class UserDAOImp implements UserDAO {

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @Autowired
    private UserRepository             userRepository;

    @Override
    public List<RowDTO> get(int start, int end) {

        // @formatter:off

        SqlParameterSource namedParameters = new MapSqlParameterSource()
                .addValue("start", start)
                .addValue("end", end);
 
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(
                        "SELECT u.id as userId, "
                        + "JSON_OBJECT('id',u.id, "
                        + "'firstName', u.first_name, " 
                        + "'lastName', u.last_name, "
                        + "'cards', cards.cardList "
                        + ") as jsonData ");
        
        /**
         * JOINs
         */
        sqlBuilder.append(
                "FROM user as u "
                );
        
        /**
         * json array using JSON_ARRAYAGG
         */
//        sqlBuilder.append("LEFT JOIN (SELECT c.user_id, c.id, "
//                + " JSON_ARRAYAGG(JSON_OBJECT("
//                + "'id', c.id, "
//                + "'userId', c.user_id, "
//                + "'cardNumber', c.card_number, "
//                + "'expirationDate', c.expiration_date "
//                + ")) as cardList "
//                + "FROM card as c "
//                + "GROUP BY c.user_id ) as cards "
//                + "ON u.id = cards.user_id ");
        
        /**
         * CAST works with CONCAT and GROUP_CONCAT
         */
      sqlBuilder.append("LEFT JOIN (SELECT c.user_id, c.id, "
      + " CAST(CONCAT('[', GROUP_CONCAT("
      + "JSON_OBJECT("
      + "'id', c.id, "
      + "'userId', c.user_id, "
      + "'cardNumber', c.card_number, "
      + "'expirationDate', c.expiration_date "
      + ")"
      + "), ']') as JSON) as cardList "
      + "FROM card as c "
      + "GROUP BY c.user_id ) as cards "
      + "ON u.id = cards.user_id ");
        
        
        /**
         * WHERE clause
         */
        sqlBuilder.append("WHERE u.id BETWEEN :start AND :end ");

        // @formatter:on
        log.info("\n\n{}\n\n", sqlBuilder.toString());

        List<RowDTO> rows = null;

        try {
            rows = namedParameterJdbcTemplate.query(sqlBuilder.toString(), namedParameters, (rs, rowNum) -> {
                RowDTO row = new RowDTO();
                row.setUserId(rs.getInt("userId"));
                row.setJsonData(rs.getString("jsonData"));
                return row;

            });
        } catch (Exception e) {
            log.warn("Exception, msg={}", e.getLocalizedMessage());
            // shutDownApplication();
        }

        return rows;
    }

    @Override
    public Page<User> get(Pageable pageable) {
        // TODO Auto-generated method stub
        return userRepository.findAll(pageable);
    }

}
