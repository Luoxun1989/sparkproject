package com.ipeppa.sparkproject.dao.impl;

import com.ipeppa.sparkproject.dao.ISessionDetailDAO;
import com.ipeppa.sparkproject.domin.SessionDetail;
import com.ipeppa.sparkproject.jdbc.JDBCHelper;

/**
 * “Go Further进无止境” <br>
 * 〈〉
 *
 * @author Luoxun
 * @create 2020/5/28
 * @since 1.0.0
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {

    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{sessionDetail.getTaskId(),
                sessionDetail.getUserId(),
                sessionDetail.getSessionId(),
                sessionDetail.getPageId(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
