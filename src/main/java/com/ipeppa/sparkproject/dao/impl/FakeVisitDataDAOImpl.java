package com.ipeppa.sparkproject.dao.impl;

import com.ipeppa.sparkproject.dao.IFakeVisitDataDAO;
import com.ipeppa.sparkproject.domin.FakeVisitData;
import com.ipeppa.sparkproject.jdbc.JDBCHelper;

/**
 * “Go Further进无止境” <br>
 * 〈〉
 *
 * @author Luoxun
 * @create 2020/6/2
 * @since 1.0.0
 */
public class FakeVisitDataDAOImpl implements IFakeVisitDataDAO {

    @Override
    public void insert(FakeVisitData fakeVisitData) {
        String sql = "";
        Object[] params = new Object[]{

        };
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
