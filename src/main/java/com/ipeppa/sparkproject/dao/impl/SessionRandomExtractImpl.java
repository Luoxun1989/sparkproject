package com.ipeppa.sparkproject.dao.impl;

import com.ipeppa.sparkproject.dao.ISessionRandomExtractDAO;
import com.ipeppa.sparkproject.domin.SessionRandomExtract;
import com.ipeppa.sparkproject.jdbc.JDBCHelper;

/**
 * “Go Further进无止境” <br>
 * 〈〉
 *
 * @author Luoxun
 * @create 2020/5/28
 * @since 1.0.0
 */
public class SessionRandomExtractImpl implements ISessionRandomExtractDAO {

    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql="";
        Object[] params = new Object[]{
                sessionRandomExtract.getTaskId(),
                sessionRandomExtract.getSessionId(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getClickCategoryIds(),
                sessionRandomExtract.getSearchKeywords()
        };
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
