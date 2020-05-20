/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: SessionAggrStatDAOImpl
 * Author:   admin
 * Date:     2020/5/20 22:51
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.dao.impl;

import com.ipeppa.sparkproject.dao.ISessionAggrStatDAO;
import com.ipeppa.sparkproject.domin.SessionAggrStat;
import com.ipeppa.sparkproject.jdbc.JDBCHelper;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author admin
 * @create 2020/5/20
 * @since 1.0.0
 */
public class SessionAggrStatDAOImpl implements ISessionAggrStatDAO {

    @Override
    public void insert(SessionAggrStat sessionAggrStat) {
        String sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] param = new Object[]{
                sessionAggrStat.getTask_id(),
                sessionAggrStat.getVisit_length_1s_3s_ratio(),
                sessionAggrStat.getVisit_length_4s_6s_ratio(),
                sessionAggrStat.getVisit_length_7s_9s_ratio(),
                sessionAggrStat.getVisit_length_10s_30s_ratio(),
                sessionAggrStat.getVisit_length_10s_30s_ratio(),
                sessionAggrStat.getVisit_length_30s_60s_ratio(),
                sessionAggrStat.getVisit_length_1m_3m_ratio(),
                sessionAggrStat.getVisit_length_3m_10m_ratio(),
                sessionAggrStat.getVisit_length_10m_30m_ratio(),
                sessionAggrStat.getVisit_length_30m_ratio(),

                sessionAggrStat.getStep_length_1_3_ratio(),
                sessionAggrStat.getStep_length_4_6_ratio(),
                sessionAggrStat.getStep_length_7_9_ratio(),
                sessionAggrStat.getStep_length_10_30_ratio(),
                sessionAggrStat.getStep_length_30_60_ratio(),
                sessionAggrStat.getStep_length_60_ratio()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,param);
    }
}