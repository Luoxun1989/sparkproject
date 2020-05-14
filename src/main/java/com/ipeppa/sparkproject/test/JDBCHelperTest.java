/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: JDBCHelperTest
 * Author:   admin
 * Date:     2020/4/2 22:40
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.test;

import com.ipeppa.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author admin
 * @create 2020/4/2
 * @since 1.0.0
 */
public class JDBCHelperTest {
    public static void main(String[] args) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        /*jdbcHelper.executeQuery("select user_name,user_address from test_user where user_id=?", new Object[]{"107"}, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    String name = rs.getString(1);
                    String add = rs.getString(1);
                    System.out.println(name + "..." + add);
                }
            }
        });*/
//        jdbcHelper.executeUpdate("delete from test_user where user_id=?",new Object[]{1008});
        String sql = "insert into test_user(user_name,user_address) values(?,?)";
        List<Object[]> paramsList = new ArrayList<Object[]>();
        paramsList.add(new Object[]{"麻er子", 30});
        paramsList.add(new Object[]{"王er五", 35});
        jdbcHelper.executeBatch(sql,paramsList);
    }
}