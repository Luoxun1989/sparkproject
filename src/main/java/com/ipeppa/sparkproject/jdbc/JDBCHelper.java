/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: JDBCHelper
 * Author:   admin
 * Date:     2020/4/2 21:19
 * Description: 数据库连接池
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.jdbc;


import com.ipeppa.sparkproject.conf.ConfigurationManager;
import com.ipeppa.sparkproject.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * 〈一句话功能简述〉<br>
 * 〈数据库连接池〉
 *
 * @author admin
 * @create 2020/4/2
 * @since 1.0.0
 */
public class JDBCHelper {

    // 第一步：在静态代码块中，直接加载数据库的驱动
    static {
        try {
            Class.forName(ConfigurationManager.getProperty(Constants.JDBC_DRIVER));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

    // 第二步，实现JDBCHelper的单例化
    // 为什么要实现代理化呢？因为它的内部要封装一个简单的内部的数据库连接池
    // 为了保证数据库连接池有且仅有一份，所以就通过单例的方式
    // 保证JDBCHelper只有一个实例，实例中只有一份数据库连接池
    private static JDBCHelper instance = null;

    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    LinkedList<Connection> datasource = new LinkedList<Connection>();

    private JDBCHelper() {
        int dataSourceSize = ConfigurationManager.getInt(Constants.JDBC_DATASOURCE_SIZE);
        for (int i = 0; i < dataSourceSize; i++) {
            String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            try {
                Connection connection = DriverManager.getConnection(url, user, password);
                datasource.push(connection);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized Connection getConnection() {
        while (datasource.size() == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 执行增删改SQL语句
     *
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public int executeUpdate(String sql, Object[] params) {
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rtn = pstmt.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
        return rtn;
    }

    /**
     * 执行查询SQL语句
     *
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql, Object[] params, QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rs = pstmt.executeQuery();
            callback.process(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 静态内部类：查询回调接口
     *
     * @author Administrator
     */
    public interface QueryCallback {
        /**
         * 处理查询结果
         *
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }

    public int[] executeBatch(String sql, List<Object[]> paramsList) {
        int rtn[] = null;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sql);
            for (Object[] params : paramsList) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
                pstmt.addBatch();
            }
            rtn = pstmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
        return rtn;
    }
}