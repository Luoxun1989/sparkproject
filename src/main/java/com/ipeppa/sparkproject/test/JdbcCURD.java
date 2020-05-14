/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: JdbcCURD
 * Author:   admin
 * Date:     2020/4/1 22:50
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.test;

import java.sql.*;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author admin
 * @create 2020/4/1
 * @since 1.0.0
 */
public class JdbcCURD {
    private final static String URL="jdbc:mysql://localhost:3306/spark_project";
    private final static String USERNAME="root";
    private final static String PASSWORD="hadoop";

    public static void main(String[] args) {
        select("select * from test_user");
//        preparedStatement("insert into test_user(user_id, user_name,user_age,user_address) values(?,?,?,?)");
    }
    private static void bulid(String sql) {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();
            int rtn = stmt.executeUpdate(sql);
            System.out.println(rtn);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }
    public static void insert(String insertSql) {
        bulid(insertSql);
    }
    public static void update(String updateSql) {
        bulid(updateSql);
    }
    public static void delete(String deleteSql) {
        bulid(deleteSql);
    }
    public static void select(String selectSql) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs= null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(selectSql);
            while (rs.next()){
                int id = rs.getInt(1);
                String name = rs.getString(2);
                String age = rs.getString(3);
                String address = rs.getString(4);
                System.out.println("id=" + id + ", name=" + name + ", age=" + age+ ", address=" + address);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }
    public static void preparedStatement(String prepareSql) {
        Connection conn = null;
        PreparedStatement pstmt= null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            pstmt = conn.prepareStatement(prepareSql);
            pstmt.setInt(1, 108);
            pstmt.setString(2, "li");
            pstmt.setString(3, "45");
            pstmt.setString(4, "李四搭嘎发光热");
            int rtn = pstmt.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}