package com.chinagoods.bigdata.functions.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

/**
 * @author xiaowei.song
 */
public class MysqlUtil {
    private static final Logger logger = LoggerFactory.getLogger(MysqlUtil.class);
    private Connection connection;
    public static final String DB_URL = "jdbc:mysql://172.18.7.7:3306/cg_search?characterEncoding=UTF-8&useSSL=false";
    public static final String DB_USER = "cg_search";
    public static final String DB_PASSWORD = "GPuBoTWz3UiMwwLz";

    public static final String SELECT_SENSITIVE_KEYWORDS_SQL = "select words key_word\n" +
            "from lexicon_sensitive ls\n" +
            "where 1=1\n" +
            "and status = 0\n" +
            "and is_deleted = 0";
    /**
     * MySQL 8.0 以下版本 - JDBC 驱动名及数据库 URL
     **/
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    public MysqlUtil() {
        connection = getConnection();
    }

    public Connection getConnection(){
        Connection conn = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        }catch (Exception e) {
            logger.error("获取mysql连接失败, dbUrl: {}, username: {}, password: {}", DB_URL, DB_USER, DB_PASSWORD, e);
            System.exit(1);
        }
        return conn;
    }

    /**
     * 查询表中关键词
     * @return keywords sensitive set违禁词集合
     * @throws SQLException 查詢異常
     */
     public Set<String> getSearchSensitiveKeywords() throws SQLException {
        Set<String> keywordsSet = new HashSet<>(3000);
        // 重建mysql连接信息
        if (connection == null || connection.isClosed()) {
             connection = getConnection();
         }
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(SELECT_SENSITIVE_KEYWORDS_SQL);
        // 展开结果集数据库
        while(rs.next()) {
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                keywordsSet.add((String) rs.getObject(i + 1));
            }
        }
        // 完成后关闭
        rs.close();
        stmt.close();
        return keywordsSet;
    }

    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    public static void main(String[] args) throws SQLException {
        MysqlUtil mysqlUtil = new MysqlUtil();
        System.out.println(mysqlUtil.getSearchSensitiveKeywords());
    }
}
