/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Constants
 * Author:   admin
 * Date:     2020/4/1 0:02
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.constant;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author admin
 * @create 2020/4/1
 * @since 1.0.0
 */
public interface Constants {

    //数据库相关的常量
     String JDBC_DRIVER="jdbc.driver";
     String JDBC_URL="jdbc.url";
     String JDBC_USER="jdbc.user";
     String JDBC_PASSWORD="jdbc.password";
     String JDBC_DATASOURCE_SIZE="jdbc.datasource.size";

    //SPARK作业相关的常量
     String SPARK_LOCAL = "spark.local";
     String SPARK_APP_NAME_SESSION = "spark.app_name_session";
     String PARAM_START_DATE = "param.start_date";
     String PARAM_END_DATE = "param.end_date";

     String SPARK_LOCAL_TASKID_SESSION = "";
     String SESSION_COUNT = "";
     String TIME_PERIOD_1s_3s = "";
     String TIME_PERIOD_4s_6s = "";
     String TIME_PERIOD_7s_9s = "";
     String TIME_PERIOD_10s_30s = "";
     String TIME_PERIOD_30s_60s = "";
     String TIME_PERIOD_1m_3m = "";
     String TIME_PERIOD_3m_10m = "";
     String TIME_PERIOD_10m_30m = "";
     String TIME_PERIOD_30m = "";
     String STEP_PERIOD_1_3 = "";
     String STEP_PERIOD_4_6 = "";
     String STEP_PERIOD_7_9 = "";
     String STEP_PERIOD_10_30 = "";
     String STEP_PERIOD_30_60 = "";
     String STEP_PERIOD_60 = "";
}