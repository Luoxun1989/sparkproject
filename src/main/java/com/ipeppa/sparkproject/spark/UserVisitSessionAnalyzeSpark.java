/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: UserVisitSessionAnalyzeSpark
 * Author:   admin
 * Date:     2020/4/4 23:14
 * Description: 用户访问session作业分析
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.ipeppa.sparkproject.conf.ConfigurationManager;
import com.ipeppa.sparkproject.constant.Constants;
import com.ipeppa.sparkproject.dao.ITaskDAO;
import com.ipeppa.sparkproject.dao.impl.DAOFactory;
import com.ipeppa.sparkproject.domin.Task;
import com.ipeppa.sparkproject.test.MockData;
import com.ipeppa.sparkproject.util.ParamUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

/**
 * 〈一句话功能简述〉<br> 
 * 〈用户访问session分析 spark作业〉
 *
 * @author admin
 * @create 2020/4/4
 * @since 1.0.0
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        String spark_app_name_session = ConfigurationManager.getProperty(Constants.SPARK_APP_NAME_SESSION);
        SparkConf sparkConf = new SparkConf().setAppName(spark_app_name_session).setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = getSQLContext(javaSparkContext.sc());
        //创建需要使用的JDBC辅助组件
        ITaskDAO iTaskDAO = DAOFactory.getTaskDAOImpl();
        //生成模拟测试数据
        mockData(javaSparkContext, sqlContext);
        long taskid= ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = iTaskDAO.findById(taskid);
        if (task == null){
            System.out.println("no task exist");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionJavaRDD = getActionRDByDateRange(sqlContext,taskParam);


        javaSparkContext.close();
    }
    /**
     * 获取指定日期范围内的用户访问行为数据
     * @param sqlContext SQLContext
     * @param taskParam 任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDByDateRange(SQLContext sqlContext, JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action where date>='"+startDate+"' and date <= '"+endDate+"'";
        DataFrame actionDF =sqlContext.sql(sql);
        return actionDF.javaRDD();
    }
    private static JavaPairRDD<String,String> aggregateBysession(JavaRDD<Row> actionJavaRDD){
        JavaPairRDD<String,Row> sessionid2ActionRDD = actionJavaRDD.mapToPair(new PairFunction<Row, String, Row>() {
            private static  final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2),row);
            }
        });
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();
        return null;
    }
    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     * @param sc SparkContext
     * @return SQLContext
     */
    private  static SQLContext getSQLContext(SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            return  new SQLContext(sc);
        }else {
            return new HiveContext(sc);
        }
    }
    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc,SQLContext sqlContext){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            MockData.mock(sc, sqlContext);
        }
    }
}