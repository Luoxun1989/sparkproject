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
import com.ipeppa.sparkproject.util.DateUtils;
import com.ipeppa.sparkproject.util.ParamUtils;
import com.ipeppa.sparkproject.util.StringUtils;
import org.apache.spark.Accumulator;
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
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

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
        //生成模拟测试数据
        mockData(javaSparkContext, sqlContext);
        //创建需要使用的JDBC辅助组件
        ITaskDAO iTaskDAO = DAOFactory.getTaskDAOImpl();
        long taskId= ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = iTaskDAO.findById(taskId);
        if (task == null){
            System.out.println("no task exist");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionJavaRDD = getActionRDDByDateRange(sqlContext,taskParam);

        JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionJavaRDD);

        /**
         * 持久化，很简单，就是对RDD调用persist()方法，并传入一个持久化级别
         *
         * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
         * StorageLevel.MEMORY_ONLY_SER()，第二选择
         * StorageLevel.MEMORY_AND_DISK()，第三选择
         * StorageLevel.MEMORY_AND_DISK_SER()，第四选择
         * StorageLevel.DISK_ONLY()，第五选择
         *
         * 如果内存充足，要使用双副本高可靠机制
         * 选择后缀带_2的策略
         * StorageLevel.MEMORY_ONLY_2()
         *
         */
        sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<String,String> sessionid2FullAggrInfoRDD = aggregateBysessionId(javaSparkContext,sqlContext,sessionid2actionRDD);
        Accumulator<String> sessionAggrStatAccumulator = javaSparkContext.accumulator(
                "", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2FullAggrInfoRDD, taskParam, sessionAggrStatAccumulator);
        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());


        javaSparkContext.close();
    }

    private static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD<String, String> sessionid2FullAggrInfoRDD,
            final JSONObject taskParam,final Accumulator<String> sessionAggrStatAccumulator) {


        return null;
    }

    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionJavaRDD) {
        return actionJavaRDD.mapToPair(new PairFunction<Row, String, Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(2), row);
			}

		});
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     * @param sqlContext SQLContext
     * @param taskParam 任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action where date>='"+startDate+"' and date <= '"+endDate+"'";
        DataFrame actionDF =sqlContext.sql(sql);
        return actionDF.javaRDD();
    }
    /*
     * @Description:
     * @Date: 2020/5/19 15:25
     * @param: [actionJavaRDD]
     * @return:
     **/
    private static JavaPairRDD<String,String> aggregateBysessionId(JavaSparkContext sc, SQLContext sqlContext,
                                                                 JavaPairRDD<String, Row> sessinoid2actionRDD){
        //groupByKey 算子 对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD = sessinoid2actionRDD.groupByKey();
        //[2020-05-19,76,d8407eb844194106a25941c001b1ca1e,5,2020-05-19 18:35:01,null,null,null,79,60,null,null]
        //date,userId,sessionId,pageId,visit_time,search_keyword,click_category_id,
        // click_product_id,order_category_ids,order_product_ids,pay_category_ids,pay_product_ids
        JavaPairRDD<Long,String> userId2PartAggrInfoRDD = sessionId2ActionsRDD.mapToPair(new PairFunction<Tuple2<String,
                Iterable<Row>>, Long, String>() {
            private final static long serialVersionUID = 1L;
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionId = tuple._1;
                Iterator<Row> allVisitInfo4OneSessionId = tuple._2.iterator();
                StringBuffer searchKeywordsBuffer = new StringBuffer();
                StringBuffer clickCategoryIdsBuffer = new StringBuffer();

                Date startTime = null;
                Date endTime = null;
                // session的访问步长   实际上就是allVisitInfo4OneSessionId大小
                int stepLength = 0;

                Long userId = null;
                while (allVisitInfo4OneSessionId.hasNext()){
                    Row row = allVisitInfo4OneSessionId.next();
                    if (null == userId){
                        userId = row.getLong(1);
                    }
                    String searchWord = row.getString(5);
                    Long clickCategoryId = row.getLong(6);
                    // 实际上真实数据 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                    if (StringUtils.isNotEmpty(searchWord)){
                        if (!searchKeywordsBuffer.toString().contains(searchWord)) {
                            searchKeywordsBuffer.append(searchWord.concat(","));
                        }
                    }
                    if (null != clickCategoryId){
                        if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                            clickCategoryIdsBuffer.append(String.valueOf(clickCategoryId).concat(","));
                        }
                    }
                    // 计算session开始和结束时间  "yyyy-MM-dd HH:mm:ss"  格式
                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    if(startTime == null) {
                        startTime = actionTime;
                    }
                    if(endTime == null) {
                        endTime = actionTime;
                    }
                    if(actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if(actionTime.after(endTime)) {
                        endTime = actionTime;
                    }
                    stepLength++;
                }

                String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                // 计算session访问时长（秒）
                long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
                String partAggrInfo = String.format("%s=%s|%s=%s|%s=%s|%s=%s|%s=%s|%s=%s",
                        Constants.FIELD_SESSION_ID,sessionId,Constants.FIELD_SEARCH_KEYWORDS,searchKeywords,
                        Constants.FIELD_CLICK_CATEGORY_IDS,clickCategoryIds, Constants.FIELD_VISIT_LENGTH ,visitLength,
                        Constants.FIELD_STEP_LENGTH,stepLength,Constants.FIELD_START_TIME,DateUtils.formatTime(startTime));

                return new Tuple2<Long, String>(userId,partAggrInfo);
            }
        });

        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long,Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0),row);
            }
        });

        /**
         * 这里就可以说一下，比较适合采用reduce join转换为map join的方式
         *
         * userid2PartAggrInfoRDD，可能数据量还是比较大，比如，可能在1千万数据
         * userid2InfoRDD，可能数据量还是比较小的，你的用户数量才10万用户
         * <Long,Tuple2<String,Row>>  = <userId,Tuple2<partAggrInfo,userInfo>>
         */
        JavaPairRDD<Long,Tuple2<String,Row>> userid2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);
        // 对join起来的数据进行拼接，并且返回 <String, String> = <sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long,
                Tuple2<String, Row>>, String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
//                Long userId = tuple._1;
                String partAggrInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;

                String sessionId = StringUtils.getFieldFromConcatString(
                        partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);
                String fullAggrInfo = String.format("%s=%s|%s=%s|%s=%s|%s=%s|%s=%s",partAggrInfo,
                        Constants.FIELD_AGE,age,Constants.FIELD_PROFESSIONAL,professional,
                        Constants.FIELD_CITY,city,Constants.FIELD_SEX,sex);

                return new Tuple2<String, String>(sessionId, fullAggrInfo);
            }
        });
        return sessionid2FullAggrInfoRDD;
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