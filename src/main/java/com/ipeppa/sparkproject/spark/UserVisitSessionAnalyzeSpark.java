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
import com.ipeppa.sparkproject.dao.ISessionAggrStatDAO;
import com.ipeppa.sparkproject.dao.ISessionDetailDAO;
import com.ipeppa.sparkproject.dao.ISessionRandomExtractDAO;
import com.ipeppa.sparkproject.dao.ITaskDAO;
import com.ipeppa.sparkproject.dao.impl.DAOFactory;
import com.ipeppa.sparkproject.domin.SessionAggrStat;
import com.ipeppa.sparkproject.domin.SessionDetail;
import com.ipeppa.sparkproject.domin.SessionRandomExtract;
import com.ipeppa.sparkproject.domin.Task;
import com.ipeppa.sparkproject.test.MockData;
import com.ipeppa.sparkproject.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

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
//        String spark_app_name_session = ConfigurationManager.getProperty(Constants.SPARK_APP_NAME_SESSION);
        SparkConf sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = getSQLContext(javaSparkContext.sc());
        //生成模拟测试数据
        mockData(javaSparkContext, sqlContext);
        //创建需要使用的JDBC辅助组件
        ITaskDAO iTaskDAO = DAOFactory.getTaskDAOImpl();
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
        System.out.println(taskId);
        Task task = iTaskDAO.findById(taskId);
        if (task == null) {
            System.out.println("no task exist");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionJavaRDD = getActionRDDByDateRange(sqlContext, taskParam);

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
//        sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = aggregateBysessionId(javaSparkContext, sqlContext, sessionid2actionRDD);
        Accumulator<String> sessionAggrStatAccumulator = javaSparkContext.accumulator(
                "", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2FullAggrInfoRDD, taskParam, sessionAggrStatAccumulator);
//        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

        /**
         * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
         *
         * 从Accumulator中，获取数据，插入数据库的时候，一定要，一定要，是在有某一个action操作以后
         * 再进行。。。
         *
         * 如果没有action的话，那么整个程序根本不会运行。。。
         *
         * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
         * 不对！！！
         *
         * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
         *
         * 计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示
         */
        //聚合统计任务
        System.out.println(filteredSessionid2AggrInfoRDD.count());
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());

        //按时间随机抽取session任务
        randomExtractSession(filteredSessionid2AggrInfoRDD,taskId,sessionid2actionRDD);
        //top10热门品类任务
        getTop10Category(filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
        //top10活跃session任务
        javaSparkContext.close();
    }

    private static void getTop10Category(JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
                                         JavaPairRDD<String, Row> sessionid2actionRDD) {

    }

    /**
     * 随机抽取session
     * @param sessionId2AggrInfoRDD
     */
    private static void randomExtractSession(JavaPairRDD<String, String> sessionId2AggrInfoRDD,
                                             final long taskId,JavaPairRDD<String, Row> sessionId2actionRDD) {
        /**
         * 第一步，计算出每天每小时的session数量
         */
        final JavaPairRDD<String,String> time2SessionIdRDD = sessionId2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String,
                String>, String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo = tuple._2;
                String startTime = StringUtils.getFieldFromConcatString(aggrInfo,"|",Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(startTime);
                return new Tuple2<String, String>(dateHour,aggrInfo);
            }
        });
        // 得到每天每小时的session数量
        Map<String, Object> countMap = time2SessionIdRDD.countByKey();
        /**
         * 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
         */
        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String,Map<String,Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>(10);
        for (Map.Entry<String,Object> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            long count = Long.valueOf(String.valueOf(countEntry.getValue()));
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (null == hourCountMap){
                hourCountMap = new HashMap<String, Long>(1);
                dateHourCountMap.put(date,hourCountMap);
            }
            hourCountMap.put(hour,count);
        }
        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        // <date,<hour,(3,5,20,102)>>
        final Map<String, Map<String, List<Integer>>> dateHourExtractMap =
                new HashMap<String, Map<String, List<Integer>>>();

        Random random = new Random();


        for (Map.Entry<String,Map<String,Long>> dateHourCountEntry : dateHourCountMap.entrySet()){
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
            long daySessionsCount = 0L;
            for(long hourCount : hourCountMap.values()) {
                daySessionsCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if(hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();
                // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                // 就可以计算出，当前小时需要抽取的session数量
                int hourExtractNumber = (int)((count*1.0 / daySessionsCount)
                        * extractNumberPerDay);
                if(hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }
                List<Integer> extractIndexList = hourExtractMap.get(hour);

                if(extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>(3);
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的数量的随机数
                for(int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while(extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }
        /**
         * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
         */
        // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2SessionIdRDD.groupByKey();


        JavaPairRDD<String,String> extractSessionIdsRDD = time2sessionsRDD.flatMapToPair(new PairFlatMapFunction<
                Tuple2<String, Iterable<String>>, String, String>() {

            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>(2);

                String dateHour = tuple._1;
                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];
                Iterator<String> iterator = tuple._2.iterator();
                List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
                ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();
                int index =0;
                while (iterator.hasNext()){
                    String sessionAggrInfo = iterator.next();
                    if (extractIndexList.contains(index)){
                        String sessionId = StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        // 将数据写入MySQL
                        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                        sessionRandomExtract.setTaskId(taskId);
                        sessionRandomExtract.setSessionId(sessionId);
                        sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                        sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                        sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                        sessionRandomExtractDAO.insert(sessionRandomExtract);

                        // 将sessionid加入list
                        extractSessionids.add(new Tuple2<String, String>(sessionId, sessionId));
                    }
                    index++;
                }
                return extractSessionids;
            }
        });
        /**
         * 第四步：获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String,Row>> extractSessionDetailRDD = extractSessionIdsRDD.join(sessionId2actionRDD);
        extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskId(taskId);
                sessionDetail.setUserId(row.getLong(0));
                sessionDetail.setSessionId(row.getString(1));
                sessionDetail.setPageId(row.getLong(2));
                sessionDetail.setActionTime(row.getString(3));
                sessionDetail.setSearchKeyword(row.getString(4));
                sessionDetail.setClickCategoryId(row.getLong(5));
                sessionDetail.setClickProductId(row.getLong(6));
                sessionDetail.setOrderCategoryIds(row.getString(7));
                sessionDetail.setOrderProductIds(row.getString(8));
                sessionDetail.setPayCategoryIds(row.getString(9));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });


    }

    private static double getFieldValue(String value, String param){
        String field = StringUtils.getFieldFromConcatString(value, "\\|", param);
        return Double.valueOf(field == null ? "0":field);
    }

    private static void calculateAndPersistAggrStat(String value, long taskId) {
        String session_count_str = StringUtils.getFieldFromConcatString(value, "|", Constants.SESSION_COUNT);
        long session_count = Long.valueOf(session_count_str == null ? "1":session_count_str);
        
        double visit_length_1s_3s = getFieldValue(value, Constants.TIME_PERIOD_1s_3s);
        double visit_length_4s_6s = getFieldValue(value, Constants.TIME_PERIOD_4s_6s);
        double visit_length_7s_9s = getFieldValue(value, Constants.TIME_PERIOD_7s_9s);
        double visit_length_10s_30s = getFieldValue(value, Constants.TIME_PERIOD_10s_30s);
        double visit_length_30s_60s = getFieldValue(value, Constants.TIME_PERIOD_30s_60s);
        double visit_length_1m_3m = getFieldValue(value, Constants.TIME_PERIOD_1m_3m);
        double visit_length_3m_10m = getFieldValue(value, Constants.TIME_PERIOD_3m_10m);
        double visit_length_10m_30m = getFieldValue(value, Constants.TIME_PERIOD_10m_30m);
        double visit_length_30m = getFieldValue(value, Constants.TIME_PERIOD_30m);

        double step_length_1_3 = getFieldValue(value, Constants.STEP_PERIOD_1_3);
        double step_length_4_6 = getFieldValue(value, Constants.STEP_PERIOD_4_6);
        double step_length_7_9 = getFieldValue(value, Constants.STEP_PERIOD_7_9);
        double step_length_10_30 = getFieldValue(value, Constants.STEP_PERIOD_10_30);
        double step_length_30_60 = getFieldValue(value, Constants.STEP_PERIOD_30_60);
        double step_length_60 = getFieldValue(value, Constants.STEP_PERIOD_60);


        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                 visit_length_1s_3s / session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                 visit_length_4s_6s /   session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                 visit_length_7s_9s / session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                 visit_length_10s_30s /  session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                 visit_length_30s_60s / session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                 visit_length_1m_3m /  session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                 visit_length_3m_10m /  session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                 visit_length_10m_30m /  session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                 visit_length_30m /  session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                 step_length_1_3 /  session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                 step_length_4_6 /  session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                 step_length_7_9 /  session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                 step_length_10_30 / session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                step_length_30_60 / session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                 step_length_60 /  session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTask_id(taskId);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

    private static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD<String, String> sessionid2FullAggrInfoRDD,
                                                                        final JSONObject taskParam, final Accumulator<String> sessionAggrStatAccumulator) {

        final String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        final String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        final String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        if(_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD = sessionid2FullAggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo = tuple._2;
                //按照年龄范围进行过滤（startAge、endAge）
                if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, startAge, endAge)) {
                    return false;
                }
                //按照职业范围筛选
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }
                //按照城市筛选
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                    return false;
                }
                //按照性别筛选
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                    return false;
                }


                // 按照搜索词进行过滤
                // 我们的session可能搜索了 火锅,蛋糕,烧烤
                // 我们的筛选条件可能是 火锅,串串香,iphone手机
                // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                // 任何一个搜索词相当，即通过
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }

                // 按照点击品类id进行过滤
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "|", Constants.FIELD_STEP_LENGTH));
                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);
                return true;
            }

            private void calculateStepLength(long stepLength) {
                if (stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if (stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if (stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if (stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if (stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if (stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }

            private void calculateVisitLength(long visitLength) {
                if (visitLength >= 1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if (visitLength >= 4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if (visitLength >= 7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if (visitLength >= 10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if (visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if (visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if (visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if (visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if (visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }
        });
        return filteredSessionId2AggrInfoRDD;
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
     *
     * @param sqlContext SQLContext
     * @param taskParam  任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action where date>='" + startDate + "' and date <= '" + endDate + "'";
        System.out.println(sql);
        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();
    }

    /*
     * @Description:
     * @Date: 2020/5/19 15:25
     * @param: [actionJavaRDD]
     * @return:
     **/
    private static JavaPairRDD<String, String> aggregateBysessionId(JavaSparkContext sc, SQLContext sqlContext,
                                                                    JavaPairRDD<String, Row> sessinoid2actionRDD) {
        //groupByKey 算子 对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD = sessinoid2actionRDD.groupByKey();
        //[2020-05-19,76,d8407eb844194106a25941c001b1ca1e,5,2020-05-19 18:35:01,null,null,null,79,60,null,null]
        //date,userId,sessionId,pageId,visit_time,search_keyword,click_category_id,
        // click_product_id,order_category_ids,order_product_ids,pay_category_ids,pay_product_ids
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = sessionId2ActionsRDD.mapToPair(new PairFunction<Tuple2<String,
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
                while (allVisitInfo4OneSessionId.hasNext()) {
                    Row row = allVisitInfo4OneSessionId.next();
                    if (null == userId) {
                        userId = row.getLong(1);
                    }
                    String searchWord = row.getString(5);
                    Long clickCategoryId = row.getLong(6);
                    // 实际上真实数据 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                    if (StringUtils.isNotEmpty(searchWord)) {
                        if (!searchKeywordsBuffer.toString().contains(searchWord)) {
                            searchKeywordsBuffer.append(searchWord.concat(","));
                        }
                    }
                    if (null != clickCategoryId) {
                        if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                            clickCategoryIdsBuffer.append(String.valueOf(clickCategoryId).concat(","));
                        }
                    }
                    // 计算session开始和结束时间  "yyyy-MM-dd HH:mm:ss"  格式
                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    if (startTime == null) {
                        startTime = actionTime;
                    }
                    if (endTime == null) {
                        endTime = actionTime;
                    }
                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }
                    stepLength++;
                }

                String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                // 计算session访问时长（秒）
                long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
                String partAggrInfo = String.format("%s=%s|%s=%s|%s=%s|%s=%s|%s=%s|%s=%s",
                        Constants.FIELD_SESSION_ID, sessionId, Constants.FIELD_SEARCH_KEYWORDS, searchKeywords,
                        Constants.FIELD_CLICK_CATEGORY_IDS, clickCategoryIds, Constants.FIELD_VISIT_LENGTH, visitLength,
                        Constants.FIELD_STEP_LENGTH, stepLength, Constants.FIELD_START_TIME, DateUtils.formatTime(startTime));

                return new Tuple2<Long, String>(userId, partAggrInfo);
            }
        });

        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });

        /**
         * 这里就可以说一下，比较适合采用reduce join转换为map join的方式
         *
         * userid2PartAggrInfoRDD，可能数据量还是比较大，比如，可能在1千万数据
         * userid2InfoRDD，可能数据量还是比较小的，你的用户数量才10万用户
         * <Long,Tuple2<String,Row>>  = <userId,Tuple2<partAggrInfo,userInfo>>
         */
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);
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
                String fullAggrInfo = String.format("%s=%s|%s=%s|%s=%s|%s=%s|%s=%s", partAggrInfo,
                        Constants.FIELD_AGE, age, Constants.FIELD_PROFESSIONAL, professional,
                        Constants.FIELD_CITY, city, Constants.FIELD_SEX, sex);

                return new Tuple2<String, String>(sessionId, fullAggrInfo);
            }
        });
        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }
}