/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: SessionAggrStatAccumulator
 * Author:   admin
 * Date:     2020/4/15 22:10
 * Description:  自定义accumulate
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.spark;

import com.ipeppa.sparkproject.constant.Constants;
import com.ipeppa.sparkproject.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * 〈一句话功能简述〉<br> 
 * 〈自定义Accumulator〉
 *
 * @author admin
 * @create 2020/4/15
 * @since 1.0.0
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {
    private static final long serialVersionUID = 6311074555136039130L;

    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String zero(String v) {
        String s = String.format("%s=0|%s=0|%s=0|%s=0|%s=0|%s=0|%s=0|%s=0|%s=0|%s=0|%s=0|%s=0|%s=0",
                Constants.SESSION_COUNT, Constants.TIME_PERIOD_1s_3s, Constants.TIME_PERIOD_4s_6s, Constants.TIME_PERIOD_7s_9s,
                Constants.TIME_PERIOD_10s_30s, Constants.TIME_PERIOD_30s_60s, Constants.TIME_PERIOD_1m_3m, Constants.TIME_PERIOD_3m_10m,
                Constants.TIME_PERIOD_10m_30m, Constants.TIME_PERIOD_30m, Constants.STEP_PERIOD_1_3, Constants.STEP_PERIOD_4_6, Constants.STEP_PERIOD_7_9,
                Constants.STEP_PERIOD_10_30, Constants.STEP_PERIOD_30_60, Constants.STEP_PERIOD_60);
        return s;
    }
    private String add(String v1, String v2) {
        // 校验：v1为空的话，直接返回v2
        if(StringUtils.isEmpty(v1)) {
            return v2;
        }

        // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if(oldValue != null) {
            // 将范围区间原有的值，累加1
            int newValue = Integer.valueOf(oldValue) + 1;
            // 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }

        return v1;
    }
}