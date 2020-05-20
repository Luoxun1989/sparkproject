/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: SessionAggrStat
 * Author:   admin
 * Date:     2020/5/20 22:50
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.domin;

import lombok.Getter;
import lombok.Setter;

/**
 * 〈一句话功能简述〉<br> 
 * 〈Session聚合统计  计算每个时间范围内 每个访问步长范围内 session占比〉
 *
 * @author admin
 * @create 2020/5/20
 * @since 1.0.0
 */

@Setter
@Getter
public class SessionAggrStat {
    double visit_length_1s_3s_ratio;
    double visit_length_4s_6s_ratio;
    double visit_length_7s_9s_ratio;
    double visit_length_10s_30s_ratio;
    double visit_length_30s_60s_ratio;
    double visit_length_1m_3m_ratio;
    double visit_length_3m_10m_ratio;
    double visit_length_10m_30m_ratio;
    double visit_length_30m_ratio;

    double step_length_1_3_ratio;
    double step_length_4_6_ratio;
    double step_length_7_9_ratio;
    double step_length_10_30_ratio;
    double step_length_30_60_ratio;
    double step_length_60_ratio;

    long task_id;
    long session_count;

}