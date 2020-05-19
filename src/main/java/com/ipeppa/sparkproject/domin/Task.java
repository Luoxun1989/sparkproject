/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Task
 * Author:   admin
 * Date:     2020/4/2 23:37
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.domin;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author admin
 * @create 2020/4/2
 * @since 1.0.0
 */
@Setter
@Getter
public class Task implements Serializable {
    private static final long serialVersionUID = 3518776796426921776L;

    private long taskId;
    private String taskName;
    private String createTime;
    private String startTime;
    private String finishTime;
    private String taskType;
    private String taskStatus;
    private String taskParam;

}