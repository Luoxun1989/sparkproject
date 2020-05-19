/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TaskDAOImpl
 * Author:   admin
 * Date:     2020/4/2 23:31
 * Description: 数据访问接口实现
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.dao.impl;

import com.ipeppa.sparkproject.dao.ITaskDAO;
import com.ipeppa.sparkproject.domin.Task;
import com.ipeppa.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;

/**
 * 〈一句话功能简述〉<br> 
 * 〈数据访问接口实现〉
 *
 * @author admin
 * @create 2020/4/2
 * @since 1.0.0
 */
public class TaskDAOImpl implements ITaskDAO {
    final Task task = new Task();

    @Override
    public Task findById(long taskid) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String sql = "select * from task where task_id=?";
        Object[] params = new Object[]{taskid};
        jdbcHelper.executeQuery(sql,params, new JDBCHelper.QueryCallback(){

            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()){
                    long taskId = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskId(taskId);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });
        return task;
    }
}