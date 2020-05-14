/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TaskDAOTest
 * Author:   admin
 * Date:     2020/4/4 22:57
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.test;

import com.ipeppa.sparkproject.dao.ITaskDAO;
import com.ipeppa.sparkproject.dao.impl.DAOFactory;
import com.ipeppa.sparkproject.domin.Task;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author admin
 * @create 2020/4/4
 * @since 1.0.0
 */
public class TaskDAOTest {
    public static void main(String[] args) {
        ITaskDAO iTaskDAO = DAOFactory.getTaskDAOImpl();
        Task task = iTaskDAO.findById(2);
        System.out.println(task.getTaskName());
    }
}