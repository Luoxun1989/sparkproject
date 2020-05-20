/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: DAOFactory
 * Author:   admin
 * Date:     2020/4/4 22:53
 * Description: DAO工厂类
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.dao.impl;

import com.ipeppa.sparkproject.dao.ISessionAggrStatDAO;
import com.ipeppa.sparkproject.dao.ITaskDAO;

/**
 * 〈一句话功能简述〉<br> 
 * 〈DAO工厂类〉
 *
 * @author admin
 * @create 2020/4/4
 * @since 1.0.0
 */
public class DAOFactory {
    public static ITaskDAO getTaskDAOImpl(){
        return new TaskDAOImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }
}