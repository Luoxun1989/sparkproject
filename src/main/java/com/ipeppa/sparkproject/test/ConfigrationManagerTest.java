/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: ConfigrationManagerTest
 * Author:   admin
 * Date:     2020/3/30 23:21
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.test;

import com.ipeppa.sparkproject.conf.ConfigurationManager;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author admin
 * @create 2020/3/30
 * @since 1.0.0
 */
public class ConfigrationManagerTest {
    public static void main(String[] args) {


        String key1 = ConfigurationManager.getProperty("key1");
        String key2 = ConfigurationManager.getProperty("key2");
        System.out.println(key1);
        System.out.println(key2);
    }

}