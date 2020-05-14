/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: ConfigurationManager
 * Author:   admin
 * Date:     2020/3/30 23:12
 * Description: 配置管理组件
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 〈一句话功能简述〉<br> 
 * 〈配置管理组件〉
 *
 * @author admin
 * @create 2020/3/30
 * @since 1.0.0
 */
public class ConfigurationManager {
    private static Properties prop = new Properties();
    static {
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            prop.load(in);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static String getProperty(String key){
        return prop.getProperty(key);
    }

    public static boolean getBoolean(String key){
        try {
            return Boolean.valueOf(prop.getProperty(key));
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public static Long getLong(String key) {
        try {
            return Long.valueOf(prop.getProperty(key));
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0L;
    }

    public static int getInt(String key) {
        try {
            return Integer.valueOf(prop.getProperty(key));
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }
}