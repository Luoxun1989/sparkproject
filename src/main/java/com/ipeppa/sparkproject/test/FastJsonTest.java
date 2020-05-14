/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: FastJsonTest
 * Author:   admin
 * Date:     2020/4/4 23:05
 * Description: JSON测试
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.ipeppa.sparkproject.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 〈一句话功能简述〉<br> 
 * 〈JSON测试〉
 *
 * @author admin
 * @create 2020/4/4
 * @since 1.0.0
 */
public class FastJsonTest {
    public static void main(String[] args) {
        String json ="[{'姓名':'安安','班级':'1','年级':'大二','科目':'高数','成绩':'80'},{'姓名':'呢呢','班级':'1','年级':'大二','科目':'高数','成绩':'88'},{'姓名':'丽丽','班级':'1','年级':'大二','科目':'高数','成绩':'85'}]";
        JSONArray jsonArray = JSONArray.parseArray(json);
        JSONObject jsonObject = jsonArray.getJSONObject(2);
        System.out.println(jsonObject.getString("姓名"));
    }

}