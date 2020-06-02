package com.ipeppa.sparkproject.domin;

import lombok.Getter;
import lombok.Setter;

/**
 * “Go Further进无止境” <br>
 * 〈〉
 *
 * @author Luoxun
 * @create 2020/6/2
 * @since 1.0.0
 */
@Setter
@Getter
public class FakeUserInfoData {
    String msisdn;//varchar2(20),y,,身份idString
    String userName;//varchar2(20),y,,身份idString
    int age;
    String professional;
    String sex;
    String brand;//varchar2(255),y,,终端品牌String
    String provinceId;//varchar2(16),y,,省份idString
    String cityId;//varchar2(16),y,,城市idString

    @Override
    public String toString() {
        return String.format("%s|%s|%s|%s|%s|%s|%s|%s",msisdn,userName,age,sex,professional,brand,provinceId,cityId);
    }
}
