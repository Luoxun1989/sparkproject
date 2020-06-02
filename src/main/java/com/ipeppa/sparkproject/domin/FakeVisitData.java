package com.ipeppa.sparkproject.domin;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * “Go Further进无止境” <br>
 * 〈〉
 *
 * @author Luoxun
 * @create 2020/6/2
 * @since 1.0.0
 */
@Getter
@Setter
public class FakeVisitData {
    String msisdn;//varchar2(20),y,,身份idString
    String recordTime;//date,y,,访问时间格式yyyymmddhhmmssString
    String sessionId;//varchar2(255),y,,会话idString
    int bookId;//number,y,,图书idString

    String currentCatalog;//varchar2(32),y,,当前目录String
    int chargeType;//number,y,,收费类型：免费：0收费：1String
    int sourceFrom;//number,y,,内容来源（虚拟节点）：从目录访问：1从排行榜访问：-11从搜索结果：-12从收藏夹：-13从订单夹：-14从系统书签：
    // -15从推荐短信访问：-16从赠送短信访问：-17从作家作品访问 ：-18从连载预定访问 ：-19从智能推荐访问 : -20从用户书签：-21从接上次看：
    // -22实体书关联电子书访问：-98其他：-99 String
    String sale_parameter;//varchar2(32),y,,渠道代码0000：自有平台直接接入0001：移动梦网接入String
    String access_points;//varchar2(32),y,,1、wlan 2、cmwap 3、cmnet 4、unknownString
    int loginType;//number,y,,登录类型 1 通过cmwap直接登录 2 通过业务直连方式登录 3 通过用户名和密码方式登录 4 游客登录String
    int visitType;//number(2),y,,1、  单机包2、  插件注：目前主要是wap漫画频道使用，平台侧可以为空String
    int sourceType;//number,y,,运营频道类型1  书籍运营频道2  漫画运营频道3  杂志运营频道4  互动运营频道5  听书运营频道6  手机报7
    // 新浪微博8  新浪阅读频道13  ues 杂志14  ues 书籍15  行业企业书屋16  行业移动学习17  ues手机报18  ues漫画19  ues听书20
    // ues统一登录21  ues rdo22  ues-vip（会员分站）23  ues互动 String
    String sourceScene;//varchar2(512),y,,详情页、阅读页的访问来源场景：1：从专区访问、2：从排行榜访问(2_排行类型_排行周期)、
    // 3：从搜索结果(3_书id_章节id_搜索id)、4：从收藏夹、5：从订单夹、6：从系统书签、7：从推荐短信访问(7_推荐人手机号码_推荐类型_事务id)、
    // 8：从赠送短信访问(8_赠送手机号码_推荐类型_事务id)、9：从作家作品访问、10：从连载预定访问、11：从智能推荐访问、12：从用户书签、
    // 13：从接上次看、  14：实体书关联电子书访问、 15：从书单访问、99：其他String


    @Override
    public String toString() {
        return String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",msisdn,recordTime,sessionId,bookId,
                currentCatalog,chargeType,sourceFrom,sale_parameter,access_points,loginType,visitType,
                sourceType,sourceScene);
    }
}
