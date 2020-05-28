package com.ipeppa.sparkproject.domin;

import lombok.Getter;
import lombok.Setter;

/**
 * “Go Further进无止境” <br>
 * 〈〉
 *
 * @author Luoxun
 * @create 2020/5/28
 * @since 1.0.0
 */
@Setter
@Getter
public class SessionDetail {
    private long taskId;
    private long userId;
    private String sessionId;
    private long pageId;
    private String actionTime;
    private String searchKeyword;
    private long clickCategoryId;
    private long clickProductId;
    private String orderCategoryIds;
    private String orderProductIds;
    private String payCategoryIds;
    private String payProductIds;

}
