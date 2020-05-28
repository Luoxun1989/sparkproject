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
@Getter
@Setter
public class SessionRandomExtract {
    private long taskId;
    private String sessionId;
    private String searchKeywords;
    private String startTime;
    private String clickCategoryIds;

}
