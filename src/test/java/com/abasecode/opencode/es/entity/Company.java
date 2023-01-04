package com.abasecode.opencode.es.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.Set;

/**
 * @author Jon
 * url: <a href="https://jon.wiki">Jon's blog</a>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Company extends BaseT implements Serializable {
    /**
     * 简称
     */
    private String abbreviation;
    /**
     * 名称
     */
    private String name;
    /**
     * 统一社会信用代码
     */
    private String creditCode;
    /**
     * 成立日期
     */
    private String startDate;
    /**
     * 法定代表人
     */
    private String represent;
    /**
     * 存续状态
     */
    private String status;
    /**
     * 企业工商注册号
     */
    private String no;
    /**
     * 注册资本
     */
    private String registerCapital;
    /**
     * 实缴资本
     */
    private String paidCapital;
    /**
     * LOGO
     */
    private String logo;
    /**
     * 行业
     */
    private String industry;
    /**
     * 融资阶段
     */
    private String financingStage;
    /**
     * 雇员数量，人员规模
     */
    private String staffSize;
    /**
     * 公司介绍
     */
    private String description;
    /**
     * 工作开始时间
     */
    private String workTimeBegin;
    /**
     * 工作结束时间
     */
    private String workTimeEnd;
    /**
     * 加班情况
     */
    private String overTime;
    /**
     * 休息时间
     */
    private String breakTime;
    /**
     * 福利
     */
    private Set<String> welfare;
    /**
     * 注册地点
     */
    private String address;
    /**
     * 地址位置
     */
    private String location;
    /**
     * 省市县区镇
     */
    private Pcct pcct;
}
