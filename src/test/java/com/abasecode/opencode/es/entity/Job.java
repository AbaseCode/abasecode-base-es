package com.abasecode.opencode.es.entity;

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
public class Job extends BaseT implements Serializable {
    /**
     * 职位名称
     */
    private String jobName;
    /**
     * 职位描述
     */
    private String jobDesc;
    /**
     * 职位类型
     */
    private String jobPosition;
    /**
     * 招聘类型
     */
    private String workType;
    /**
     * 经验年限
     */
    private Integer jobExperienceYear;
    /**
     * 学历要求
     */
    private String jobEducational;
    /**
     * 最低薪资
     */
    private Integer salaryMin;
    /**
     * 最高薪资
     */
    private Integer salaryMax;
    /**
     * 薪资数量
     */
    private Integer salaryMonth;
    /**
     * 奖金
     */
    private String bonus;
    /**
     * 职位关键字
     */
    private Set<String> jobTag;
    /**
     * 工作地点
     */
    private String address;
    /**
     * 地址位置
     */
    private String location;
    /**
     * 状态
     */
    private Boolean status;
    /**
     * 公司信息
     */
    private Company company;

}
