package org.apache.nifi.gcc.ruledistribute;

/**
 * 规则处理器接口
 * @author GCC
 * @date 2021-05-10
 */
public interface Ruler {


    /**
     * 规则语法的验证
     * @param ruleStr 规则语法
     * @return 布尔
     */
    public boolean verifyRuleStr(String ruleStr);


    /**
     * 初始化rule
     * @param ruleStr 规则字符串
     */
    public void initRule(String ruleStr);


    /**
     * 筛选出符合规则的
     * @param data 待判断数据
     * @return 符合规则的
     */
    public String accordRule(String data);

    /**
     * 规则是否可用
     * @return 布尔
     */
    public boolean isAvailable();


}
