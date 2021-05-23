package org.apache.nifi.gcc.ruledistribute.logicrule;

/**
 * 规则关系枚举
 * @author GCC
 */
public enum Logic {

    AND("&&"),

    OR("||");

    private String name;

    Logic(String name){
        this.name = name;
    }

    public String getValue(){
        return name;
    }

}
