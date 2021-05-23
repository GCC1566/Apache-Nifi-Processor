package org.apache.nifi.gcc.ruledistribute.logicrule;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.nifi.documentation.init.NopComponentLog;
import org.apache.nifi.gcc.ruledistribute.Ruler;
import org.apache.nifi.gcc.util.DataDealUtil;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.List;

/**
 * 字段规则器
 * @author GCC
 * @date 根据规则进行分发
 */
public class FieldValueRuler implements Ruler {


    ComponentLog logger = new NopComponentLog();
    //规则逻辑
    private Logic logic;
    //详细规则
    private List<JSONObject> fields = new ArrayList<>();
    //规则是否可用
    private boolean useflag = false;
    //规则表达式
    private JSONObject rulejson ;

    @Override
    public boolean verifyRuleStr(String ruleStr) {
        rulejson = DataDealUtil.getInstance().formatJsonDatas(ruleStr);
        if(!rulejson.isEmpty() && rulejson.containsKey("logic") && rulejson.containsKey("fields")){
              String logic = rulejson.getStr("logic");
              JSONArray fields = rulejson.getJSONArray("fields");
              JSONObject field = fields.getJSONObject(0);
              if(!logic.trim().equals("&&") && !logic.trim().equals("||")){
                  return false;
              }
              if(field.containsKey("field") && field.containsKey("value")){
                  return JSONUtil.isJsonArray(JSONUtil.toJsonStr(field.get("value")));
              }
        }
        return false;
    }

    @Override
    public boolean isAvailable() {
        return useflag;
    }

    @Override
    public void initRule(String ruleStr){
        if(verifyRuleStr(ruleStr)) {
           try {
               String logicstr = rulejson.getStr("logic").trim();
               if(logicstr.equals("&&")){
                   this.logic = Logic.AND;
               }
               if(logicstr.equals("||")){
                   this.logic = Logic.OR;
               }
               fields = rulejson.getJSONArray("fields").toList(JSONObject.class);
               useflag = true;
           }catch (Exception e){
               logger.error(e.getMessage());
           }

        }
    }

    @Override
    public String accordRule(String data){

        if(JSONUtil.isJsonArray(data)){
            List<JSONObject> result = new ArrayList<>();
            JSONArray array = DataDealUtil.getInstance().formatJsonArrayDatas(data);
            for(Object json:array){
                if(execuRules(JSONUtil.parseObj(json))){
                    result.add(JSONUtil.parseObj(json));
                }
            }
            return result.isEmpty() ? "":JSONUtil.toJsonStr(result);
        }else if(JSONUtil.isJsonObj(data)){
            JSONObject json = DataDealUtil.getInstance().formatJsonDatas(data);
            if(execuRules(json)){
                return data;
            }

        }
        return "";
    }

    /**
     * 规则执行
     * @param jsonObject 待判断参数
     * @return 结论
     */
    private boolean execuRules(JSONObject jsonObject){
       boolean flag = false;
       if(logic == Logic.AND){
           for(JSONObject rulefield:fields){
               String field = rulefield.getStr("field");
               JSONArray values = rulefield.getJSONArray("value");
               if(!jsonObject.containsKey(field)){
                    return false;
               }else if(!values.contains(jsonObject.get(field))){
                    return false;
               }
           }
           return true;
       }else if(logic == Logic.OR){
           for(JSONObject rulefield:fields){
               String field = rulefield.getStr("field");
               JSONArray values = rulefield.getJSONArray("value");
                if(jsonObject.containsKey(field)){
                    if(values.contains(jsonObject.get(field))){
                        return true;
                    }
                }
           }
       }
       return flag;
    }

    public Logic getLogic() {
        return logic;
    }

    public void setLogic(Logic logic) {
        this.logic = logic;
    }

    public List<JSONObject> getFields() {
        return fields;
    }

    public void setFields(List<JSONObject> fields) {
        this.fields = fields;
    }
}
