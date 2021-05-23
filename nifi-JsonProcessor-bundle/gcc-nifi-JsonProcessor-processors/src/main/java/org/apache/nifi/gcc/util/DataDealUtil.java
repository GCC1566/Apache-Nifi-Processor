package org.apache.nifi.gcc.util;


import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;


/**
 * 数据处理工具类
 * @author GCC
 * @date 2021-05-10
 */
public class DataDealUtil {

    private static DataDealUtil datadealor = null;

    private DataDealUtil(){}

    public static DataDealUtil getInstance(){
        if(datadealor == null){
            datadealor = new DataDealUtil();
        }
        return datadealor;
    }

    /**
     * 将Json格式化的字符串转换为JSON对象
     * 若数据不符合则返回空JSON
     * @param datas
     * @return
     */
    public JSONObject formatJsonDatas(String datas){
        JSONObject json = new JSONObject();
        try{
            json = JSONUtil.parseObj(datas);
        }catch (Exception e){

        }
        return json;
    }

    /**
     * 将JsonArray格式化为JSONArray对象
     * @param datas
     * @return
     */
    public JSONArray formatJsonArrayDatas(String datas){
        JSONArray jsonArray = new JSONArray();
        try{
            jsonArray = JSONUtil.parseArray(datas);
        }catch (Exception e){

        }
        return jsonArray;
    }

}
