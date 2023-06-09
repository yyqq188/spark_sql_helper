package com.yhl.parser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yhl.entity.T_PERM_ARAP_SOURCE;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;


public class T_PERM_ARAPParser extends AbstractParser implements Serializable {
    @Override
    public T_PERM_ARAP_SOURCE parse(String str) throws Exception {
        //data是这次更新的字段 key是原有全部字段，需要用data里的变更值去更新key的有关字段
        JSONObject jsonObject = JSON.parseObject(str);
        JSONObject meta = jsonObject.getJSONObject("meta");
        JSONObject data = jsonObject.getJSONObject("data");
        JSONObject key = jsonObject.getJSONObject("key");
        if(Objects.isNull(key)) {
            key = new JSONObject();
        }

        String op = "";
        try {
            op = meta.getString("op");
        } catch (Exception e) {
            System.out.println("op is null");
        }
        //如果是upd操作的话，会有data选项。然后将data中更新的字段和key中原来的全部字段进行合并
        if("upd".equals(op)) {
            Set<String> dataskey = data.keySet();
            for(String datakey:dataskey){
                key.remove(datakey);
                //if(!StringUtils.isBlank(datakey) && key.containsKey(datakey)){
                //
                //}
            }
            key.putAll(data);
            key.putAll(meta);
        }
        //ins操作没有key
        if("ins".equals(op)){
            //data.putAll(meta);
            key.putAll(data);
            key.putAll(meta);
        }
        //todo 这里还有待观察，估计不对    del操作没有key
        if("del".equals(op)){
            //data.putAll(meta);
            key.putAll(data);
            key.putAll(meta);
        }
        return  key.toJavaObject(T_PERM_ARAP_SOURCE.class);
    }
}
