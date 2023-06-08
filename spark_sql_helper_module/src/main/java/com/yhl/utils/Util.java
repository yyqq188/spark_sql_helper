package com.yhl.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yhl.entity.source.T_PERM_ARAP_SOURCE;
import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.Template;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.io.*;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Util {
    //解析T_PERM_ARAP
    public static T_PERM_ARAP_SOURCE parseT_PERM_ARAP(String jsonStr) {
        //data是这次更新的字段 key是原有全部字段，需要用data里的变更值去更新key的有关字段
        JSONObject jsonObject = JSON.parseObject(jsonStr);
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

    public static Template getTemplate(String vmFilePath){
        final VelocityEngine velocityEngine = new VelocityEngine();
        velocityEngine.setProperty(RuntimeConstants.RESOURCE_LOADER,"classpath");
        velocityEngine.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        velocityEngine.init();
        final Template template = velocityEngine.getTemplate(vmFilePath);
        return template;
    }
    public static Writer getFileWriter(String javaFilePath) throws IOException {
        return new PrintWriter(new File(javaFilePath));
    }
    //首字母大写
    public static String LargerFirstChar(String str) {
        char[] chars = str.toCharArray();
        chars[0] -= 32;
        if(chars[0]>97){
        }
        return String.valueOf(chars);
    }

    /**
     * hash算法
     */
    public static String specialRowKeyTransform(String rowKey){
        //主键hash值模100取绝对值
        int a = Math.abs(rowKey.hashCode()) % 100;
        String x = rowKey;
        if(a < 10){
            x = "" + a + "_" + x;
        }else{
            x = a + "_" + x;
        }
        //返回值 为 a_rowKey
        return x;
    }
    public static String readFileContent(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        StringBuffer sbf = new StringBuffer();
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                sbf.append(tempStr);
            }
            reader.close();

            return sbf.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return sbf.toString();
    }

    //将字符写入文件
    public static void writetoFile(String str,String filePath) throws IOException {
        //String str = "风声雨声读书声";
        File txt=new File(filePath);
        if(!txt.exists()){
            txt.createNewFile();
        }
        byte bytes[]=new byte[512];
        bytes=str.getBytes();
        //int b=bytes.length;   //是字节的长度，不是字符串的长度
        FileOutputStream fos=new FileOutputStream(txt);
        //fos.write(bytes,0,b);
        fos.write(bytes);
        fos.close();


    }

}
