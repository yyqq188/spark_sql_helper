package com.yhl.utils;

import com.yhl.config.HbaseConfig;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseUtil {
    public static Connection getHBaseConnection(String env) throws IOException {
        Configuration configuration = HbaseConfig.getHBaseConf(env);
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    // ==============================hbase 常用的工具类  todo 注意这里只针对单family ===================
    /**
     * 根据rowkey 查找 所有列的值
     */
    public static Map<String,String> getRowKeyInfo(
            String tableName ,String rowKey ,Connection connection ) throws Exception{
        Map<String,String> mapResult = new HashMap<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result  = table.get(get);
        if (!result.isEmpty()) {
            for (Cell cell : result.listCells()) {
                String qualifier = new String(CellUtil.cloneQualifier(cell),"UTF-8");
                String value = new String(CellUtil.cloneValue(cell),"UTF-8");
                mapResult.put(qualifier,value);
            }
        }
        table.close();
        return mapResult;
    }

    /**
     * 根据rowkey family qualifier查找指定column列
     */
    public static Map<String,String> getRowKeyInfo(
            String tableName , String family,List<String> columns,
            String rowKey ,Connection connection ) throws Exception{
        Map<String,String> mapResult = new HashMap<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        for(String column:columns) {
            get.addColumn(family.getBytes(),column.getBytes());
        }

        Result result  = table.get(get);
        if (!result.isEmpty()) {
            for (Cell cell : result.listCells()) {
                String column = new String(CellUtil.cloneQualifier(cell),"UTF-8");
                String value = new String(CellUtil.cloneValue(cell),"UTF-8");
                mapResult.put(column,value);
            }
        }
        table.close();
        return mapResult;
    }
    /**
     * 带二级索引表的查找 指定列的值
     */
    public static  Map<String,Map<String,String>> getDataBySecondIndex(
            String secondIndexRowkey, String secondIndexTable,
            String tableName, String family,List<String> columns,Connection connection) throws Exception{
        //查找二级索引
        List<String> array = new ArrayList<>();
        if (!StringUtils.isBlank(secondIndexRowkey)) {
            Table table  = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(secondIndexRowkey));
            Result result = table.get(get);
            for (Cell cell:result.listCells()) {
                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                array.add(value);
            }
            table.close();
        }

        //array中有二级索引的结果，该结果是数据表的rowkey集合
        List<Get> getList = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (String rowkey:array) {
            if (!StringUtils.isBlank(rowkey)) {
                Get get = new Get(Bytes.toBytes(rowkey));
                for(String column:columns){
                    get.addColumn(family.getBytes(),column.getBytes());
                }
                getList.add(get);
            }
        }
        Result[] results = table.get(getList);
        Map<String,Map<String,String>> mapResult = new HashMap<>();
        for (Result r: results) {
            Map<String,String> mapQV = new HashMap<>();
            String rowkey = new String(r.getRow(),"UTF-8");
            for (Cell cell:r.listCells()) {
                String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                mapQV.put(qualifier,value);
            }
            mapResult.put(rowkey,mapQV);
        }
        table.close();
        return mapResult;
    }
}
