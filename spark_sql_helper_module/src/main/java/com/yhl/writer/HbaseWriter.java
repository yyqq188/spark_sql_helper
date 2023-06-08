package com.yhl.writer;

import com.alibaba.fastjson.JSONObject;
import com.yhl.baks.datax_config.DataxConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HbaseWriter extends AbstractWriter implements Serializable {

    @Override
    public void writer(Iterator<Row> iterator,DataxConfig pluginConfig) throws Exception {
        DataxConfig parameter = pluginConfig.getConfiguration("parameter");
        String tableName = pluginConfig.getString("tableName");
        String family = pluginConfig.getString("family");
        String rowkey = pluginConfig.getString("rowkey");
        List<String> columns = pluginConfig.getList("columns", String.class);
        Configuration configuration = HBaseConfiguration.create();
        JSONObject parameterJsonObj = JSONObject.parseObject(parameter.toJSON());
        for(String key:parameterJsonObj.keySet()){
            configuration.set(key,parameterJsonObj.getString(key));
        }
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table hbaseOutTable = connection.getTable(TableName.valueOf(tableName));

        List<Put> puts  = new ArrayList<>();
        while (iterator.hasNext()) {
            Row next = iterator.next();

            //将put写入hbase
            Put put = new Put(next.getAs(rowkey).toString().getBytes());
            for(String column:columns){
                put.addColumn(family.getBytes(),column.getBytes(),
                        next.getAs(column).toString().getBytes());
            }

            puts.add(put);
        }
        hbaseOutTable.put(puts);


    }

}
