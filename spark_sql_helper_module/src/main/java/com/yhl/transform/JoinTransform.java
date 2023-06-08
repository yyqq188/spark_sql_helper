package com.yhl.transform;

import com.yhl.baks.datax_config.DataxConfig;
import com.yhl.utils.HbaseUtil;
import com.yhl.utils.Util;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class JoinTransform extends AbstractTransform implements Serializable {
    public static Logger logger
              = Logger.getLogger(JoinTransform.class.getSimpleName());
    public JoinTransform(SparkSession spark, DataxConfig pluginConfig) {
        super(spark, pluginConfig);
    }


    @Override
    public Dataset<Row> transform(Dataset<Row> dataset) throws Exception{
        //hbase family
        String family = pluginConfig.getString("parameter.family");
        //各种条件的解析
        String tableName = pluginConfig.getString("parameter.tableName");
        //关联用到的join 条件
//        List<String> joinColumns = pluginConfig.getList("parameter.joinColumns",String.class);
        List<String> joinColumns = Arrays.asList(pluginConfig.getString("parameter.joinColumns"));
        //hash算法
        String joinHash = pluginConfig.getString("parameter.joinColumnsHash");

        String joinDelimiter = pluginConfig.getString("parameter.joinDelimiter");
        //关联之后的结果schema
        String ddlStr = pluginConfig.getString("parameter.ddl");

        Boolean isShow = Objects.isNull(pluginConfig.getString("parameter.isShow"))?
                false:(pluginConfig.getString("parameter.isShow").equals("false"))?false:true;

        //String joinColumns2 = "";
        //if(joinColumns.size()>1) {
        //    //分隔符
        //
        //    joinColumns2 = String.join(joinDelimiter,joinColumns);
        //}else{
        //    joinColumns2 = joinColumns.get(0);
        //}






        //这里是为后面生成dataframe提供schema
        String columnStr = ddlStr
                .replaceAll("string", "")
                .replaceAll("long", "")
                .replaceAll("int", "")
                .replaceAll("\\s+", "");

        String[] columns = columnStr.split(",");

        //dataset转化为rdd
        JavaRDD<Row> rowJavaRDD = dataset.toJavaRDD();
        //join操作
        JavaRDD<Row> rowJavaRDD1 = rowJavaRDD.mapPartitions(partition -> {
            //获得hbase连接
            Connection connection = HbaseUtil.getHBaseConnection("prod");
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Get> gets = new ArrayList<>();
            while (partition.hasNext()) {
                Row next = partition.next();
                List<String> collect = joinColumns.stream().map(joinColumn ->
                        next.getString(next.fieldIndex(joinColumn)))
                        .collect(Collectors.toList());
                String rowkeyStr = "";
                if (Objects.isNull(joinColumns)) {
                    rowkeyStr = String.join("", collect);
                } else {
                    rowkeyStr = String.join(joinDelimiter, collect);
                }

                if ("hash".equals(joinHash)) {
                    rowkeyStr = Util.specialRowKeyTransform(rowkeyStr);
                }

                //从dataset中解析出需要的rowkey
                Get get = new Get(Bytes.toBytes(rowkeyStr));
                for (String column : columns) {
                    get.addColumn(family.getBytes(), column.getBytes());
                }

                gets.add(get);

            }
            //批量返回
            Result[] results = table.get(gets);
            List<Row> rows = new ArrayList<>();
            for (Result r : results) {
                List<String> list = new ArrayList<>();
                if(Objects.isNull(r.getRow())) continue;
                String rowkey = new String(r.getRow(), "UTF-8");
                //list.add(rowkey);  //由于没有加上rowkey的schema信息，这里不添加了
                for (String column : columns) {
                    if(Objects.isNull(r.getValue(family.getBytes(), column.getBytes()))) continue;
                    String value =
                            new String(r.getValue(family.getBytes(), column.getBytes()), "UTF-8");
                    list.add(value);

                }
                System.out.println("+++++++++++++==============++++++++++++++++++");
                System.out.println(list);
                rows.add(RowFactory.create(list.toArray())); //一定要转为数组

                //List<Row> rows = new ArrayList<>();
                //rows.add(RowFactory.create(1L, 3L));
                //rows.add(RowFactory.create(2L, 3L));

            }


            //JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            //JavaRDD<String> parallelize = jsc.parallelize(list);
            //JavaRDD<Row> map = parallelize.map(x -> RowFactory.create(x));


            return rows.iterator();
        });
        //生成新的dataset
        Dataset<Row> rowData = spark.createDataFrame(rowJavaRDD1, StructType.fromDDL(ddlStr));
        if(isShow){
            rowData.show();
        }
        return rowData;
    }


}



//rowJavaRDD.foreachPartition(partition -> {
//    //获得hbase连接
//    Table table = connection.getTable(TableName.valueOf(tableName));
//    List<Get> gets = new ArrayList<>();
//    while(partition.hasNext()){
//        Row next = partition.next();
//        String s = next.getString(1);
//
//        Get get = new Get(Bytes.toBytes(s));
//        gets.add(get);
//
//    }
//    //批量返回
//    Result[] results = table.get(gets);
//    //生成新的dataset
//    //加上condition 返回新的condition
//    Dataset<Row> a = null;
//
//});











        ////join操作
        //rowJavaRDD.foreachPartition(partition -> {
        //        //获得hbase连接
        //        Table table = connection.getTable(TableName.valueOf(tableName));
        //        List<Put> puts = new ArrayList<>();
        //while(partition.hasNext()){
        //Row next = partition.next();
        //String s = next.getString(1);
        //
        //Put put = new Put(Bytes.toBytes(s));
        //puts.add(put);
        //
        //}
        ////批量发送
        //table.put(puts);
        //
        //});