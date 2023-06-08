package com.yhl;

import com.alibaba.fastjson.JSONObject;
import com.yhl.baks.datax_config.DataxConfig;
import com.yhl.parser.AbstractParser;
import com.yhl.parser.ParserPlugin;
import com.yhl.reader.AbstractReader;
import com.yhl.reader.ReaderPlugin;
import com.yhl.transform.TransformPlugin;
import com.yhl.writer.AbstractWriter;
import com.yhl.writer.WriterPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;

public class SparkSqlHelperMain {
    //    public static void main(String[] args) throws Exception {
//        run(args);
//    }
    public static void run(String jsonPath) throws Exception {
        //===================这里是解析json配置==================================
        InputStream resourceAsStream = SparkSqlHelperMain.class.getClassLoader()
                .getResourceAsStream(jsonPath);

        //这个SparkFiles需要在spark已经启动的情况才有效，这种场景不适用这里
//        final String filePath = SparkFiles.get("kafka2hbase.json");
//        InputStream resourceAsStream =  new FileInputStream(filePath);


        DataxConfig dataxConfig = DataxConfig.from(resourceAsStream);
        DataxConfig parser = dataxConfig.getConfiguration("job.parser");
        DataxConfig spark = dataxConfig.getConfiguration("job.spark");
        DataxConfig reader = dataxConfig.getConfiguration("job.reader");
        List<DataxConfig> transformerList = dataxConfig.getListConfiguration("job.transformer");
        DataxConfig writer = dataxConfig.getConfiguration("job.writer");


        //校验参数
        if(Objects.isNull(spark)) {
            System.out.println("!!!! spark配置不能为空");
            return;
        }
        if(Objects.isNull(parser)) {
            System.out.println("!!!! parser配置不能为空");
            return;
        }
        if(Objects.isNull(reader)) {
            System.out.println("!!!! reader配置不能为空");
            return;
        }
        if(Objects.isNull(transformerList)) {
            System.out.println("!!!! transform配置不能为空");
        }
        if(Objects.isNull(writer)) {
            System.out.println("!!!! writer配置不能为空");
        }

        //============================================解析生成ssc============================================
        JSONObject sparkJsonStr = JSONObject.parseObject(spark.getConfiguration("parameter").toJSON());
        SparkConf conf = new SparkConf();
        String appName = spark.getString("appName");
        Integer seconds = spark.getInt("seconds");
        String logLevel = spark.getString("logLevel");
        //未解析master
        conf.setAppName(appName);
        for(String key:sparkJsonStr.keySet()){
            conf.set(key,sparkJsonStr.get(key).toString());
        }

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel(logLevel);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(seconds));



        //============================================source============================================
        //避免 ssc 序列化
        ReaderPlugin readerPlugin = new ReaderPlugin();
        AbstractReader abstractReader = readerPlugin.newInstance(reader);
        //todo 这里后期加上print
        JavaDStream<String> directStream = abstractReader.createSource(ssc,reader);


        //parser的解析
        ParserPlugin parserPlugin = new ParserPlugin();
        AbstractParser abstractParser = parserPlugin.newInstance(parser);

        directStream.foreachRDD(rdd ->{

            //为了能活用，对下面的写法再进行精简
            SparkSession spark2 = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
//            Dataset<Row> dataset = spark2.createDataFrame(rdd.map(Util::parseT_PERM_ARAP),T_PERM_ARAP_SOURCE.class);
            Dataset<Row> dataset = spark2.createDataFrame(rdd.map(abstractParser::parse),
                    Class.forName(parser.getString("parameter.pojoClassPath")));



            //============================================transform============================================
            TransformPlugin transformPlugin = new TransformPlugin(spark2, transformerList);
            Dataset<Row> rowDataset = transformPlugin.transform2DS(dataset);


            //todo 这里后期加上判断 if(Objects.isNull(writer)) {}


            rowDataset.foreachPartition(partitionOfRecords -> {
                //============================================sink============================================
                WriterPlugin writerPlugin = new WriterPlugin();
                AbstractWriter writer1 = writerPlugin.newInstance(writer);
                writer1.writer(partitionOfRecords,writer);
            });

        });
        ssc.start();
        ssc.awaitTermination();

    }



}
