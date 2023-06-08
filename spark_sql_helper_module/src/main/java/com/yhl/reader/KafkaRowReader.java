package com.yhl.reader;

import com.alibaba.fastjson.JSONObject;
import com.yhl.baks.datax_config.DataxConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.Serializable;
import java.util.*;

public class KafkaRowReader extends AbstractReader implements Serializable {
    //public KafkaRowReader( DataxConfig pluginConfig) {
    //    super( pluginConfig);
    //}


    @Override
    public JavaDStream<String> createSource(JavaStreamingContext ssc,DataxConfig pluginConfig) throws Exception {

        //解析生成kafka source
        List<String> topics = pluginConfig.getList("topic", String.class);
        DataxConfig parameter = pluginConfig.getConfiguration("parameter");
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        String parameterJsonStr = parameter.toJSON();
        JSONObject parse = JSONObject.parseObject(parameterJsonStr);
        for(String key:parse.keySet()){
            kafkaParams.put(key,parse.get(key));
        }
        //有个别的参数不能通过json文件传入，在这里配置
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());


        JavaInputDStream<ConsumerRecord<String, String>> directStream =
                KafkaUtils.createDirectStream(ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));


        //JavaDStream<String> javaDStream = directStream.map(
        //        new Function<ConsumerRecord<String, String>, String>() {
        //    @Override
        //    public String call(ConsumerRecord<String, String> record) throws Exception {
        //        System.out.println("=============");
        //        System.out.println(record.value());
        //        T_PERM_ARAP_SOURCE t_perm_arap_source = Util.parseT_PERM_ARAP(record.value());
        //
        //        return JSON.toJSONString(t_perm_arap_source);
        //    }
        //});
        JavaDStream<String> javaDStream = directStream.mapPartitions(
                new FlatMapFunction<Iterator<ConsumerRecord<String, String>>, String>() {
            @Override
            public Iterator<String> call(Iterator<ConsumerRecord<String, String>> consumerRecordIterator) throws Exception {
                return  new Iterator<String>() {
                    @Override
                    public boolean hasNext() {
                        return consumerRecordIterator.hasNext();
                    }

                    @Override
                    public String next() {
                        return consumerRecordIterator.next().value();
                    }
                };

            }
        });


        return javaDStream;
    }





}
