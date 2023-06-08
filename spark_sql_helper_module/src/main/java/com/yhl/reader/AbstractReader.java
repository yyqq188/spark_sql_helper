package com.yhl.reader;

import com.yhl.baks.datax_config.DataxConfig;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public abstract class AbstractReader{
    //public DataxConfig pluginConfig;
    //public AbstractReader(DataxConfig pluginConfig){
    //    this.pluginConfig = pluginConfig;
    //}
    public abstract JavaDStream<String> createSource(JavaStreamingContext ssc,DataxConfig pluginConfig)  throws Exception;

}
