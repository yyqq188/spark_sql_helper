package com.yhl.transform;

import com.yhl.baks.datax_config.DataxConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractTransform {
    public SparkSession spark;
    public DataxConfig pluginConfig;
    public AbstractTransform(SparkSession spark,DataxConfig pluginConfig){
        this.spark = spark;
        this.pluginConfig = pluginConfig;
    }
    public abstract Dataset<Row> transform(Dataset<Row> dataset)  throws Exception;

}
