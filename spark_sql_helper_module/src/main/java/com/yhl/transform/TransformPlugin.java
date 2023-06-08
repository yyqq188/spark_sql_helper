package com.yhl.transform;

import com.yhl.baks.datax_config.DataxConfig;
import com.yhl.utils.Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class TransformPlugin {
    public SparkSession spark;
    public List<DataxConfig> transformListConfig;
    public TransformPlugin(SparkSession spark,List<DataxConfig> transformListConfig){
        this.spark = spark;
        this.transformListConfig = transformListConfig;
    }
    public Dataset<Row> transform2DS(Dataset<Row> dataset) throws Exception {
        Dataset<Row> newDataset = dataset;
        for(DataxConfig transformConfig:transformListConfig){
            AbstractTransform instance = newInstance(transformConfig);
            newDataset = instance.transform(newDataset);
        }

        return newDataset;
    }


    public AbstractTransform newInstance(DataxConfig pluginConfig) throws Exception {
        String fullName = getFullClazzName(pluginConfig);
        Class<?> clazz = Class.forName(fullName);
        AbstractTransform transform =
                (AbstractTransform) clazz.getConstructor(SparkSession.class, DataxConfig.class)
                .newInstance(spark, pluginConfig);

        return transform;
    }

    public String getFullClazzName(DataxConfig pluginConfig) {
        String name = pluginConfig.getString("name");
        String packageName = "com.yhl.transform";
        return packageName+"."+ Util.LargerFirstChar(name)+"Transform";
    }

}
