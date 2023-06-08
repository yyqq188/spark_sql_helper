package com.yhl.transform;

import com.yhl.baks.datax_config.DataxConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FilterMultiTransform extends AbstractTransform{

    public FilterMultiTransform(SparkSession spark, DataxConfig pluginConfig) {
        super(spark, pluginConfig);
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> dataset) throws Exception {


        return null;

    }

}
