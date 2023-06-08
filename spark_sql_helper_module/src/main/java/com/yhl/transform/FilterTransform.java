package com.yhl.transform;

import com.yhl.baks.datax_config.DataxConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Objects;

public class FilterTransform extends AbstractTransform{

    public FilterTransform(SparkSession spark, DataxConfig pluginConfig) {
        super(spark, pluginConfig);
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> dataset) throws Exception {
        String field = pluginConfig.getString("parameter.field");
        List<String> paras = pluginConfig.getList("parameter.paras", String.class);
        Boolean isShow = Objects.isNull(pluginConfig.getBool("parameter.isShow"))?
                false:pluginConfig.getBool("parameter.isShow");
        Dataset<Row> filter = dataset.filter(String.join(" ", field, paras.get(0), paras.get(1)));
        if(isShow){
            filter.show();
        }
        return filter;

    }

}
