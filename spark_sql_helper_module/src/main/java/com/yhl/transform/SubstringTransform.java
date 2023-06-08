package com.yhl.transform;

import com.yhl.baks.datax_config.DataxConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Objects;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.substring;

public class SubstringTransform extends AbstractTransform{

    public SubstringTransform(SparkSession spark, DataxConfig pluginConfig) {
        super(spark, pluginConfig);
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> dataset) throws Exception {

        String field = pluginConfig.getString("parameter.field");
        List<Integer> paras = pluginConfig.getList("parameter.paras",Integer.class);
        String target = pluginConfig.getString("parameter.target");
        Boolean isShow = Objects.isNull(pluginConfig.getBool("parameter.isShow"))?
                false:pluginConfig.getBool("parameter.isShow");        final Dataset<Row> rowDataset = dataset.withColumn(target, substring(col(field),
                paras.get(0), paras.get(1)));
        if(isShow){
            rowDataset.show();
        }

        return rowDataset;

    }


}
