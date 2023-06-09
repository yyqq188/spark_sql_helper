package com.yhl.transform;

import com.yhl.baks.datax_config.DataxConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Objects;

/**
 * {
 *           "name": "sql",
 *           "parameter": {
 *             "tableName": "T",
 *             "sqlStr": "select * from T"
 *           }
 *         }
 */
public class SqlTransform extends AbstractTransform{
    public SqlTransform(SparkSession spark, DataxConfig pluginConfig) {
        super(spark, pluginConfig);
    }
    @Override
    public Dataset<Row> transform(Dataset<Row> dataset) throws Exception {
        String tableName = pluginConfig.getString("parameter.tableName");
        String sqlStr = pluginConfig.getString("parameter.sqlStr");
        Boolean isShow = Objects.isNull(pluginConfig.getString("isShow"))?
                false:(pluginConfig.getString("isShow").equals("false"))?false:true;
//        System.out.println("********************************");
//        System.out.println(sqlStr);
        dataset.createOrReplaceTempView(tableName);
        final Dataset<Row> sql = spark.sql(sqlStr);
        if(isShow){
            sql.show();
        }
        return sql;
    }
}
