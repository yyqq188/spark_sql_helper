package com.yhl.writer;

import com.yhl.baks.datax_config.DataxConfig;
import org.apache.spark.sql.Row;

import java.util.Iterator;

public abstract class AbstractWriter{
    public abstract void writer(Iterator<Row> iterator,DataxConfig pluginConfig)  throws Exception;
}
