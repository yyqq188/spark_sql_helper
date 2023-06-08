package com.yhl.writer;

import com.yhl.baks.datax_config.DataxConfig;
import com.yhl.utils.Util;

public class WriterPlugin {


    public AbstractWriter newInstance(DataxConfig pluginConfig) throws Exception {
        String fullName = getFullClazzName(pluginConfig);
        Class<?> clazz = Class.forName(fullName);
        AbstractWriter writer =
                (AbstractWriter) clazz.getConstructor().newInstance();

        return writer;
    }

    private String getFullClazzName(DataxConfig pluginConfig) {
        String name = pluginConfig.getString("name");
        String packageName = "com.yhl.writer";
        return packageName+"."+ Util.LargerFirstChar(name);
    }
}
