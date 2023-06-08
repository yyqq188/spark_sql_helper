package com.yhl.reader;

import com.yhl.baks.datax_config.DataxConfig;
import com.yhl.utils.Util;

public class ReaderPlugin {

    public AbstractReader newInstance(DataxConfig pluginConfig) throws Exception {
        String fullName = getFullClazzName(pluginConfig);
        Class<?> clazz = Class.forName(fullName);
        AbstractReader transform =
                (AbstractReader) clazz.getConstructor()
                        .newInstance();

        return transform;
    }

    private String getFullClazzName(DataxConfig pluginConfig) {
        String name = pluginConfig.getString("name");
        String packageName = "com.yhl.reader";
        return packageName+"."+ Util.LargerFirstChar(name);
    }
}
