package com.yhl.parser;

import com.yhl.baks.datax_config.DataxConfig;
import com.yhl.utils.Util;

public class ParserPlugin {

    public AbstractParser newInstance(DataxConfig pluginConfig) throws Exception {
        String fullName = getFullClazzName(pluginConfig);
        Class<?> clazz = Class.forName(fullName);
        AbstractParser parser =
                (AbstractParser) clazz.getConstructor()
                .newInstance();
        return parser;
    }

    public String getFullClazzName(DataxConfig pluginConfig) {
        String name = pluginConfig.getString("name");
        String packageName = "com.yhl.parser";
        return packageName+"."+ Util.LargerFirstChar(name)+"Parser";
    }

}
