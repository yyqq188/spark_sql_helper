package com.yhl.parser;

import com.alibaba.fastjson.JSONObject;

public abstract class AbstractParser {
    public abstract <T> T parse(String str)  throws Exception;
}
