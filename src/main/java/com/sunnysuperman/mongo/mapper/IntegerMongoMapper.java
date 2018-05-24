package com.sunnysuperman.mongo.mapper;

import org.bson.Document;

import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.mongo.MongoMapper;

public class IntegerMongoMapper implements MongoMapper<Integer> {

    @Override
    public Integer map(Document doc) {
        if (doc.isEmpty()) {
            return null;
        }
        Object value = doc.entrySet().iterator().next().getValue();
        return FormatUtil.parseInteger(value);
    }

    private static final IntegerMongoMapper INSTANCE = new IntegerMongoMapper();

    public static final IntegerMongoMapper getInstance() {
        return INSTANCE;
    }

}
