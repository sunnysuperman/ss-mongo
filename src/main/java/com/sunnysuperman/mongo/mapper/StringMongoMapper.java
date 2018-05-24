package com.sunnysuperman.mongo.mapper;

import org.bson.Document;

import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.mongo.MongoMapper;

public class StringMongoMapper implements MongoMapper<String> {

    @Override
    public String map(Document doc) {
        if (doc.isEmpty()) {
            return null;
        }
        Object value = doc.entrySet().iterator().next().getValue();
        return FormatUtil.parseString(value);
    }

    private static final StringMongoMapper INSTANCE = new StringMongoMapper();

    public static final StringMongoMapper getInstance() {
        return INSTANCE;
    }

}
