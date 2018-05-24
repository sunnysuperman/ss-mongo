package com.sunnysuperman.mongo.mapper;

import org.bson.Document;

import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.mongo.MongoMapper;

public class LongMongoMapper implements MongoMapper<Long> {

    @Override
    public Long map(Document doc) {
        if (doc.isEmpty()) {
            return null;
        }
        Object value = doc.entrySet().iterator().next().getValue();
        return FormatUtil.parseLong(value);
    }

    private static final LongMongoMapper INSTANCE = new LongMongoMapper();

    public static final LongMongoMapper getInstance() {
        return INSTANCE;
    }

}
