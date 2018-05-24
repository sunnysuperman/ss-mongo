package com.sunnysuperman.mongo.mapper;

import org.bson.Document;

import com.sunnysuperman.mongo.MongoMapper;

public class RawMongoMapper implements MongoMapper<Document> {

    @Override
    public Document map(Document doc) {
        return doc;
    }

    private static final RawMongoMapper INSTANCE = new RawMongoMapper();

    public static final RawMongoMapper getInstance() {
        return INSTANCE;
    }

}