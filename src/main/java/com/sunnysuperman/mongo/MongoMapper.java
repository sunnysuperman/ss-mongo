package com.sunnysuperman.mongo;

import org.bson.Document;

public interface MongoMapper<T> {

    T map(Document doc);

}