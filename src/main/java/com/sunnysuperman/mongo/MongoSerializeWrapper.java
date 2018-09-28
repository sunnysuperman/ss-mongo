package com.sunnysuperman.mongo;

import org.bson.Document;

public interface MongoSerializeWrapper<T> {

    Document wrap(Document doc, T bean);
}
