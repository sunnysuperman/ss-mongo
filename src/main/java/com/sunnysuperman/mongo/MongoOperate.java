package com.sunnysuperman.mongo;

import com.mongodb.client.MongoDatabase;

public interface MongoOperate<T> {

    T execute(MongoDatabase database);

}