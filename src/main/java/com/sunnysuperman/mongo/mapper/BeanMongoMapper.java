package com.sunnysuperman.mongo.mapper;

import org.bson.Document;

import com.sunnysuperman.commons.bean.ParseBeanOptions;
import com.sunnysuperman.mongo.MongoMapper;
import com.sunnysuperman.repository.serialize.SerializeManager;

public class BeanMongoMapper<T> implements MongoMapper<T> {
    private Class<T> clazz;
    private ParseBeanOptions options;

    public BeanMongoMapper(Class<T> clazz, ParseBeanOptions options) {
        super();
        this.clazz = clazz;
        this.options = options;
    }

    public BeanMongoMapper(Class<T> clazz) {
        super();
        this.clazz = clazz;
    }

    @Override
    public T map(Document doc) {
        return SerializeManager.deserialize(doc, clazz, options);
    }

}