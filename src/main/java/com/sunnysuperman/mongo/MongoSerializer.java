package com.sunnysuperman.mongo;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.bson.Document;

import com.sunnysuperman.commons.bean.Bean;
import com.sunnysuperman.repository.InsertUpdate;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.serialize.Serializer;

public class MongoSerializer {

    private static boolean isSimpleType(Class<?> type) {
        return (type.isPrimitive() && type != void.class) || type == Double.class || type == Float.class
                || type == Long.class || type == Integer.class || type == Short.class || type == Character.class
                || type == Byte.class || type == Boolean.class || type == String.class;
    }

    private static Object serializeObject(Object value) {
        if (value == null) {
            return null;
        }
        if (isSimpleType(value.getClass())) {
            return value;
        }
        if (value instanceof Date) {
            return ((Date) value).getTime();
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).doubleValue();
        }
        if (value.getClass().isArray() && value.getClass().getComponentType().equals(byte.class)) {
            // byte array (should be blob type)
            return value;
        }
        if (value instanceof Collection) {
            Collection<?> collection = (Collection<?>) value;
            List<Object> items = new ArrayList<>(collection.size());
            for (Object entry : collection) {
                items.add(serializeObject(entry));
            }
            return items;
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            List<Object> items = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                items.add(serializeObject(Array.get(value, i)));
            }
            return items;
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            return serializeMap(map, true);
        }
        return serializeMap(Bean.toMap(value), true);
    }

    public static Document serializeMap(Map<?, ?> map, boolean removeNullFields) throws RepositoryException {
        if (map == null) {
            return null;
        }
        Document doc = new Document();
        for (Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            Object value = entry.getValue();
            value = serializeObject(value);
            if (removeNullFields && value == null) {
                continue;
            }
            doc.put(key, value);
        }
        return doc;
    }

    public static Document serialize(Object bean, Set<String> fields, InsertUpdate insertUpdate,
            boolean removeNullFields) throws RepositoryException {
        Map<String, Object> raw = Serializer.serialize(bean, fields, insertUpdate).getDoc();
        return serializeMap(raw, removeNullFields);
    }

    public static Document serialize(Object bean) throws RepositoryException {
        return serialize(bean, null, InsertUpdate.INSERT, true);
    }
}
