package com.sunnysuperman.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.sunnysuperman.commons.model.Pagination;
import com.sunnysuperman.commons.model.PullPagination;
import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.InsertUpdate;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.serialize.SerializeDoc;
import com.sunnysuperman.repository.serialize.Serializer;

public class MongoRepository {
    public static final String ID = "_id";
    protected Logger logger = LoggerFactory.getLogger(MongoRepository.class);
    protected boolean traceLog;
    protected MongoClient client;
    protected String db;

    public MongoRepository() {
        super();
    }

    public MongoRepository(MongoClient client, String db) {
        super();
        this.client = client;
        this.db = db;
    }

    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public boolean isTraceLog() {
        return traceLog;
    }

    public void setTraceLog(boolean traceLog) {
        this.traceLog = traceLog;
    }

    public MongoClient getClient() {
        return client;
    }

    public void setClient(MongoClient client) {
        this.client = client;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public static Document getIdDocument(Object id) {
        return new Document(ID, id);
    }

    private void closeCursor(MongoCursor<?> cursor) {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception ex) {
                logger.error(null, ex);
            }
        }
    }

    protected void trace(MongoClient client, long t1, Object... params) {
        if (traceLog && logger.isInfoEnabled()) {
            long t2 = System.nanoTime();
            long take = TimeUnit.NANOSECONDS.toMillis(t2 - t1);
            for (int i = 0; i < params.length; i++) {
                Object param = params[i];
                if (param == null) {
                    params[i] = "null";
                }
            }
            logger.info("[Mongo] " + StringUtil.join(params, " ") + ", take: " + take + "ms");
        }
    }

    public <T> T execute(MongoOperate<T> op) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            return op.execute(database);
        } finally {
            if (traceLog) {
                trace(client, t1, "execute");
            }
        }
    }

    public <T> boolean save(T bean, String collectionName, Set<String> fields, InsertUpdate insertUpdate,
            MongoSerializeWrapper<T> wrapper, boolean removeNullFields) {
        SerializeDoc sdoc = Serializer.serialize(bean, fields, insertUpdate);
        if (collectionName == null) {
            collectionName = sdoc.getTableName();
        }
        Map<String, Object> raw = sdoc.getDoc();
        Document doc = MongoSerializer.serializeMap(raw, removeNullFields);
        if (wrapper != null) {
            doc = wrapper.wrap(doc, bean);
        }
        // insert only
        if (insertUpdate == InsertUpdate.INSERT || sdoc.getIdValues() == null) {
            insert(collectionName, doc);
            return true;
        }
        // update
        Document update = new Document("$set", doc);
        if (removeNullFields) {
            // unset fields
            Document unset = new Document();
            for (String key : raw.keySet()) {
                if (doc.containsKey(key)) {
                    continue;
                }
                unset.append(key, StringUtil.EMPTY);
            }
            if (!unset.isEmpty()) {
                update.append("$unset", unset);
            }
        }
        if (sdoc.getIdValues() == null) {
            throw new RepositoryException("Require id to update");
        }
        boolean updated = updateById(collectionName, update, sdoc.getIdValues()[0]);
        if (updated || insertUpdate == InsertUpdate.UPDATE) {
            return updated;
        }
        // upsert
        Document insert = MongoSerializer.serializeMap(sdoc.getUpsertDoc(), removeNullFields);
        if (wrapper != null) {
            insert = wrapper.wrap(insert, bean);
        }
        insert(collectionName, insert);
        return true;
    }

    public <T> boolean save(T bean, MongoSerializeWrapper<T> wrapper) {
        return save(bean, null, null, InsertUpdate.UPSERT, wrapper, true);
    }

    public <T> boolean save(T bean) {
        return save(bean, null, null, InsertUpdate.UPSERT, null, true);
    }

    public <T> void insert(T bean, MongoSerializeWrapper<T> wrapper) {
        save(bean, null, null, InsertUpdate.INSERT, wrapper, true);
    }

    public <T> void insert(T bean) {
        save(bean, null, null, InsertUpdate.INSERT, null, true);
    }

    public <T> boolean update(T bean, MongoSerializeWrapper<T> wrapper) {
        return save(bean, null, null, InsertUpdate.UPDATE, wrapper, true);
    }

    public <T> boolean update(T bean) {
        return save(bean, null, null, InsertUpdate.UPDATE, null, true);
    }

    public <T> boolean update(T bean, Set<String> fields, MongoSerializeWrapper<T> wrapper) {
        return save(bean, null, fields, InsertUpdate.UPDATE, wrapper, true);
    }

    public <T> boolean update(T bean, Set<String> fields) {
        return save(bean, null, fields, InsertUpdate.UPDATE, null, true);
    }

    public void insert(String collectionName, Document doc) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            collection.insertOne(doc);
        } finally {
            if (traceLog) {
                trace(client, t1, "insert:" + collectionName, doc);
            }
        }
    }

    public void insertMany(String collectionName, List<Document> docs) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            collection.insertMany(docs);
        } finally {
            if (traceLog) {
                trace(client, t1, "insertMany:" + collectionName, docs);
            }
        }
    }

    public boolean update(String collectionName, Document update, Document filter) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.updateOne(filter, update).getModifiedCount() > 0;
        } finally {
            if (traceLog) {
                trace(client, t1, "update:" + collectionName, "filter:", filter, "update:", update);
            }
        }
    }

    public boolean updateById(String collectionName, Document update, Object id) {
        Document filter = getIdDocument(id);
        return update(collectionName, update, filter);
    }

    public long updateMany(String collectionName, Document filter, Document update, UpdateOptions options) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.updateMany(filter, update, options).getModifiedCount();
        } finally {
            if (traceLog) {
                trace(client, t1, "updateMany:" + collectionName, "filter:", filter, "update:", update);
            }
        }
    }

    public MongoSaveResult upsert(String collectionName, Document upsert, Object id) {
        long t1 = traceLog ? System.nanoTime() : 0;
        Document filter = getIdDocument(id);
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            boolean updated = collection.updateOne(filter, upsert, new UpdateOptions().upsert(true))
                    .getMatchedCount() > 0;
            return updated ? MongoSaveResult.UPDATED : MongoSaveResult.INSERTED;
        } finally {
            if (traceLog) {
                trace(client, t1, "upsert:" + collectionName, "filter:", filter, "upsert:", upsert);
            }
        }
    }

    public MongoSaveResult save(String collectionName, Document doc) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            Object id = doc.get(ID);
            if (id == null) {
                collection.insertOne(doc);
                return MongoSaveResult.INSERTED;
            }
            Document update = new Document();
            update.put("$set", doc);
            boolean updated = collection.updateOne(getIdDocument(id), update, new UpdateOptions().upsert(true))
                    .getMatchedCount() > 0;
            return updated ? MongoSaveResult.UPDATED : MongoSaveResult.INSERTED;
        } finally {
            if (traceLog) {
                trace(client, t1, "save:" + collectionName, doc);
            }
        }
    }

    public boolean remove(String collectionName, Bson filter) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.deleteOne(filter).getDeletedCount() > 0;
        } finally {
            if (traceLog) {
                trace(client, t1, "remove:" + collectionName, "filter:", filter);
            }
        }
    }

    public long removeMany(String collectionName, Bson filter) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.deleteMany(filter).getDeletedCount();
        } finally {
            if (traceLog) {
                trace(client, t1, "removeMany:" + collectionName, "filter:", filter);
            }
        }
    }

    public BulkWriteResult batch(String collectionName, List<WriteModel<Document>> requests) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.bulkWrite(requests);
        } finally {
            if (traceLog) {
                trace(client, t1, "batch:" + collectionName, "requests:", requests);
            }
        }
    }

    public <T> T find(String collectionName, Bson filter, MongoMapper<T> mapper) {
        return find(collectionName, filter, null, null, mapper);
    }

    public <T> T find(String collectionName, Bson filter, Bson sort, Bson fields, MongoMapper<T> mapper) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        MongoCursor<Document> cursor = null;
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            FindIterable<Document> iter = collection.find(filter);
            if (sort != null) {
                iter.sort(sort);
            }
            if (fields != null) {
                iter.projection(fields);
            }
            cursor = iter.limit(1).iterator();
            if (cursor.hasNext()) {
                return mapper.map(cursor.next());
            }
            return null;
        } finally {
            closeCursor(cursor);
            if (traceLog) {
                trace(client, t1, "find:" + collectionName, "filter:", filter, "sort:", sort, "fields:", fields);
            }
        }
    }

    public Document findAndUpdate(String collectionName, Bson filter, Bson update, FindOneAndUpdateOptions options) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.findOneAndUpdate(filter, update, options);
        } finally {
            if (traceLog) {
                trace(client, t1, "findAndUpdate:" + collectionName, "filter:", filter, "update:", update);
            }
        }
    }

    public Document findAndRemove(String collectionName, Bson filter, FindOneAndDeleteOptions options) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.findOneAndDelete(filter, options);
        } finally {
            if (traceLog) {
                trace(client, t1, "findAndRemove:" + collectionName, "filter:", filter);
            }
        }
    }

    public <T> T findById(String collectionName, Object id, MongoMapper<T> mapper) {
        return find(collectionName, getIdDocument(id), mapper);
    }

    public <T> List<T> findForList(String collectionName, Bson filter, Bson sort, Bson fields, int offset, int limit,
            MongoMapper<T> mapper) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        MongoCursor<Document> cursor = null;
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            FindIterable<Document> iter = collection.find(filter);
            if (sort != null) {
                iter.sort(sort);
            }
            if (offset > 0) {
                iter.skip(offset);
            }
            if (limit > 0) {
                iter.limit(limit);
            }
            if (fields != null) {
                iter.projection(fields);
            }
            cursor = iter.iterator();
            List<T> items = new ArrayList<>(limit);
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                T item = mapper.map(doc);
                if (item != null) {
                    items.add(item);
                }
            }
            return items;
        } finally {
            closeCursor(cursor);
            if (traceLog) {
                trace(client, t1, "findForList:" + collectionName, "filter:", filter, "sort:", sort, "fields:", fields);
            }
        }
    }

    public <T> PullPagination<T> findForPullPagination(String collectionName, Bson filter, Bson sort, Bson fields,
            String marker, int limit, MongoMapper<T> mapper) {
        int offset = StringUtil.isEmpty(marker) ? 0 : Integer.parseInt(marker);
        List<T> items = findForList(collectionName, filter, sort, fields, offset, limit + 1, mapper);
        if (items.isEmpty()) {
            return PullPagination.emptyInstance();
        }
        boolean hasMore = items.size() > limit;
        if (hasMore) {
            List<T> trimItems = new ArrayList<T>(limit);
            int size = 0;
            for (T item : items) {
                trimItems.add(item);
                size++;
                if (size == limit) {
                    break;
                }
            }
            items = trimItems;
        }
        int newOffset = offset + limit;
        return PullPagination.newInstance(items, String.valueOf(newOffset), hasMore);
    }

    public <T> Pagination<T> findForPagination(String collectionName, Bson filter, Bson sort, Bson fields, int offset,
            int limit, MongoMapper<T> mapper) {
        List<T> items = findForList(collectionName, filter, sort, fields, offset, limit, mapper);
        int size = items.size();
        if (size == 0) {
            return Pagination.emptyInstance(limit);
        }
        if (offset != 0 || size == limit) {
            long sizeLong = count(collectionName, filter);
            size = sizeLong > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) sizeLong;
        }
        return new Pagination<T>(items, size, offset, limit);
    }

    public long count(String collectionName, Bson filter) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.count(filter);
        } finally {
            if (traceLog) {
                trace(client, t1, "count:" + collectionName, "filter:", filter);
            }
        }
    }

    public <T> void aggregate(String collectionName, List<? extends Bson> pipeline, List<T> docs,
            MongoMapper<T> mapper) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        MongoCursor<Document> cursor = null;
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            AggregateIterable<Document> iter = collection.aggregate(pipeline);
            iter.useCursor(true).allowDiskUse(true).batchSize(300);
            cursor = iter.iterator();
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                docs.add(mapper.map(doc));
            }
        } finally {
            closeCursor(cursor);
            if (traceLog) {
                trace(client, t1, "aggregate:" + collectionName, "pipeline:", pipeline);
            }
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }

}
