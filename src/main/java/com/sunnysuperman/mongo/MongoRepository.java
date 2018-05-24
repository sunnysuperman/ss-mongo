package com.sunnysuperman.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.UpdateOptions;
import com.sunnysuperman.commons.model.PullPagination;

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

    protected void trace(MongoClient client, String action, long t1) {
        if (traceLog && logger.isInfoEnabled()) {
            long t2 = System.nanoTime();
            long take = TimeUnit.NANOSECONDS.toMillis(t2 - t1);
            logger.info("[Mongo] " + action + " take: " + take + "ms");
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
                trace(client, "execute", t1);
            }
        }
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
                trace(client, "insert", t1);
            }
        }
    }

    public boolean update(String collectionName, Document doc, Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Require id");
        }
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.updateOne(getIdDocument(id), doc).getModifiedCount() > 0;
        } finally {
            if (traceLog) {
                trace(client, "update", t1);
            }
        }
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
                trace(client, "updateMany", t1);
            }
        }
    }

    public MongoSaveResult upsert(String collectionName, Document doc, Object id) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            boolean updated = collection.updateOne(getIdDocument(id), doc, new UpdateOptions().upsert(true))
                    .getMatchedCount() > 0;
            return updated ? MongoSaveResult.UPDATED : MongoSaveResult.INSERTED;
        } finally {
            if (traceLog) {
                trace(client, "upsert", t1);
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
                trace(client, "save", t1);
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
                trace(client, "find", t1);
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
                trace(client, "findAndUpdate", t1);
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
                trace(client, "findAndRemove", t1);
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
                items.add(mapper.map(doc));
            }
            return items;
        } finally {
            closeCursor(cursor);
            if (traceLog) {
                trace(client, "findForList", t1);
            }
        }
    }

    public <T> PullPagination<T> findForPullPagination(String collectionName, Bson filter, Bson sort, Bson fields,
            String marker, int limit, MongoMapper<T> mapper) {
        int offset = StringUtils.isEmpty(marker) ? 0 : Integer.parseInt(marker);
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

    public long count(String collectionName, Bson filter) {
        long t1 = traceLog ? System.nanoTime() : 0;
        MongoClient client = getClient();
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.count(filter);
        } finally {
            if (traceLog) {
                trace(client, "count", t1);
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
                trace(client, "aggregate", t1);
            }
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }

}
