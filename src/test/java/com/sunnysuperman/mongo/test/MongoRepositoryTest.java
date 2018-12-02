package com.sunnysuperman.mongo.test;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.mongo.MongoRepository;
import com.sunnysuperman.mongo.MongoSerializeWrapper;
import com.sunnysuperman.mongo.mapper.RawMongoMapper;
import com.sunnysuperman.repository.serialize.IdGenerator;
import com.sunnysuperman.repository.serialize.SerializeBean;
import com.sunnysuperman.repository.serialize.SerializeId;
import com.sunnysuperman.repository.serialize.SerializeProperty;
import com.sunnysuperman.repository.serialize.Serializer;

import junit.framework.TestCase;

public class MongoRepositoryTest extends TestCase {

    @SerializeBean(value = "test_device", camel2underline = false)
    public static class Device {
        @SerializeId(generator = IdGenerator.PROVIDE)
        @SerializeProperty(column = "_id")
        private String id;

        @SerializeProperty(updatable = false)
        private Long createdAt;

        @SerializeProperty
        private String name;

        @SerializeProperty
        private String notes;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Long getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(Long createdAt) {
            this.createdAt = createdAt;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getNotes() {
            return notes;
        }

        public void setNotes(String notes) {
            this.notes = notes;
        }

    }

    @SerializeBean(value = "test_device2", camel2underline = false)
    public static class Device2 {
        @SerializeId(generator = IdGenerator.PROVIDE)
        @SerializeProperty(column = "_id")
        private ObjectId id;

        @SerializeProperty(updatable = false)
        private Long createdAt;

        @SerializeProperty
        private String name;

        @SerializeProperty
        private String notes;

        public ObjectId getId() {
            return id;
        }

        public void setId(ObjectId id) {
            this.id = id;
        }

        public Long getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(Long createdAt) {
            this.createdAt = createdAt;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getNotes() {
            return notes;
        }

        public void setNotes(String notes) {
            this.notes = notes;
        }

    }

    private static MongoRepository repository;
    static {
        MongoClient client = new MongoClient("127.0.0.1", 29000);
        repository = new MongoRepository(client, "test");
        repository.setTraceLog(true);
        try {
            Serializer.scan(MongoRepositoryTest.class.getPackage().getName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void test_insert_update() {
        String id = "1000";
        Long createdAt = 123L;
        repository.remove("test_device", MongoRepository.getIdDocument(id));

        // insert 1
        {
            String name = "Device name on insert 1";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            device.setCreatedAt(createdAt);
            repository.insert(device);
            Document saved = repository.find("test_device", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getLong("createdAt").equals(createdAt));
        }

        repository.remove("test_device", MongoRepository.getIdDocument(id));

        // insert 2
        {
            final String name = "Device name on insert 2";
            Device device = new Device();
            device.setId(id);
            device.setCreatedAt(createdAt);
            repository.insert(device, new MongoSerializeWrapper<Device>() {

                @Override
                public Document wrap(Document doc, Device bean) {
                    doc.put("name", name);
                    return doc;
                }

            });
            Document saved = repository.find("test_device", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getLong("createdAt").equals(createdAt));
        }

        {
            String name = "Device name on update 1";
            String notes = "notes on update 1";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            device.setNotes(notes);
            repository.update(device);
            Document saved = repository.find("test_device", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getString("notes").equals(notes));
        }

        {
            String name = "Device name on update 2";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            repository.update(device);
            Document saved = repository.find("test_device", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getString("notes") == null);
        }

        {
            String name = "Device name on update";
            final String name2 = "Device name in wrap";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            repository.update(device, null, new MongoSerializeWrapper<Device>() {

                @Override
                public Document wrap(Document doc, Device bean) {
                    doc.put("name", name2);
                    return doc;
                }

            });
            Document saved = repository.find("test_device", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getString("name").equals(name2));
        }
    }

    public void test_insert_update2() {
        Long createdAt = 123L;
        repository.removeMany("test_device2", new Document());

        ObjectId id;
        // insert 1
        {
            String name = "Device name on insert 1";
            Device2 device = new Device2();
            device.setName(name);
            device.setCreatedAt(createdAt);
            repository.insert(device);

            Document saved = repository.find("test_device2", new Document(), RawMongoMapper.getInstance());
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getLong("createdAt").equals(createdAt));
            id = new ObjectId(FormatUtil.parseString(saved.get(MongoRepository.ID)));
        }

        {
            String name = "Device name on update 1";
            String notes = "notes on update 1";
            Device2 device = new Device2();
            device.setId(id);
            device.setName(name);
            device.setNotes(notes);
            repository.update(device);
            Document saved = repository.find("test_device2", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getString("notes").equals(notes));
        }

        {
            String name = "Device name on update 2";
            Device2 device = new Device2();
            device.setId(id);
            device.setName(name);
            repository.update(device);
            Document saved = repository.find("test_device2", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getString("notes") == null);
        }
    }

    public void test_save() {
        String id = "1001";
        Long createdAt = 123L;
        repository.remove("test_device", MongoRepository.getIdDocument(id));

        {
            String name = "name on save";
            String notes = "notes on save";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            device.setNotes(notes);
            device.setCreatedAt(createdAt);
            repository.save(device);
            Document saved = repository.find("test_device", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getLong("createdAt").equals(createdAt));
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getString("notes").equals(notes));

        }

        {
            String name = "name on save 2";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            repository.save(device);
            Document saved = repository.find("test_device", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getLong("createdAt").equals(createdAt));
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getString("notes") == null);
        }

        {
            final String name = "name on save 3";
            final String notes = "notes on save 3";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            device.setNotes(notes);
            repository.save(device, new MongoSerializeWrapper<Device>() {

                @Override
                public Document wrap(Document doc, Device bean) {
                    doc.put("name", name);
                    doc.put("notes", notes);
                    return doc;
                }

            });
            Document saved = repository.find("test_device", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getLong("createdAt").equals(createdAt));
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getString("notes").equals(notes));
        }
    }

    public void test_save2() {
        Long createdAt = 123L;
        ObjectId id;
        repository.removeMany("test_device2", new Document());

        // insert
        {
            String name = "name on save 1";
            Device2 device = new Device2();
            device.setName(name);
            device.setCreatedAt(createdAt);
            repository.save(device);

            Document saved = repository.find("test_device2", new Document(), RawMongoMapper.getInstance());
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getLong("createdAt").equals(createdAt));
            id = new ObjectId(FormatUtil.parseString(saved.get(MongoRepository.ID)));
        }

        {
            String name = "name on save 2";
            Device2 device = new Device2();
            device.setId(id);
            device.setName(name);
            repository.save(device);
            Document saved = repository.find("test_device2", MongoRepository.getIdDocument(id),
                    RawMongoMapper.getInstance());
            assertTrue(saved.getLong("createdAt").equals(createdAt));
            assertTrue(saved.getString("name").equals(name));
            assertTrue(saved.getString("notes") == null);
        }
    }

}
