package com.kafka.restbackend.repository;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@Repository
public class RocksDbRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbRepository.class);

    @Value("${rocksDb.dir}")
    private String rocksDbDir;

    @Value("${rocksDb.name}")
    private String rocksDbName;

    File dbDir;

    RocksDB db;

    @Bean
    void initialize() {
        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);
        dbDir = new File(rocksDbDir, rocksDbName);
        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
            db = RocksDB.open(options, dbDir.getAbsolutePath());
        } catch(IOException | RocksDBException ex) {
            LOGGER.error("Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
                    ex.getCause(), ex.getMessage(), ex.getStackTrace());
        }
        LOGGER.info("RocksDB initialized and ready to use");
    }

    public synchronized void save(String key, String value) {
        try {
            db.put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            LOGGER.error("Error saving entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
        }
    }

    public synchronized String find(String key) {
        String result = null;
        try {
            byte[] bytes = db.get(key.getBytes());
            if(bytes == null) return null;
            result = new String(bytes);
        } catch (RocksDBException e) {
            LOGGER.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", key, e.getCause(), e.getMessage());
        }
        return result;
    }

    public synchronized void delete(String key) {
        try {
            db.delete(key.getBytes());
        } catch (RocksDBException e) {
            LOGGER.error("Error deleting entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
        }
    }
}