package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.IOException;

import org.apache.bookkeeper.conf.ServerConfiguration;

public interface KeyValueStorageFactory {
    enum DbConfigType {
        Small, // Used for ledgers db, doesn't need particular configuration
        Huge   // Used for location index, lots of writes and much bigger dataset
    }

    KeyValueStorage newKeyValueStorage(String path, DbConfigType dbConfigType, ServerConfiguration conf) throws IOException;
}
