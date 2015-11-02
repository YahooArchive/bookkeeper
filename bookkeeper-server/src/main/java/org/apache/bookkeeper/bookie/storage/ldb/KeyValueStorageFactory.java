package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.IOException;

public interface KeyValueStorageFactory {
    KeyValueStorage newKeyValueStorage(String path) throws IOException;
}
