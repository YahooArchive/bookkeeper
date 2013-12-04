package org.apache.bookkeeper.bookie.storage;

import java.nio.ByteBuffer;

public class LedgerData {

    private boolean exists;
    private boolean isFenced;
    private byte[] masterKey;

    public LedgerData(boolean exists, boolean isFenced, byte[] masterKey) {
        this.exists = exists;
        this.isFenced = isFenced;
        this.masterKey = masterKey;
    }

    public boolean exists() {
        return exists;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    public boolean isFenced() {
        return isFenced;
    }

    public void setFenced(boolean isFenced) {
        this.isFenced = isFenced;
    }

    public byte[] getMasterKey() {
        return masterKey;
    }

    public void setMasterKey(byte[] masterKey) {
        this.masterKey = masterKey;
    }

    public static LedgerData fromArray(byte[] array) {
        ByteBuffer buf = ByteBuffer.wrap(array);
        boolean exists = buf.get() == 1 ? true : false;
        boolean isFenced = buf.get() == 1 ? true : false;
        byte[] masterKey = new byte[buf.remaining()];
        buf.get(masterKey);
        return new LedgerData(exists, isFenced, masterKey);
    }

    byte[] toArray() {
        ByteBuffer buf = ByteBuffer.allocate(2 + masterKey.length);
        buf.put(exists ? (byte) 1 : 0);
        buf.put(isFenced ? (byte) 1 : 0);
        buf.put(masterKey);
        buf.flip();
        return buf.array();
    }
}
