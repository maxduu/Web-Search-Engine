package edu.upenn.cis.cis455.storage;

public class StorageFactory {
    public static StorageInterface getDatabaseInstance(String directory) {
        return new BerkeleyStorage(directory);
    }
}
