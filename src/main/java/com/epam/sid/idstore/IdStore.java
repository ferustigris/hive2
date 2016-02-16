package com.epam.sid.idstore;

/**
 * Interface for id storage
 */
public interface IdStore {
    /**
     * Returns the next available key for particular sequence
     * @param sequenceName - sequence name
     * @return key
     */
    long getNextId(String sequenceName);

    /**
     * Closes storage or connection to one
     */
    void close();
}
