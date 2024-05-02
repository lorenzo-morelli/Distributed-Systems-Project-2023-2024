package it.polimi.common;

import java.io.Serializable;

/**
 * The KeyValuePair class is a record used to represent a key-value pair.
 * It contains the key and the value.
 *
 * @param key   represents the key of the pair.
 * @param value represents the value of the pair.
 */

public record KeyValuePair(Integer key, Integer value) implements Serializable {

    /**
     * The toString method returns the string representation of the key-value pair.
     */
    @Override
    public String toString() {
        return key + "," + value;
    }
}