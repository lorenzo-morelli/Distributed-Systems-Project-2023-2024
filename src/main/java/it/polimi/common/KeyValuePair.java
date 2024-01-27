package it.polimi.common;

import java.io.Serializable;

// Representation of a key-value pair
public class KeyValuePair implements Serializable{
    private Integer key;
    private Integer value;

    public KeyValuePair(Integer key, Integer value) {
        this.key = key;
        this.value = value;
    }
    public Integer getValue(){
        return value;
    }
    public Integer getKey(){
        return key;
    }
    // Override toString() for a more meaningful output
    @Override
    public String toString() {
        return "(" + key + ", " + value + ")";
    }
}