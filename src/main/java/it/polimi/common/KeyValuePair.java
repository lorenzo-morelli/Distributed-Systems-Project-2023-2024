package it.polimi.common;

// Representation of a key-value pair
public class KeyValuePair {
    Integer key;
    Integer value;

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