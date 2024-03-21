package it.polimi.common;

import java.io.Serializable;

public record KeyValuePair(Integer key, Integer value) implements Serializable {

    @Override
    public String toString() {
        return key + "," + value;
    }
}