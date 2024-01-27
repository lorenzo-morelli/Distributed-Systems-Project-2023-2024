package it.polimi.common;

import java.util.List;

// Operator interface
public interface Operator{
    List<KeyValuePair> execute(List<KeyValuePair> input);
}