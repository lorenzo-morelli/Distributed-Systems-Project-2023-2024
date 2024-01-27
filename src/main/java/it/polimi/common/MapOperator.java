package common;

import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

// Map operator implementation

public class MapOperator implements Operator {
    private final IntUnaryOperator function;

    public MapOperator(IntUnaryOperator function) {
        this.function = function;
    }

    @Override
    public List<KeyValuePair> execute(List<KeyValuePair> input) {
        // Implementation of the map operator
        return input.stream()
                .map(kv -> new KeyValuePair(kv.key,function.applyAsInt(kv.value)))
                .collect(Collectors.toList());
    }
}