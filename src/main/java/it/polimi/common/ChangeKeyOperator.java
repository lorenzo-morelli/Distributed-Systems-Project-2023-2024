package common;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ChangeKeyOperator implements Operator {
    private Function<Integer, Integer> keyTransformationFunction;

    public ChangeKeyOperator(Function<Integer, Integer> keyTransformationFunction) {
        this.keyTransformationFunction = keyTransformationFunction;
    }

    @Override
    public List<KeyValuePair> execute(List<KeyValuePair> input) {
        List<KeyValuePair> output = new ArrayList<>();

        for (KeyValuePair pair : input) {
            // Apply the key transformation function to change the key
            int transformedKey = keyTransformationFunction.apply(pair.getValue());
            KeyValuePair transformedPair = new KeyValuePair(transformedKey, pair.getValue());
            output.add(transformedPair);
        }

        return output;
    }
}
