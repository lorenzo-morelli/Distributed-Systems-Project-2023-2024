package common;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class FilterOperator implements Operator {
    private Predicate<Integer> filterFunction;

    public FilterOperator(Predicate<Integer> filterFunction) {
        this.filterFunction = filterFunction;
    }

    @Override
    public List<KeyValuePair> execute(List<KeyValuePair> input) {
        List<KeyValuePair> output = new ArrayList<>();

        for (KeyValuePair pair : input) {
            // Apply the filter function to the value
            if (filterFunction.test(pair.getValue())) {
                output.add(pair);
            }
        }

        return output;
    }
}
