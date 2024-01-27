package it.polimi.coordinator;

import it.polimi.common.ChangeKeyOperator;
import it.polimi.common.FilterOperator;
import it.polimi.common.MapOperator;
import it.polimi.common.Operator;
import it.polimi.common.ReduceOperator;

public class CreateOperator {

    private static MapOperator createMapOperator(String functionName) {
        switch (functionName) {
            case "ADD":
                return new MapOperator(x -> x + 5); // Example: Add 5 to the input value
            case "MULTIPLY":
                return new MapOperator(x -> x * 5);
            default:
                throw new IllegalArgumentException("Unknown map function: " + functionName);
        }
       
    }

    private static FilterOperator createFilterOperator(String functionName) {
        switch (functionName) {
            case "IS_EVEN":
                return new FilterOperator(value -> value % 2 == 0);
            case "IS_ODD":
                return new FilterOperator(value -> value % 2 != 0);
            default:
                throw new IllegalArgumentException("Unknown filter function: " + functionName);
        }
    }

    private static ChangeKeyOperator createChangeKeyOperator(String functionName) {

        switch (functionName) {
            case "DOUBLE_KEY":
                return new ChangeKeyOperator(key -> key * 2);

            case "INCREMENT_KEY":
                return new ChangeKeyOperator(key -> key + 1);

            default:
                throw new IllegalArgumentException("Unknown key transformation function: " + functionName);
        }
    }

    private static ReduceOperator createReduceOperator(String functionName) {
        // Implement logic to map function name to a reduce function
        switch (functionName) {
            case "SUM":
                return new ReduceOperator(values -> values.stream().mapToInt(Integer::intValue).sum());

            case "PRODUCT":
                return new ReduceOperator(values -> values.stream().reduce(1, (a, b) -> a * b));
                
            default:
                throw new IllegalArgumentException("Unknown reduce function: " + functionName);
        }
    }

    public static Operator createOperator(String operator, String function){
        switch (operator) {
            case "map":
                // Instantiate and return MapOperator based on the function name
                return CreateOperator.createMapOperator(function);

            case "filter":
                // Instantiate and return FilterOperator based on the predicate name
                return CreateOperator.createFilterOperator(function);

            case "changeKey":
                // Instantiate and return ChangeKeyOperator based on the function name
                return CreateOperator.createChangeKeyOperator(function);

            case "reduce":
                // Instantiate and return ReduceOperator based on the function name
                return CreateOperator.createReduceOperator(function);

            default:
                throw new IllegalArgumentException("Unknown operator type: " + operator);
        }
    }
}