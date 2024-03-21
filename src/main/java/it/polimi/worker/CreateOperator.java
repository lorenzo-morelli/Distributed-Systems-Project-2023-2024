package it.polimi.worker;


import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.polimi.worker.operators.ChangeKeyOperator;
import it.polimi.worker.operators.FilterOperator;
import it.polimi.worker.operators.MapOperator;
import it.polimi.worker.operators.ReduceOperator;
import it.polimi.worker.models.Operator;

public class CreateOperator {


    private static IntUnaryOperator createIntUnaryOperator(String function) {
        Pattern FUNCTION_PATTERN = Pattern.compile("(\\w+)\\((\\d+)\\)");
        Matcher matcher = FUNCTION_PATTERN.matcher(function);

        if (matcher.matches()) {
            String operation = matcher.group(1);
            int value = Integer.parseInt(matcher.group(2));

            switch (operation) {
                case "ADD":
                    return x -> x + value;
                case "MULTIPLY":
                    return x -> x * value;
                case "SUBTRACT":
                    return x -> x - value;
                case "DIVIDE":
                    if (value != 0) {
                        return x -> x / value;
                    } else {
                        throw new IllegalArgumentException("Cannot divide by zero");
                    }
                default:
                    throw new IllegalArgumentException("Unknown function: " + function);
            }
        } else {
            throw new IllegalArgumentException("Invalid function format: " + function);
        }
    }

    private static MapOperator createMapOperator(String functionName) {
        return new MapOperator(createIntUnaryOperator(functionName));
    }

    private static FilterOperator createFilterOperator(String functionName) {
        Pattern FUNCTION_PATTERN = Pattern.compile("(\\w+)(?:\\((\\d+)\\))?");
        Matcher matcher = FUNCTION_PATTERN.matcher(functionName);

        if (matcher.matches()) {
            String operation = matcher.group(1);

            return switch (operation) {
                case "IS_EVEN" -> new FilterOperator(x -> x % 2 == 0);
                case "IS_ODD" -> new FilterOperator(x -> x % 2 != 0);
                case "LT" -> new FilterOperator(x -> x < Integer.parseInt(matcher.group(2)));
                case "GT" -> new FilterOperator(x -> x > Integer.parseInt(matcher.group(2)));
                case "GTE" -> new FilterOperator(x -> x >= Integer.parseInt(matcher.group(2)));
                case "LTE" -> new FilterOperator(x -> x <= Integer.parseInt(matcher.group(2)));
                default -> throw new IllegalArgumentException("Unknown filter function: " + functionName);
            };
        } else {
            throw new IllegalArgumentException("Invalid filter function format: " + functionName);
        }
    }

    private static ChangeKeyOperator createChangeKeyOperator(String functionName) {
        return new ChangeKeyOperator(createIntUnaryOperator(functionName));
    }

    private static ReduceOperator createReduceOperator(String functionName) {
        return switch (functionName) {
            case "SUM" -> new ReduceOperator(values -> values.stream().mapToInt(Integer::intValue).sum());
            case "PRODUCT" -> new ReduceOperator(values -> values.stream().reduce(1, (a, b) -> a * b));
            case "MIN" -> new ReduceOperator(values -> values.stream().min(Integer::compareTo).orElseThrow());
            case "MAX" -> new ReduceOperator(values -> values.stream().max(Integer::compareTo).orElseThrow());
            case "COUNT" -> new ReduceOperator(List::size);
            default -> throw new IllegalArgumentException("Unknown reduce function: " + functionName);
        };
    }

    public static Operator createOperator(String operator, String function) {
        return switch (operator) {
            case "MAP" -> CreateOperator.createMapOperator(function);
            case "FILTER" -> CreateOperator.createFilterOperator(function);
            case "CHANGEKEY" -> CreateOperator.createChangeKeyOperator(function);
            case "REDUCE" -> CreateOperator.createReduceOperator(function);
            default -> throw new IllegalArgumentException("Unknown operator type: " + operator);
        };
    }
}