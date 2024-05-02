package it.polimi.worker;


import java.util.function.IntUnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.polimi.worker.operators.ChangeKeyOperator;
import it.polimi.worker.operators.FilterOperator;
import it.polimi.worker.operators.MapOperator;
import it.polimi.worker.operators.ReduceOperator;
import it.polimi.worker.models.Operator;

/**
 * This class is responsible for creating the operators from strings.
 * It uses the factory pattern to create the operators.
 * The operators are created by parsing the function string and creating the corresponding lambda function.
 */
public class CreateOperator {
    /**
     * The createIntUnaryOperator method creates an IntUnaryOperator from a string.
     * The method parses the function string and creates the corresponding lambda function.
     * The function string is in the format "operation(value)".
     * The method uses a regex pattern to parse the function string.
     *
     * @param function which is string representing the function.
     * @return the IntUnaryOperator corresponding to the function.
     */
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

    /**
     * The createMapOperator method creates a MapOperator from a string.
     * This method calls the createIntUnaryOperator method to create the lambda function.
     *
     * @param functionName which is string representing the function.
     * @return the MapOperator corresponding to the function.
     */
    private static MapOperator createMapOperator(String functionName) {
        return new MapOperator(createIntUnaryOperator(functionName));
    }

    /**
     * The createFilterOperator method creates a FilterOperator from a string.
     * The method parses the function string and creates the corresponding lambda function.
     * The function string is in the format "operation(value)".
     * The method uses a regex pattern to parse the function string.
     *
     * @param functionName which is string representing the function.
     * @return the FilterOperator corresponding to the function.
     */
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

    /**
     * The createChangeKeyOperator method creates a ChangeKeyOperator from a string.
     * This method calls the createIntUnaryOperator method to create the lambda function.
     *
     * @param functionName which is string representing the function.
     * @return the ChangeKeyOperator corresponding to the function.
     */
    private static ChangeKeyOperator createChangeKeyOperator(String functionName) {
        return new ChangeKeyOperator(createIntUnaryOperator(functionName));
    }

    /**
     * The createReduceOperator method creates a ReduceOperator from a string.
     * The method parses the function string and creates the corresponding lambda function.
     * The function string is in the format "operation".
     * The method uses a switch statement to create the lambda function based on the function string.
     *
     * @param functionName which is string representing the function.
     * @return the ReduceOperator corresponding to the function.
     */
    private static ReduceOperator createReduceOperator(String functionName) {
        return switch (functionName) {
            case "SUM" -> new ReduceOperator(values -> values.stream().mapToInt(Integer::intValue).sum());
            case "MIN" -> new ReduceOperator(values -> values.stream().min(Integer::compareTo).orElseThrow());
            case "MAX" -> new ReduceOperator(values -> values.stream().max(Integer::compareTo).orElseThrow());
            default -> throw new IllegalArgumentException("Unknown reduce function: " + functionName);
        };
    }

    /**
     * The createOperator method creates an Operator from a string.
     * The method parses the operator and creates the corresponding operator using the factory pattern.
     * The operator is created by parsing the function string and creating the corresponding lambda function.
     *
     * @param operator which is string representing the operator.
     * @param function which is string representing the function.
     * @return the Operator corresponding to the function.
     */
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