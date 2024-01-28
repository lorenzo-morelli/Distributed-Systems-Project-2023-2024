package it.polimi.worker;


import java.io.Serializable;
import java.util.function.IntUnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.polimi.common.Operator;
import it.polimi.common.operators.ChangeKeyOperator;
import it.polimi.common.operators.FilterOperator;
import it.polimi.common.operators.MapOperator;
import it.polimi.common.operators.ReduceOperator;

public class CreateOperator implements Serializable{


    private static IntUnaryOperator createIntUnaryOperator(String function) {
        Pattern FUNCTION_PATTERN = Pattern.compile("(\\w+)\\((\\d+)\\)");
        Matcher matcher = FUNCTION_PATTERN.matcher(function);

        if (matcher.matches()) {
            String operation = matcher.group(1);
            int value = Integer.parseInt(matcher.group(2));

            switch (operation) {
                case "ADD":
                    return new IntUnaryOperator() {
                        @Override
                        public int applyAsInt(int x) {
                            return x + value;
                        }
                    };                
                case "MULTIPLY":
                    return new IntUnaryOperator() {
                        @Override
                        public int applyAsInt(int x) {
                            return x * value;
                        }
                    };                
                case "SUBTRACT":
                    return new IntUnaryOperator() {
                        @Override
                        public int applyAsInt(int x) {
                            return x - value;
                        }
                    };                
                case "DIVIDE":
                    if (value != 0) {
                        return new IntUnaryOperator() {
                            @Override
                            public int applyAsInt(int x) {
                                return x / value;
                            }
                        };
                    } else {
                        throw new IllegalArgumentException("Cannot divide by zero");
                    }
                default:
                    throw new IllegalArgumentException("Unknown map function: " + function);
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
            
            switch (operation) {
                case "IS_EVEN":
                    return new FilterOperator(x -> x % 2 == 0);
                case "IS_ODD":
                    return new FilterOperator(x -> x % 2 != 0);
                case "LT":
                    return new FilterOperator(x -> x < Integer.parseInt(matcher.group(2)));
                case "GT":
                    return new FilterOperator(x -> x > Integer.parseInt(matcher.group(2)));
                case "GTE":
                    return new FilterOperator(x -> x >= Integer.parseInt(matcher.group(2)));
                case "LTE":
                    return new FilterOperator(x -> x <= Integer.parseInt(matcher.group(2)));
                default:
                    throw new IllegalArgumentException("Unknown filter function: " + functionName);
            }
        } else {
            throw new IllegalArgumentException("Invalid filter function format: " + functionName);
        }
    }

    private static ChangeKeyOperator createChangeKeyOperator(String functionName) {
        return new ChangeKeyOperator(createIntUnaryOperator(functionName));
    }

    private static ReduceOperator createReduceOperator(String functionName) {
        switch (functionName) {
            case "SUM":
                return new ReduceOperator(values -> values.stream().mapToInt(Integer::intValue).sum());

            case "PRODUCT":
                return new ReduceOperator(values -> values.stream().reduce(1, (a, b) -> a * b));

            case "MIN":
                return new ReduceOperator(values -> values.stream().min(Integer::compareTo).orElseThrow());

            case "MAX":
                return new ReduceOperator(values -> values.stream().max(Integer::compareTo).orElseThrow());
            
            case "COUNT":
                return new ReduceOperator(values -> values.size());
            default:
                throw new IllegalArgumentException("Unknown reduce function: " + functionName);
        }
    }

    public static Operator createOperator(String operator, String function){
        switch (operator) {
            case "MAP":
                // Instantiate and return MapOperator based on the function name
                return CreateOperator.createMapOperator(function);

            case "FILTER":
                // Instantiate and return FilterOperator based on the predicate name
                return CreateOperator.createFilterOperator(function);

            case "CHANGEKEY":
                // Instantiate and return ChangeKeyOperator based on the function name
                return CreateOperator.createChangeKeyOperator(function);

            case "REDUCE":
                // Instantiate and return ReduceOperator based on the function name
                return CreateOperator.createReduceOperator(function);

            default:
                throw new IllegalArgumentException("Unknown operator type: " + operator);
        }
    }
}