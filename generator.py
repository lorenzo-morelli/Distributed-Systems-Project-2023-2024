import csv
import random
import json
import pandas as pd

keys = 0
PART_GEN_PATH = "files/program3/key"
RESULT_GEN_PATH = "files/program3/result.csv"
RESULT_REAL_PATH = "result-3.csv"
OPERATIONS_PATH = "files/program3/program.json"


def generate_data():
    global keys
    keys = int(input("Enter the number of keys: "))
    values = int(input("Enter the number of values per keys: "))
    max = int(input("Enter the max value for the values: "))
    for i in range(1, keys + 1):
        with open(f"{PART_GEN_PATH}{i}.csv", "w", newline='') as file:
            writer = csv.writer(file)
            for _ in range(1, values + 1):
                key = i
                value = random.randint(1, max)
                if key and value:
                    writer.writerow([key, value])


def filter_data(data, function):
    function_name, parameter = parse_function(function)

    if function_name == "IS_EVEN":
        return data[data['value'] % 2 == 0]
    elif function_name == "IS_ODD":
        return data[data['value'] % 2 != 0]
    elif function_name == "LT":
        return data[data['value'] < int(parameter)]
    elif function_name == "GT":
        return data[data['value'] > int(parameter)]
    elif function_name == "GTE":
        return data[data['value'] >= int(parameter)]
    elif function_name == "LTE":
        return data[data['value'] <= int(parameter)]
    else:
        raise ValueError(f"Unsupported filter function: {function_name}")


def parse_function(function_str):
    parts = function_str.split('(')
    if len(parts) == 2 and parts[1].endswith(')'):
        function_name = parts[0]
        parameter = parts[1][:-1]
        return function_name, parameter
    else:
        return function_str, None


def map_data(data, function):
    function_name, parameter = parse_function(function)

    if function_name == "ADD":
        return pd.DataFrame({'key': data['key'], 'value': (data['value'] + int(parameter)).astype(int)})
    elif function_name == "MULTIPLY":
        return pd.DataFrame({'key': data['key'], 'value': (data['value'] * int(parameter)).astype(int)})
    elif function_name == "SUBTRACT":
        return pd.DataFrame({'key': data['key'], 'value': (data['value'] - int(parameter)).astype(int)})
    elif function_name == "DIVIDE":
        return pd.DataFrame({'key': data['key'], 'value': (data['value'] / int(parameter)).astype(int)})
    else:
        raise ValueError(f"Unsupported map function: {function_name}")


def change_key_data(data, function):
    function_name, parameter = parse_function(function)

    if function_name == "ADD":
        return pd.DataFrame({'key': (data['value'] + int(parameter)).astype(int), 'value': data['value']})
    elif function_name == "MULTIPLY":
        return pd.DataFrame({'key': (data['value'] * int(parameter)).astype(int), 'value': data['value']})
    elif function_name == "SUBTRACT":
        return pd.DataFrame({'key': (data['value'] - int(parameter)).astype(int), 'value': data['value']})
    elif function_name == "DIVIDE":
        return pd.DataFrame({'key': (data['value'] / int(parameter)).astype(int), 'value': data['value']})
    else:
        raise ValueError(f"Unsupported change_key function: {function_name}")


def reduce_data(data, function):
    function_name, _ = parse_function(function)

    if function_name == "SUM":
        result = data.groupby('key')['value'].sum().reset_index()
        result.columns = ['key', 'value']
        return result
    elif function_name == "PRODUCT":
        result = data.groupby('key')['value'].prod().reset_index()
        result.columns = ['key', 'value']
        return result
    else:
        raise ValueError(f"Unsupported reduce function: {function_name}")


def apply_operation(data, operation):
    operator = operation["operator"]
    function = operation["function"]

    if operator == "FILTER":
        return filter_data(data, function)
    elif operator == "MAP":
        return map_data(data, function)
    elif operator == "CHANGEKEY":
        return change_key_data(data, function)
    elif operator == "REDUCE":
        return reduce_data(data, function)
    else:
        raise ValueError(f"Unsupported operator: {operator}")


def operations():
    new_partitions = input(f"Specify the number of partitions (default set to: {keys}): ")
    new_partitions = int(new_partitions) if new_partitions else keys
    if new_partitions == 0:
        print("No partitions to process.")
        return
    with open(OPERATIONS_PATH) as config_file:
        config = json.load(config_file)

    merged_data = pd.concat(
        [pd.read_csv(f"{PART_GEN_PATH}{i}.csv", header=None, names=['key', 'value']) for i in
         range(1, new_partitions + 1)],
        ignore_index=True)

    for operation in config["operations"]:
        if operation["operator"] == "FILTER":
            merged_data = filter_data(merged_data, operation["function"])
        elif operation["operator"] == "MAP":
            merged_data = map_data(merged_data, operation["function"])
        elif operation["operator"] == "CHANGEKEY":
            merged_data = change_key_data(merged_data, operation["function"])
        elif operation["operator"] == "REDUCE":
            merged_data = reduce_data(merged_data, operation["function"])
        else:
            raise ValueError(f"Unsupported operator: {operation['operator']}")

    merged_data['value'] = merged_data['value'].astype(int)

    # Save the result to a new file
    merged_data.to_csv(RESULT_GEN_PATH, index=False, header=False)


def validate_data():
    try:
        gen = pd.read_csv(RESULT_GEN_PATH,header=None, names=['key', 'value'])
        real = pd.read_csv(RESULT_REAL_PATH,header=None, names=['key', 'value'])
    except FileNotFoundError:
        print("Data not available. Please generate data and apply operations first.")
        return
    
    gen = gen.sort_values(by=['key','value']).reset_index(drop=True)
    real = real.sort_values(by=['key','value']).reset_index(drop=True)
    if (gen.equals(real)):
        print("Congrats, data is correct!")
    else:
        print("Sorry, data is incorrect")
    

def welcome():
    print("Please select an option:")
    print("1. Generate data")
    print("2. Apply operations")
    print("3. Validate data")
    print("4. Exit")
    choice = input("> ")
    match choice:
        case '1':
            generate_data()
        case '2':
            operations()
        case '3':
            validate_data()
        case '4':
            print("Goodbye!")
            exit()
        case _:
            print("Invalid choice. Please try again.")
    welcome()


print("Welcome to our Fault Tolerant Dataflow Platform!")
welcome()
