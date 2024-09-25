
# Distributed Systems Project 2023-2024

**Politecnico di Milano - Distributed Systems Course**

## Project Overview

This project involves the design and implementation of a fault-tolerant distributed dataflow platform for processing large volumes of key-value pairs, where both keys and values are integers. The platform supports the execution of programs composed of four core operators:

-   **Map** (`map(f: int → int)`): Transforms each tuple `<k, v>` into `<k, f(v)>`.
-   **Filter** (`filter(f: int → boolean)`): Retains tuples `<k, v>` where `f(v)` returns true; otherwise, the tuple is discarded.
-   **ChangeKey** (`changeKey(f: int → int)`): Transforms each tuple `<k, v>` into `<f(v), v>`.
-   **Reduce** (`reduce(f: list<int> → int)`): Aggregates values for each key `k` and outputs a single tuple `<k, f(V)>`, where `V` is the list of values associated with `k`.

The `reduce` operator, when used, always concludes the dataflow.

### Platform Features

-   **Distributed Execution**: The platform consists of a coordinator and several worker nodes, each running on separate machines within a distributed system.
-   **Parallel Processing**: The coordinator handles the submission of arbitrarily long sequences of operators specified as dataflow programs (e.g., in JSON format). These operators are executed in parallel across multiple partitions of input data.
-   **Fault Tolerance**: Fault-tolerance mechanisms ensure minimal re-execution of tasks in the event of worker failure. Workers may fail at any time, but the coordinator is assumed to be reliable.
-   **Data Handling**: Intermediate results are stored in both memory and durable storage. The system assumes reliable network links (TCP), with input data provided in CSV files containing key-value pairs.

### Design Considerations

-   **Architecture**: A processing system is implemented utilizing distributed file systems alongside network channels for efficient data transfer and storage.
- **Scheduling & Pipelining**: The platform employs a batching processing approach, optimizing memory usage while ensuring the durability of intermediate results.
-   **Predefined Functions**: Programs can reference predefined functions by name, such as `ADD(5)`, which increases input values by 5.

### Assumptions

-   Workers can fail at any point, but the coordinator remains operational.
-   Network links and node storage are reliable.
-   Input data is stored in CSV format, with each line representing a key-value pair.
-   Programs utilize predefined functions, such as `ADD(x)`.

### Implementation


The project has been implemented in Java.

----------

## Repository Structure

-   **[Fault Tolerant Dataflow Platform.pdf](https://github.com/lorenzo-morelli/DS_Project/blob/master/Fault%20Tolerant%20Dataflow%20Platform.pdf)**: Project presentation document.
-   **[conf](https://github.com/lorenzo-morelli/DS_Project/tree/master/conf)**: Configuration files for logging (`log4j.properties`).
-   **[docs](https://github.com/lorenzo-morelli/DS_Project/tree/master/docs)**: Generated Javadoc documentation.
-   **[src/main/java/it/polimi](https://github.com/lorenzo-morelli/DS_Project/tree/master/src/main/java/it/polimi)**: Project source code.

## Team

| Name  | Email | GitHub |
| ------------- | ------------- | ------------- |
| Luca Lain  | luca.lain@mail.polimi.it | [@lucalain](https://github.com/lucalain) |
| Lorenzo Morelli | lorenzo1.morelli@mail.polimi.it  | [@lorenzo-morelli](https://github.com/lorenzo-morelli) |
