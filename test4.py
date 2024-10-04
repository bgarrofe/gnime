from collections import defaultdict, deque


class Connection:
    def __init__(self, source_node, source_output, target_node, target_input):
        self.source_node = source_node      # Node producing the output
        # Output method of the source node (e.g., load_data)
        self.source_output = source_output
        self.target_node = target_node      # Node consuming the input
        # Input method of the target node (e.g., apply_filter)
        self.target_input = target_input

    def transfer(self, data=None):
        # Call the source node's output method. It may or may not require input data.
        if data is None:
            # If no input data is provided (i.e., it's a source node), call the method directly.
            output_data = getattr(self.source_node, self.source_output)()
        else:
            # If input data is provided, pass it to the source node's output method.
            output_data = getattr(self.source_node, self.source_output)(data)

        # Transfer the output data to the target node's input method.
        # getattr(self.target_node, self.target_input)(output_data)
        return output_data

# Example base class for a Node


class Node:
    def __init__(self, name):
        self.name = name

    def process(self):
        pass

# Example source node: CSV Reader


class CSVReader(Node):
    def __init__(self, filepath):
        super().__init__("CSVReader")
        self.filepath = filepath

    def load_data(self):
        print(f"Loading data from {self.filepath}")
        # In reality, this would read and return CSV data
        return [{"name": "Alice"}, {"name": "Bob"}]

# Example transform node: Data filter


class FilterData(Node):
    def __init__(self):
        super().__init__("FilterData")

    def apply_filter(self, data):
        print("Filtering data")
        # Example: Filter out data (filtering logic here)
        return [item for item in data if item["name"] != "Bob"]


class AddData(Node):
    def __init__(self):
        super().__init__("AddData")

    def add_data(self, data):
        print("Adding data")
        # Example: Filter out data (filtering logic here)
        return data + [{"name": "Charlie"}]

# Example sink node: Save to Database


class SaveToDatabase(Node):
    def __init__(self):
        super().__init__("SaveToDatabase")

    def save_data(self, data):
        print(f"Saving data to database: {data}")


class Workflow:
    def __init__(self):
        self.connections = []  # List of all connections between nodes
        # Adjacency list to track the graph
        self.adjacency_list = defaultdict(list)
        # Tracks how many inputs each node has
        self.in_degree = defaultdict(int)
        self.nodes = set()  # Set to track all nodes

    def add_connection(self, connection):
        self.connections.append(connection)
        self.adjacency_list[connection.source_node].append(
            connection.target_node)
        self.in_degree[connection.target_node] += 1
        self.nodes.add(connection.source_node)
        self.nodes.add(connection.target_node)

    def topological_sort(self):
        # Queue for nodes with no dependencies (in-degree of 0)
        queue = deque(
            [node for node in self.nodes if self.in_degree[node] == 0])
        execution_order = []

        while queue:
            node = queue.popleft()
            execution_order.append(node)

            # For each node that depends on the current node
            for neighbor in self.adjacency_list[node]:
                self.in_degree[neighbor] -= 1
                if self.in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(execution_order) == len(self.nodes):
            return execution_order  # Return sorted nodes in topological order
        else:
            raise ValueError(
                "There is a cycle in the workflow, which is not allowed.")

    def run(self):
        # Perform topological sort to determine execution order
        execution_order = self.topological_sort()

        print(execution_order)

        print("Running the workflow in topological order...")
        node_outputs = {}  # Dictionary to store output data from each node

        # Execute nodes in the determined order
        for node in execution_order:
            # Find all connections where this node is a source
            for connection in self.connections:
                if connection.source_node == node:
                    # Pass output data from source node to target node
                    # Get previously produced data if any
                    input_data = node_outputs.get(node, None)
                    output_data = connection.transfer(data=input_data)

                    # Store the output for the target node
                    node_outputs[connection.target_node] = output_data

        print("Workflow completed.")


# Example usage with the previously defined classes
csv_reader = CSVReader("data.csv")
filter_data = FilterData()
add_data = AddData()
save_db = SaveToDatabase()

# Create a workflow
workflow = Workflow()

# Create and add connections to the workflow
connection_1 = Connection(csv_reader, 'load_data', filter_data, 'apply_filter')
connection_2 = Connection(filter_data, 'apply_filter', add_data, 'add_data')
connection_3 = Connection(add_data, 'add_data', save_db, 'save_data')

workflow.add_connection(connection_1)
workflow.add_connection(connection_2)
workflow.add_connection(connection_3)

# Run the workflow
workflow.run()
