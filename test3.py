from pipefunc import Pipeline, PipeFunc

from gnime.core import Node
from gnime.nodes.io.csvreader.node import CSVReaderNode
from gnime.nodes.io.csvwriter.node import CSVWriterNode

node1 = CSVReaderNode('node_1', config={'file_path': 'data.csv'})
node2 = CSVWriterNode('node_2', config={'file_path': 'data2.csv'})


def safe_list_get(l, idx, default=None):
    try:
        return l[idx]
    except IndexError:
        return default


class Connection:

    connection_dict = {}

    def add_connection(self, from_node: Node, to_node: Node, from_port: int, to_port: int):

        from_node_outputs = from_node.get_outputs()
        to_node_inputs = to_node.get_inputs()

        input_name = safe_list_get(to_node_inputs, to_port)
        output_name = safe_list_get(from_node_outputs, from_port)

        self.connection_dict[to_node.name] = {
            input_name: output_name
        }

    def get_renames_for_node(self, node_id):
        if node_id in self.connection_dict:
            return self.connection_dict[node_id]
        return {}


conn = Connection()
conn.add_connection(node1, node2, 0, 0)


# connections = [
#     {
#         "from_node": "node_1",
#         "to_node": "node_2",
#         "from_port": "0",
#         "to_port": "0"
#     }
# ]

# connection_dict = {
#     "node_1": {},
#     "node_2": {
#         "input": "node_1_output_0"
#     }
# }


fa = PipeFunc(
    node1.execute,
    output_name=node1.output_name,
    renames=conn.get_renames_for_node(node1.name)
)
fb = PipeFunc(
    node2.execute,
    output_name=node2.output_name,
    renames=conn.get_renames_for_node(node2.name)
)

pipeline = Pipeline([fa, fb], profile=True)
print(pipeline())
