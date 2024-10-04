
from gnime.runner import Runner
from gnime.nodes.io import CSVReaderNode, CSVWriterNode

if __name__ == '__main__':

    runner = Runner()

    runner.add(
        CSVReaderNode(node_id=1),
        context={'file_path': 'data.csv'}
    )

    runner.add(
        CSVWriterNode(node_id=2),
        context={'file_path': 'data2.csv'}
    )

    inputs = {'input': None}

    result = runner.run(
        'node_2_output', **inputs)

    print(result)
