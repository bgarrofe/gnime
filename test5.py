import diskcache

from gnime.nodes.node import output_table, input_table
from gnime.pipeline import Pipeline
from gnime.stage import NodeStage


@output_table(name="features")
class Load(NodeStage):

    @staticmethod
    def download_data():
        data = {
            'data': [1, 2, 3, 4, 5]
        }
        features = data['data']
        return features

    def run(self):
        features = self.download_data()
        return features


@input_table(name="features")
@output_table(name="transformed_features")
class Transform(NodeStage):

    def run(self, features):

        transformed_features = [f * 2 for f in features]
        return transformed_features


@input_table(name="transformed_features")
class Display(NodeStage):

    def run(self, transformed_features):
        print(transformed_features)


def build_pipeline():

    disk_cache = diskcache.Cache('/tmp/disk_cache')
    load = Load(cache=disk_cache)
    transform = Transform(cache=disk_cache).after(load)
    display = Display(cache=disk_cache).after(transform)

    pipeline = Pipeline()
    pipeline.add_stages([load, transform, display])

    return pipeline


p = build_pipeline()
p.start()
