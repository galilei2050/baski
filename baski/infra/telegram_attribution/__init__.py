from .. import pubsub, bigquery


def setup(dataset_id):
    bigquery.load_dataset(dataset_id)
    bigquery.load_tables(dataset_id, __file__, "bigquery")
    pubsub.load_schemas(root=__file__, path='pubsub/schema')
    pubsub.load_topics(root=__file__, path='pubsub/topic')
