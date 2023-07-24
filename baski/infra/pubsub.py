import json
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub
from google.pubsub_v1 import Schema

from baski.infra import for_yml_file_in_dir, project_id


__all__ = ['load_topics', 'load_schemas' ]


def load_pubsub_schema(data, schema_client):
    project_path = f"projects/{project_id}"

    schema_id = data['name']
    schema_path = schema_client.schema_path(project_id, schema_id)
    schema = Schema(name=schema_path, type_=Schema.Type.AVRO, definition=json.dumps(data))

    try:
        result = schema_client.create_schema(
            request={"parent": project_path, "schema": schema, "schema_id": schema_id}
        )
        print(f"Created a schema {schema_id} using an Avro schema file:\n{result}")
        return result
    except AlreadyExists:
        schema = schema_client.get_schema(
            request={"name": schema_path}
        )
        if json.loads(schema.definition) == data:
            print(f"Schema {schema_id} is up to date")
            return
        result = schema_client.commit_schema(
            request={"schema": schema, "name": schema_path}
        )
        print(f"Updated a schema {schema_id} using an Avro schema file:\n{result}")
        return result


def load_subscription(data, topic_path, subscriber):
    subscription_id = data['name']
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    data['name'] = subscription_path
    data['topic'] = topic_path

    if 'bigquery_config' in data and data['bigquery_config'].get('table'):
        data['bigquery_config']['table'] = data['bigquery_config']['table'].replace('PROJECT_ID', project_id)

    try:
        subscriber.create_subscription(request=data)
        print(f"Created subscription {subscription_path}")
    except AlreadyExists:
        print(f"A subscription {subscription_path} is already exists. Update is not supported")


def load_topic(data, publisher, subscriber, schema_client):
    topic = data['topic']
    topic_path = publisher.topic_path(project_id, topic['name'])
    topic['name'] = topic_path
    if topic.get('schema_settings'):
        schema_id = topic['schema_settings']['schema']
        schema_path = schema_client.schema_path(project_id, schema_id)
        topic['schema_settings']['schema'] = schema_path

    try:
        publisher.create_topic(request=topic)
        print(f"Created a topic {topic}")
    except AlreadyExists:
        print(f"A topic {topic_path} is already exists. Update is not supported")

    for subscription in (data.get('subscriptions') or []):
        load_subscription(subscription, topic_path, subscriber)


def load_topics(root, path):
    publisher = pubsub.PublisherClient()
    subscriber = pubsub.SubscriberClient()
    schema_client = pubsub.SchemaServiceClient()
    for_yml_file_in_dir(root, path, load_topic, publisher, subscriber, schema_client)


def load_schemas(root, path):
    schema_client = pubsub.SchemaServiceClient()
    for_yml_file_in_dir(root, path, load_pubsub_schema, schema_client)
