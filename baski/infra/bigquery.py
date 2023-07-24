import typing
from google.cloud import bigquery
from .filesystem_iterators import for_yml_file_in_dir


def load_dataset(dataset_id):
    print("Load BigQuery dataset")
    cl = bigquery.Client()
    project_id = cl.project

    # Create dataset
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = "us-east4"
    cl.create_dataset(dataset, exists_ok=True)


def load_tables(dataset_id, root, path):
    print("Load BigQuery tables")
    cl = bigquery.Client()
    project_id = cl.project

    tables: typing.List[bigquery.table.TableListItem] = list(cl.list_tables(dataset_id))
    known_table_ids = set([t.table_id for t in tables])

    def load_table(table_data):
        short_table_id = table_data['table_id']
        full_table_id = f"{project_id}.{dataset_id}.{table_data['table_id']}"
        schema = [bigquery.SchemaField(**field) for field in table_data["schema"]]
        if short_table_id in known_table_ids:
            table = cl.get_table(full_table_id)
            table.schema = schema
            cl.update_table(table, ["schema"])
            print(f"Table {short_table_id} schema updated")
        else:
            table = bigquery.Table(full_table_id, schema=schema)
            cl.create_table(table, exists_ok=True)
            print(f"Table {short_table_id} created")

    for_yml_file_in_dir(root, path, load_table)
