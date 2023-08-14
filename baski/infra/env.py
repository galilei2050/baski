from google.cloud import firestore

project_id = firestore.Client().project

confirmed = False

if not confirmed:
    result = input(f"Is project {project_id} correct? (y/N): ")
    if result.lower() not in ('y', 'yes'):
        print("Aborting")
        exit(1)
