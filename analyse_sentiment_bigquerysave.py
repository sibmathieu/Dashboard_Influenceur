from google.cloud import bigquery
from google.cloud import storage

# Set Google Cloud Storage credentials
client = storage.Client.from_service_account_json("cred.json")

def upload_csv_to_bigquery(project_id, dataset_name, table_name, file_path):
    # Instanciation du client BigQuery
    client = bigquery.Client(project=project_id)

    # Référence à l'ensemble de données et à la table cible
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    # Lecture du fichier CSV
    with open(file_path, "rb") as file:
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True

        # Chargement du fichier CSV dans BigQuery
        job = client.load_table_from_file(file, table_ref, job_config=job_config)
        job.result()  # Attente de la fin du chargement

    print(f"Le fichier {file_path} a été chargé dans BigQuery avec succès.")

# Exemple d'utilisation
project_id = "projet-big-data-380518"
dataset_name = "data_set2"
table_name = "score_analysis"
file_path = "intensity_score.csv"

upload_csv_to_bigquery(project_id, dataset_name, table_name, file_path)
