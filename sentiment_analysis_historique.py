
import io
import pandas as pd
from google.cloud import storage, bigquery
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import pyarrow 
import csv

def hello_world(request):
  storage_client = storage.Client()

  bucket_name = "commentaires_analysis"
  file_name = "historique/commentaires_nettoyes.csv"

  # Read file from bucket as pandas dataframe
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(file_name)
  data = blob.download_as_string()
  df = pd.read_csv(io.BytesIO(data))
  print(f"Loaded {len(df)} rows from {file_name}")

  # Set BigQuery dataset and table names
  project_id = "projet-big-data-380518"
  dataset_id = "data_set2"
  table_id = f"{project_id}.{dataset_id}.comments_analysis"

  ############################################
  tokenizer = AutoTokenizer.from_pretrained("flaubert/flaubert_small_cased")
  model = AutoModelForSequenceClassification.from_pretrained("nlptown/flaubert_small_cased_sentiment")
  classification_pipeline = pipeline(
      task="sentiment-analysis",
      model=model,
      tokenizer=tokenizer 
    )
  # Analyse de sentiment pour chaque commentaire
  sentiments = []
  sentiment_scores = []
  intensity_scores = []
  for comment in df['comments']:
      result = classification_pipeline(comment)
      sentiments.append(result[0]['label'])
      sentiment_scores.append(result[0]['score'])
      intensity_scores.append(result[0]['score'] * 2 - 1)  # Normalisation du score pour obtenir une plage de -1 à 1
      

  # Ajout des colonnes 'sentiment' et 'score' au dataframe
  df['sentiment'] = sentiments
  df['score'] = sentiment_scores
  df['intensity_score'] = intensity_scores
    
  # Insert results into BigQuery table
  bq_client = bigquery.Client()
  table_ref = bq_client.dataset(dataset_id).table('comments_analysis')
  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
  load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
  load_job.result()
  print(f"{len(df)} rows inserted into BigQuery table {table_id}")
  ############################################

  # Convert dataframe to CSV
  csv_file_path = "/tmp/comments_analysis_scores.csv"
  df.to_csv(csv_file_path, index=False, encoding='utf-8')

  # Write updated file to bucket
  target_folder = "historique/comments_analysis_scores.csv"
  updated_blob = bucket.blob(target_folder)
  updated_blob.upload_from_filename(csv_file_path)
  print(f"comments_analysis_scores.csv saved to {target_folder}")

  
  return "All Good"