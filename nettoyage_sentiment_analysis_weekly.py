import pandas as pd
import numpy as np
import re
from unidecode import unidecode
import csv
import io
from google.cloud import storage, bigquery
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import pyarrow 
from datetime import datetime, timedelta


def hello_world(request,context):
  storage_client = storage.Client()
  bucket_name = "commentaires_analysis"
  lastweek = (datetime.now().date() - timedelta(days=7)).strftime("%Y-%m-%d")
  today = datetime.now().date().strftime("%Y-%m-%d")
  file_name = f"weekly/{lastweek}_{today}/comments.csv"

  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(file_name)

  # Read file from bucket as pandas dataframe
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(file_name)
  data = blob.download_as_string()
  df = pd.read_csv(io.BytesIO(data))
  print(f"Loaded {len(df)} rows from {file_name}")

  # Set BigQuery dataset and table names
  project_id = "projet-big-data-380518"
  dataset_id = "data_set2"
  table_id = f"{project_id}.{dataset_id}.comments_analysis2"

  def clean_comments(comment):
      # Conversion en chaîne de caractères
      comment = str(comment)
      # Filtrage des commentaires vides
      if comment.strip():
          # Filtrage des commentaires qui ne contiennent que des chiffres
          if not comment.isdigit():
              # Conversion en minuscules
              comment = comment.lower()
              # Suppression des URLs
              comment = re.sub(r'http\S+', '', comment)
              # Suppression des balises HTML
              comment = re.sub(r'<.*?>', '', comment)
              # Suppression des caractères spéciaux sauf les apostrophes
              comment = re.sub(r"[^a-zA-Z0-9àáâãäåçèéêëìíîïñòóôõöøùúûüýÿžÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÑÒÓÔÕÖØÙÚÛÜÝŸŽ' ]+", '', comment)
              # Suppression des accents
              comment = unidecode(comment)
      return comment

  # Nettoyer les commentaires
  cleaned_comments = []
  video_ids = []
  video_titles = []
  comment_dates = []
  for video_id, video_title, comment, date in zip(df['video_id'], df['video_title'], df['comments'], df['date']):
      cleaned_comment = clean_comments(comment)
      if isinstance(cleaned_comment, str):
          cleaned_comments.append(cleaned_comment)
          video_ids.append(video_id)
          video_titles.append(video_title)
          comment_dates.append(date)

  # Créer un nouveau DataFrame avec les commentaires nettoyés
  cleaned_df = pd.DataFrame({'video_id': video_ids, 'video_title': video_titles, 'comments': cleaned_comments, 'date':comment_dates})

  # Supprimer les lignes qui contiennent uniquement des chiffres
  cleaned_df = cleaned_df[~cleaned_df["comments"].str.isnumeric()]

  # Supprimer les lignes avec des commentaires vides
  cleaned_df = cleaned_df[~cleaned_df["comments"].str.strip().eq("")]


  ###########ANALYSE SENTIMENT : Machine learning #####################
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
  for comment in cleaned_df['comments']:
      result = classification_pipeline(comment)
      sentiments.append(result[0]['label'])
      sentiment_scores.append(result[0]['score'])
      intensity_scores.append(result[0]['score'] * 2 - 1)  # Normalisation du score pour obtenir une plage de -1 à 1
      
  # Ajout des colonnes 'sentiment' et 'score' au dataframe
  cleaned_df['sentiment'] = sentiments
  cleaned_df['score'] = sentiment_scores
  cleaned_df['intensity_score'] = intensity_scores
    
  # Insert results into BigQuery table
  bq_client = bigquery.Client()
  table_ref = bq_client.dataset(dataset_id).table('comments_analysis2')
  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
  load_job = bq_client.load_table_from_dataframe(cleaned_df, table_ref, job_config=job_config)
  load_job.result()
  print(f"{len(cleaned_df)} rows inserted into BigQuery table {table_id}")
  
  ############################################

  # Convert dataframe to CSV
  csv_file_path = "/tmp/comments_analysis_scores.csv"
  cleaned_df.to_csv(csv_file_path, index=False, encoding='utf-8')

  # Write updated file to bucket
  target_folder = "weekly/comments_analysis_scores.csv"
  updated_blob = bucket.blob(target_folder)
  updated_blob.upload_from_filename(csv_file_path)
  print(f"comments_analysis_scores.csv saved to {target_folder}")

  
  return "All Good"

  
