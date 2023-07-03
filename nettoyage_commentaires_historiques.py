import pandas as pd
import numpy as np
import re
from unidecode import unidecode
import csv
import io
from google.cloud import storage

def hello_world(request):
    storage_client = storage.Client()

    bucket_name = "commentaires_analysis"
    file_name = "historique/comments.csv"

    # Read file from bucket as pandas dataframe
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_string()
    df = pd.read_csv(io.BytesIO(data))
    print(f"Loaded {len(df)} rows from {file_name}")

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
    for video_id, video_title, comment in zip(df['video_id'], df['video_title'], df['comments']):
        cleaned_comment = clean_comments(comment)
        if isinstance(cleaned_comment, str):
            cleaned_comments.append(cleaned_comment)
            video_ids.append(video_id)
            video_titles.append(video_title)

    # Créer un nouveau DataFrame avec les commentaires nettoyés
    cleaned_df = pd.DataFrame({'video_id': video_ids, 'video_title': video_titles, 'comments': cleaned_comments})

    # Supprimer les lignes qui contiennent uniquement des chiffres
    cleaned_df = cleaned_df[~cleaned_df["comments"].str.isnumeric()]

    # Supprimer les lignes avec des commentaires vides
    cleaned_df = cleaned_df[~cleaned_df["comments"].str.strip().eq("")]

    # Write comments to a CSV file
    csv_file_path = "/tmp/commentaires_nettoyes.csv"
    cleaned_df.to_csv(csv_file_path, index=False, encoding='utf-8')

    # Upload the CSV file to a bucket
    destination_blob_name = "historique/commentaires_nettoyes.csv"

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(csv_file_path)
    return 'ok'
