import io       #Lire et écrire des données dans le csv
import pandas as pd     #Convertir les dictionnaires en une DateFrame => Manipulation de données
from google.cloud import storage, bigquery
import os
import csv
import google.auth
from googleapiclient.discovery import build
import pyarrow  #Lire et écrire les données sur BigQuery


def hello_world(request):
    
    # Remplacez la chaîne ID et la clé API par les vôtres
    channel_id = 'UCHYjRLGdr85gxGfRwC8G-Gw'
    api_key = 'AIzaSyAwqUXgS03FRDEmmT6tCo7jGQeX0nNDZpw'

    # Création d'un client pour accéder à l'API YouTube
    youtube = build('youtube', 'v3', developerKey=api_key)

    # Récupération de la liste des vidéos de la chaîne
    videos = []
    next_page_token = None

    while True:
        # Récupération des informations de la page suivante
        res = youtube.search().list(    #Appel de l'API pour récupérer les données YouTube
            part='id,snippet',      #On dit quoi récupérer (id de vidéo et snippet)
            channelId=channel_id,
            type='video',   #Type de contenu vidéo
            maxResults=70,  #À MODIFIER EN FONCTION DE LA CHAINE YOUTUBE QU'ON ANALYSE
            pageToken=next_page_token
        ).execute()

        # Ajout des vidéos à la liste
        videos += res['items']

        # Si la dernière page est atteinte, on sort de la boucle
        next_page_token = res.get('nextPageToken')
        if next_page_token is None:
            break

    # Récupération des statistiques de chaque vidéo
    videos_stats = []
    for video in videos:
        video_id = video['id']['videoId']
        res = youtube.videos().list(
            part='statistics',
            id=video_id
        ).execute()

        statistics = res['items'][0]['statistics']

        # Enregistrement des statistiques de la vidéo dans une liste
        video_stats = {             #Un dictionnaire par catégorie => Le tout forme une liste
            'video_id': video_id,
            'title': video['snippet']['title'],
            'published_at': video['snippet']['publishedAt'],
            'views': statistics['viewCount'],
            'likes': statistics['likeCount'],
            'comments': statistics['commentCount']
        }
        videos_stats.append(video_stats)        #Création d'une liste

    # Enregistrement des statistiques dans un fichier CSV
    with open('/tmp/videos_stats.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Écriture de l'en-tête du fichier CSV
        writer.writerow(['video_id', 'title', 'published_at', 'views', 'likes', 'comments'])
        
        # Écriture des données dans le fichier CSV
        for video_stats in videos_stats:
            writer.writerow([video_stats['video_id'], video_stats['title'], video_stats['published_at'], video_stats['views'], video_stats['likes'], video_stats['comments']])

    
    df = pd.DataFrame(videos_stats)         #Convertir les listes en dataFrame
    # Convert dataframe to CSV
    df_csv = df.to_csv(index=False).encode("utf-8")

    storage_client = storage.Client()
    bucket_name = "commentaires_analysis"

    # Write updated file to bucket
    bucket = storage_client.get_bucket(bucket_name)     #Permet de récupérer un bucket
    target_folder = "historique/videos_stats.csv"            #Ciblage du bucket cherché
    updated_blob = bucket.blob(target_folder)           
    updated_blob.upload_from_string(df_csv)

    #ID de connection
    project_id = "projet-big-data-380518"
    dataset_id = "stats_video"
    table_id = f"{project_id}.{dataset_id}.stats_videos"

    #Ecriture du BigQuery à partir du DataFrame en se basant du df
    bq_client = bigquery.Client()   #Créer un client BigQuery pour se connecter au projet
    table_ref = bq_client.dataset(dataset_id).table('stats_videos')     #Table cible pour envoyer les données
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE     #Ecrase les données précédentes et les remplace si elles existent dejà
    load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)       #Charge les données 
    load_job.result()
    print(f"{len(videos_stats)} rows inserted into BigQuery table {table_id}")

    return "All Good"