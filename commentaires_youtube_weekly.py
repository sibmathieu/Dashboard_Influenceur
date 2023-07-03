import os
import csv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import storage
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta

def commentaires(request):
    api_key = "AIzaSyAwqUXgS03FRDEmmT6tCo7jGQeX0nNDZpw"
    youtube = build("youtube", "v3", developerKey=api_key)

    channel_id = "UCHYjRLGdr85gxGfRwC8G-Gw"
    page_token = ""
    videos = []

    today = datetime.now()

    while page_token is not None:
        try:
            uploads_list = youtube.channels().list(part="contentDetails", id=channel_id).execute()

            playlist_id = uploads_list["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

            playlist_items = (
                youtube.playlistItems()
                .list(part="snippet,contentDetails", playlistId=playlist_id, pageToken=page_token)
                .execute()
            )

            for item in playlist_items["items"]:
                video_id = item["contentDetails"]["videoId"]
                videos.append(video_id)

            page_token = playlist_items.get("nextPageToken")

        except HttpError as e:
            print("An error occurred: %s" % e)
            page_token = None

    video_comments = []
    for video_id in videos:
        try:
            video_response = youtube.videos().list(part="snippet", id=video_id).execute()

            video_title = video_response["items"][0]["snippet"]["title"]

            comments = []
            dates = []
            results = youtube.commentThreads().list(part="snippet", videoId=video_id, textFormat="plainText").execute()

            while results:
                for item in results["items"]:
                    clean_date = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"][0:10]
                    date_publication = datetime.strptime(clean_date, "%Y-%m-%d")
                    if date_publication >= today - timedelta(days=7):
                        comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                        comment_date = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                        # Remove line breaks within comments
                        comment = comment.replace("\n", " ").replace("\r", "")
                        comments.append(comment)
                        dates.append(comment_date)

                # Check if there are more comments
                if "nextPageToken" in results:
                    results = (
                        youtube.commentThreads()
                        .list(
                            part="snippet",
                            videoId=video_id,
                            textFormat="plainText",
                            pageToken=results["nextPageToken"],
                        )
                        .execute()
                    )
                else:
                    break

            for comment in comments:
                video_comment_dict = {
                    "video_id": video_id,
                    "video_title": video_title,
                    "comment": comment,
                    "date": comment_date,
                }
                video_comments.append(video_comment_dict)

        except HttpError as e:
            print("An error occurred: %s" % e)

    # Upload the comments directly to a Cloud Storage bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket("commentaires_analysis")
    lastweek = today - timedelta(days=7)
    today = today.strftime("%Y-%m-%d")
    lastweek = lastweek.strftime("%Y-%m-%d")
    blob = bucket.blob(f"weekly/{lastweek}_{today}/comments.csv")

    with NamedTemporaryFile(mode="w", delete=False, newline="") as temp_file:
        fieldnames = ["video_id", "video_title", "comments", "date"]
        writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
        writer.writeheader()

        for comment_dict in video_comments:
            video_id = comment_dict["video_id"]
            video_title = comment_dict["video_title"]
            comment = comment_dict["comment"]
            date = comment_dict["date"]

            writer.writerow({"video_id": video_id, "video_title": video_title, "comments": comment, "date": date})

    blob.upload_from_filename(temp_file.name, content_type="text/csv")

    return "ok"