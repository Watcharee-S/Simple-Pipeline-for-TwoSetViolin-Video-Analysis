from googleapiclient.discovery import build
from google.cloud import storage
import base64
import os
import json

## create youtube api connection
def get_youtube():
    api_service_name = "youtube"
    api_version = "v3"
    api_key = os.environ.get("api_key")
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

## get upload playlist id
def get_channel_upload_id(youtube):
    channel_id = os.environ.get("channel_id")
    request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id
        )

    response = request.execute()
    channel_upload_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return channel_upload_id

## get all upload videos id
def get_video_id(youtube, channel_upload_id):    
    video_ids = []
    more_pages = True
    page_token = ''

    while more_pages:
        if page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                    part ="contentDetails",
                    pageToken = page_token,
                    playlistId = channel_upload_id,
                    maxResults = 50)

            response = request.execute()
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            page_token = response.get('nextPageToken')

    return video_ids

## get data of videos
def get_video():
    youtube = get_youtube()
    channel_upload_id = get_channel_upload_id(youtube)
    video_ids = get_video_id(youtube, channel_upload_id)
    all_video = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
                    part="snippet,statistics",
                    id=",".join(video_ids[i:i+50]))
        response = request.execute()
        for i in range(len(response['items'])):
            all_video.append(response['items'][i])
    return all_video

## writing json file to gcs
def json_write(event, context):
    data = get_video()

    bucket_name = os.environ.get("bucket_name")
    destination_blob_name = os.environ.get("destination_name")
    contents = json.dumps(data, ensure_ascii=False)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)
