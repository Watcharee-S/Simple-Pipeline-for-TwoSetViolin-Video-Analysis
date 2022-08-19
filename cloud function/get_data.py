from googleapiclient.discovery import build
from google.cloud import storage
import base64
import os
import json
import pandas as pd

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

def to_csv(data):
    video_data = []
    for i in range(len(data)):
        video_df = {}
        video_df['publish'] = data[i]['snippet']['publishedAt']
        video_df['title'] = data[i]['snippet']['title']
        if 'defaultLanguage' not in data[i]['snippet'].keys():
            video_df['language'] = None
        else:
            video_df['language'] = data[i]['snippet']['defaultLanguage']
        video_df['category'] = data[i]['snippet']['categoryId']
        video_df['view'] = data[i]['statistics']['viewCount']
        video_df['like'] = data[i]['statistics']['likeCount']
        if 'commentCount' not in data[i]['statistics'].keys():
            video_df['comments'] = None
        else:
            video_df['comments'] = data[i]['statistics']['commentCount']
        video_data.append(video_df)
        
    df = pd.DataFrame(video_data)
    df_str = df.to_csv(index = False)
    return df_str

## writing file to gcs
def write_json_to_gcs():
    data = get_video()

    bucket_name = os.environ.get("bucket_name")
    destination_blob_names = os.environ.get("destination_json")
    contents = json.dumps(data, ensure_ascii=False)
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_names)

    blob.upload_from_string(contents)

def write_csv_to_gcs():
    data = get_video()

    bucket_name = os.environ.get("bucket_name")
    destination_blob_names = os.environ.get("destination_csv")
    contents = to_csv(data)
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_names)

    blob.upload_from_string(contents, content_type='text/csv')

def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        write_json_to_gcs()
        write_csv_to_gcs()
        return f'Upload Success!'
