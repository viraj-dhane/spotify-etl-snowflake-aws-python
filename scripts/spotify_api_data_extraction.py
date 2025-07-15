import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    cilent_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager = SpotifyClientCredentials(client_id=cilent_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')

    playlist_link = "https://open.spotify.com/playlist/31ymdYCITDnZRtkKzP3Itp?si=zcrpuq8ZTtORizQ_qp6A2Q&nd=1&dlsi=6af549d0a50c4daf"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]

    spotify_data = sp.playlist_tracks(playlist_URI)

    client = boto3.client('s3')

    filename = "spotify_raw_" + str(datetime.now()) + ".json"

    client.put_object(
        Bucket="data-engg-project-spotify-etl-data-pipeline",
        Key= "raw_data/to_process/" + filename,    # path where we want to store data
        Body=json.dumps(spotify_data)   #json - convert data into json string and store it in s3
    )
