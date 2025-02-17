from googleapiclient.discovery import build
import re
import os
import pandas as pd

from quixstreams import Application
import json
import logging
import time
from dotenv import load_dotenv

load_dotenv()
youtube_key = os.getenv('youtube_key')
print("🇲🇽",youtube_key)
youtube = build('youtube', 'v3', developerKey=youtube_key)


### YOUTUBE API FUNCTIONS
def get_videoid(url):
    # Extract video ID from both regular and shorts URLs
    videoid = re.search(r"(?:v=|\/shorts\/)([^&\/]+)", url)
    if videoid:
        return videoid.group(1)
    return None

def get_video_stats(videoids):

    # Join video IDs into a single string
    videoids_str = ','.join(videoids)

    # Request to get video statistics for the last 50 videos
    request = youtube.videos().list(
        part="statistics,snippet,contentDetails",
        id=videoids_str
    )

    response = request.execute()


    # Extract video stats
    video_stats = []
    
    for item in response['items']:
        individualVidData = item['snippet']
        contentDetails = item['contentDetails']
        globalVidData = item['id']
        individualVidDataStats = item['statistics']

        try:
            tagg = ",".join(individualVidData.get('tags'))
        except:
            tagg= "No Tags"

        video_stats.append({
        'videoid':globalVidData,
        'publishedat':individualVidData['publishedAt'],
        'channelid':individualVidData['channelId'],
        'title':individualVidData['title'],
        'description':individualVidData['description'],
        'thumbnails':individualVidData['thumbnails']['default']['url'],
        'channeltitle':individualVidData['channelTitle'],
        'tags':tagg,
        'categoryid': individualVidData['categoryId'],
        'viewcount': int(individualVidDataStats.get('viewCount', 0)),
        'duration': contentDetails.get('duration'),
        'likecount': int(individualVidDataStats.get('likeCount', 0)),
        'favoritecount': int(individualVidDataStats.get('favoriteCount', 0)),
        'commentcount': int(individualVidDataStats.get('commentCount', 0))
    })

    return video_stats

def iso8601_to_seconds(duration):
    hours = minutes = seconds = 0
    match = re.match(r'PT((\d+)H)?((\d+)M)?((\d+)S)?', duration)
    if match:
        hours = int(match.group(2)) if match.group(2) else 0
        minutes = int(match.group(4)) if match.group(4) else 0
        seconds = int(match.group(6)) if match.group(6) else 0
    return hours * 3600 + minutes * 60 + seconds

def cleanVideoDF(vStats=dict, videoType:str=None):
    df_video = pd.DataFrame(vStats).copy()
    df_video['publishedat'] = pd.to_datetime(df_video['publishedat'])
    
    df_video['duration'] =  df_video['duration'].apply(iso8601_to_seconds)
    df_video['typeofvideo'] = videoType

    df_video['year'] = df_video['publishedat'].dt.year
    df_video['month'] = df_video['publishedat'].dt.month
    df_video['month_name'] = df_video['publishedat'].dt.month_name()
    df_video['day_name'] = df_video['publishedat'].dt.day_name()
    df_video['day_type'] = df_video['publishedat'].dt.weekday.apply(lambda x: 'Weekend' if x >= 5 else 'Weekday')

    df_video['first_day_of_month'] = df_video['publishedat'].dt.tz_localize(None).dt.to_period('M').dt.start_time

    df_video['publishedat'] = df_video['publishedat'].astype(int) // 10**9  # Convert to epoch time
    df_video['first_day_of_month'] = df_video['first_day_of_month'].astype(int) // 10**9  # Convert to epoch time
    

    df_video_to_export = df_video.copy().to_dict('records')
    return df_video_to_export

def get_singleVideoData(url:str = None):
    id_v = get_videoid(url)
    vid_stats = get_video_stats(videoids=[id_v])
    df_video_to_export = cleanVideoDF(vStats=vid_stats, videoType="video")
    summaryOfData = df_video_to_export[0]
    
    return {
        "videoid":summaryOfData['videoid'],
        "viewcount":summaryOfData['viewcount'],
        "likecount":summaryOfData['likecount'],
        "commentcount":summaryOfData['commentcount'],
        "duration":summaryOfData['duration'],
        "title":summaryOfData['title'],
        "channelid":summaryOfData['channelid'],
    }

### KAFKA FUNCTIONS
def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG"
    )

    with app.get_producer() as producer:
        while True:
            video = get_singleVideoData(url='https://www.youtube.com/watch?v=lFbany6g8Kw')
            
            logging.debug(f"Sending video data: {video}")
            
            producer.produce(
                topic = "track_video",
                key="Youtube",
                value=json.dumps(video)
            )
            logging.info("Video data sent")
            time.sleep(60)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()