import csv
import os
import json

from googleapiclient.discovery import build

# * IMPORTANT
query = r'python tutorial'

# read secret api key
with open('./SECRETS.json', 'r') as f:
    secrets = json.load(f)
api_token = secrets['api_token']

# Functions
def save_response_to_JSON(file_name, response) -> None:
    with open(file_name + '.json', 'w', encoding='utf-8', newline='') as f:
        json.dump(response, f)

# creating data directory to save data
if not os.path.exists('./data'):
    os.mkdir('data')


###########################
# # Start fetching data # #
###########################


# building connection
youtube = build('youtube', 'v3', developerKey=api_token)

# fetching top 5 channels
if not os.path.exists('./data/channels.csv'):
    print('Start collecting top 5 playlists in search for "python"...')
    playlist_response = youtube.search().list(
        part='snippet',
        q=query,
        maxResults=5,
        type='playlist'
    ).execute()

    # parsing results and saving to csv
    with open('./data/channels.csv', 'w', encoding='utf-8', newline='') as channels_CSV:
        csv_writer = csv.writer(channels_CSV, quoting=csv.QUOTE_NONNUMERIC)
        csv_writer.writerow(['channeltitle','channelId'])

        for item in playlist_response['items']:
            row = [
                item['snippet']['channelTitle'],
                item['snippet']['channelId']
            ]
            csv_writer.writerow(row)

    print('Done collecting top 5 playlists!')
else:
    print('Already Satisfied playlists!')


# collecting channels statistics
if not os.path.exists('./data/channel_statistics.csv'):

    print('Start fetching channels Statistics...')

    with open('./data/channel_statistics.csv', 'w', encoding='utf-8', newline='') as channel_statistics_CSV:
        csv_writer = csv.writer(channel_statistics_CSV, quoting=csv.QUOTE_NONNUMERIC)
        csv_writer.writerow(['channelTitle','channelId','uploadsId','videoCount','subscriberCount','viewCount','commentCount'])

        with open('./data/channels.csv', 'r', encoding='utf-8') as channels_CSV:
            csv_reader = csv.reader(channels_CSV)
            next(csv_reader) # skip header

            for playlist in csv_reader:
                channelResponse = youtube.channels().list(
                    part='contentDetails,statistics',
                    id=playlist[1]
                ).execute()

                for item in channelResponse['items']:
                    row = [
                        playlist[0],
                        playlist[1],
                        item['contentDetails']['relatedPlaylists']['uploads'],
                        item['statistics'].get('videoCount'),
                        item['statistics'].get('subscriberCount'),
                        item['statistics'].get('viewCount'),
                        item['statistics'].get('commentCount')
                    ]

                    csv_writer.writerow(row)

    print('Done fetching channels Statistics!')
else:
    print('Already Satisfied channels Statistics!')


# getting all videos for each channel
# creating video_ids.csv to store results
if not os.path.exists('./data/video_ids.csv'):
    print('Start fetching Videos...')
    with open('./data/video_ids.csv', 'w', encoding='utf-8', newline='') as videos_CSV:
        csv_writer = csv.writer(videos_CSV, quoting=csv.QUOTE_NONNUMERIC)
        csv_writer.writerow(['channelTitle','videoTitle','videoId','publishedDateTime'])

        with open('./data/channel_statistics.csv', 'r', encoding='utf-8') as channel_statistics_CSV:
            csv_reader = csv.reader(channel_statistics_CSV)
            next(csv_reader) # skip header

            for channel in csv_reader:
                nextPageToken = None
                # collecting videos then appending to video_ids.csv
                while True:
                    # fetching result
                    videosResponse = youtube.playlistItems().list(
                        part='snippet',
                        playlistId=channel[2],
                        maxResults=50,
                        pageToken=nextPageToken
                    ).execute()

                    # appending results to video_ids.csv
                    for item in videosResponse['items']:
                        row = [
                            channel[0],
                            item['snippet']['title'],
                            item['snippet']['resourceId']['videoId'],
                            item['snippet']['publishedAt']
                        ]

                        csv_writer.writerow(row)

                    # retriving next page token
                    nextPageToken = videosResponse.get('nextPageToken')

                    # break while, when no more videos
                    if not nextPageToken: break
                print(f'Done fetching videos from channel: {channel[0]}...')

    print(f'Done fetching videos!')
else:
    print('Already Satisfied videos!')



# fetching video data
if not os.path.exists('./data/videos_data.csv'):
    print('Start fetching Videos Data...')

    with open('./data/videos_data.csv', 'w', encoding='utf-8', newline='') as videos_data_CSV:
        csv_writer = csv.writer(videos_data_CSV, quoting=csv.QUOTE_NONNUMERIC)
        csv_writer.writerow(['channelTitle','videoId','videoTitle','videoViews','videoLike','videoDislike','videoComment','publishedDateTime'])

        with open('./data/video_ids.csv', 'r', encoding='utf-8') as videos_CSV:
            csv_reader = csv.reader(videos_CSV)
            next(csv_reader) # skip header

            lastLoop = True
            counter = 1

            while lastLoop:
                # read 50 videos at a time
                video_ids = []
                for _ in range(50):
                    try: videoId = next(csv_reader)
                    except StopIteration:
                        lastLoop = False
                        break
                    video_ids.append(videoId[2])

                videos_dataResponse = youtube.videos().list(
                    part='snippet,statistics',
                    id=','.join(video_ids),
                    maxResults=50
                ).execute()

                # appending results to videos_data.csv
                for item in videos_dataResponse['items']:
                    row = [
                        item['snippet']['channelTitle'],
                        item['id'],
                        item['snippet']['title'],
                        item['statistics'].get('viewCount'),
                        item['statistics'].get('likeCount'),
                        item['statistics'].get('dislikeCount'),
                        item['statistics'].get('commentCount'),
                        item['snippet']['publishedAt']
                    ]

                    csv_writer.writerow(row)


                print(f'Done fetching Videos Data: {counter}...')
                counter += 1
    print(f'Done fetching videos!')
else:
    print('Already Satisfied videos Data!')