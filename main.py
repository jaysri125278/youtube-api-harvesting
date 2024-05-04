import streamlit as st
from streamlit_option_menu import option_menu
import googleapiclient.discovery
import googleapiclient.errors
import mysql.connector
from datetime import datetime
import pandas as pd
import plotly.express as px
import time


api_service_name = 'youtube'
api_version = 'v3'
api_key = 'AIzaSyDu4i8Rro-w4onGCpS5VPTuHMsiLEbNQms'

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

def convert_datetime(youtube_datetime):
    youtube_format = '%Y-%m-%dT%H:%M:%SZ'
    mysql_format = '%Y-%m-%d %H:%M:%S'
    return datetime.strptime(youtube_datetime, youtube_format).strftime(mysql_format)
def parse_duration(duration_str):
    if duration_str.startswith('PT'):
        duration_str = duration_str[2:]  # Remove 'PT' prefix
        hours, minutes, seconds = 0, 0, 0
        if 'H' in duration_str:
            hours, duration_str = duration_str.split('H')
            hours = int(hours)
        if 'M' in duration_str:
            minutes_str = duration_str.split('M')[0]
            if 'S' in minutes_str:
                minutes_str, seconds_str = minutes_str.split('S')
                seconds = int(seconds_str)
            minutes = int(minutes_str)
        elif 'S' in duration_str:
            seconds = duration_str.split('S')[0]
            seconds = int(seconds)
        duration = hours * 3600 + minutes * 60 + seconds
    else:
        duration = 0
    return duration


def get_channel_data(c_id):
    request = youtube.channels().list(
        part = 'snippet, contentDetails, statistics, status',
        id = c_id
    )

    response = request.execute()
    channel_data = []
    for item in response['items']:
        data = dict(
            channel_name = item['snippet']['title'],
            channel_id = item['id'],
            subscription_count = item['statistics']['subscriberCount'],
            thumbnail = item['snippet']['thumbnails']['default']['url'],
            channel_views = item['statistics']['viewCount'],
            Views=item["statistics"]["viewCount"],
            Total_Videos=item["statistics"]["videoCount"],
            channel_description = item['snippet']['description'],
            playlist_id = item['contentDetails']['relatedPlaylists']['uploads'],
            status = item['status']['privacyStatus']
        )
        channel_data.append(data)
    return channel_data

def get_playlist_data(channel_ids):
    playlist_data = []
    for cid in channel_ids:
        request = youtube.channels().list(
            part = 'snippet, contentDetails, statistics',
            id = cid
        )
        response = request.execute()
        for item in response['items']:
            data = dict(
                channel_id = item['id'],
                playlist_id = item['contentDetails']['relatedPlaylists']['uploads'],
            )
            playlist_data.append(data)
    return playlist_data
def get_playlist_id(c_id):
    playlist_ids = []
    for cid in c_id:
        request = youtube.channels().list(
            part = 'snippet, contentDetails, statistics',
            id = cid
        )
        response = request.execute()
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        playlist_ids.append(playlist_id)
    return playlist_ids
def get_video_data(p_id):
    next_page_token = None
    playlist_id = p_id
    video_data = []  # Define video_data before the loop

    while True:
        request = youtube.playlistItems().list(
            part="contentDetails,snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        
        response = request.execute()
        video_ids = [response['items'][i]['contentDetails']['videoId'] for i in range(len(response['items']))]
        next_page_token = response.get('nextPageToken')

        batch_video_data = get_video_details(video_ids, playlist_id)
        video_data.extend(batch_video_data) 

        if next_page_token is None:
            break

    return video_data
def get_video_details(video_ids, p_id):
    v_id = ','.join(video_ids)
    video_data = []
    request = youtube.videos().list(
        part="contentDetails,snippet,statistics",
        id=v_id
    )
    response = request.execute()

    for index, video in enumerate(response['items'], start=1):
      video_details = dict(
          Channel_name = video['snippet']['channelTitle'],
          Channel_id = video['snippet']['channelId'],
          Video_id =  video['id'],
          Playlist_id = p_id,
          Title =  video['snippet']['title'],
          Tags =  video['snippet'].get('tags'),
          Thumbnail = video['snippet']['thumbnails']['default']['url'],
          Description = video['snippet']['description'],
          Published_date = convert_datetime(video['snippet']['publishedAt']),
          Duration = parse_duration(video['contentDetails']['duration']),
          Views = video['statistics']['viewCount'],
          Likes = video['statistics'].get('likeCount'),
          Comments = video['statistics'].get('commentCount'),
          Favorite_count = video['statistics']['favoriteCount'],
          Definition = video['contentDetails']['definition'],
          Caption_status = video['contentDetails']['caption']
      )
        
      video_data.append(video_details)
    return video_data
def get_comment_data(video_ids):
    comment_data = []
    next_page_token = None
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token
            )
            response = request.execute()

            for item in response['items']:
                comment_details = dict(
                    video_id=video_id,
                    comment_id=item['id'],
                    comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    published_date=convert_datetime(item['snippet']['topLevelComment']['snippet']['publishedAt'])
                )
                comment_data.append(comment_details)
    except googleapiclient.errors.HttpError as e:
        if e.resp.status == 403:
            st.warning("Comments are disabled for some video.")
        else:
            st.warning("An error occurred: {}".format(e))
    return comment_data

def connection_to_mysql():
    connection = mysql.connector.connect(
        host = 'localhost',
        port = '3306',
        user = 'root',
        password = "1234",
        database = "youtube"
    )
    return connection
def insert_channel_details(connection,channel_data):
    values = [(data['channel_id'], data['channel_name'], data['channel_views'], data['channel_description'], data['status'])for data in channel_data]
    cursor = connection.cursor() 
    query = f"insert into channel (channel_id, channel_name, channel_views, channel_description, channel_status) Values (%s, %s, %s, %s, %s);" 
    try:
        cursor.executemany(query, values)  
        connection.commit()
        return True
    except Exception as e:
        return str(e)
def insert_playlist_details(connection, playlist_data):
    cursor = connection.cursor()
    query = "INSERT INTO playlist (playlist_id, channel_id) VALUES (%s, %s)"
    try:
        for data in playlist_data:
            playlist_id = data.get('playlist_id')
            channel_id = data.get('channel_id')
            cursor.execute(query, (playlist_id, channel_id))

        connection.commit()
        return True  
    except Exception as e:
        return str(e)  


def insert_video_details(connection, video_data):
    cursor = connection.cursor()
    query = """
        INSERT INTO video (
            video_id, playlist_id, video_name, video_description,
            published_date, view_count, like_count, dislike_count,
            favourite_count, comment_count, duration, thumbnail,
            caption_status
        ) 
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    try:
        for video_list in video_data:
            for video_details in video_list:
                cursor.execute(query, (
                    video_details.get('Video_id', ''),
                    video_details.get('Playlist_id', ''),
                    video_details.get('Title', ''),
                    video_details.get('Description', ''),
                    video_details.get('Published_date', ''),
                    video_details.get('Views', ''),
                    video_details.get('Likes', ''),
                    video_details.get('Dislikes', 0),
                    video_details.get('Favorite_count', ''),
                    video_details.get('Comments', ''),
                    video_details.get('Duration', ''),
                    video_details.get('Thumbnail', ''),
                    video_details.get('Caption_status', '')
                ))

        connection.commit()
        return True 
    except Exception as e:
        return str(e) 


def insert_comment_data(connection, comments):
    cursor = connection.cursor()
    try:
        for comment in comments:
            insert_query = """
                INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date)
                VALUES (%s, %s, %s, %s, %s)
            """
            data = (
                comment["comment_id"],
                comment["video_id"],
                comment["comment_text"],
                comment["author"],
                comment["published_date"],
            )
            cursor.execute(insert_query, data)

        connection.commit()
        return True
    except Exception as e:
        return str(e)


if __name__ == '__main__':
    connection = connection_to_mysql()
    with st.sidebar:
        selected = option_menu(None, ["Home", 'Data zone', 'Query Zone'], 
            icons=['house', 'bi-database', 'bi-bar-chart'], menu_icon="cast", default_index=1)
    if selected == 'Home':
        st.title(':orange[YouTube Data Harvesting and Warehousing using SQL and Streamlit]')
        st.write("## **<span style='color:orange'>Domain: </span>** Social Media", unsafe_allow_html=True)
        st.write("## **<span style='color:orange'>Skills: </span>** Python, Streamlit, SQL", unsafe_allow_html=True)
        st.write("## **<span style='color:orange'>Developed By: </span>** Jaysri Saravanan", unsafe_allow_html=True)

    if selected == 'Data zone':
        tab1, tab2 = st.tabs([r"$\huge Extract $", r"$\huge Store $"])
        with tab1:
            st.markdown("#    ")

            st.title(":orange[YouTube Channel Data Extractor]")
        
            channel_id = st.text_input("**Enter the channel ID:**")
            if st.button('view channel details'):
                if channel_id:
                    channel_data = get_channel_data(channel_id)
                    for channel in channel_data:
                        st.markdown(f"<h2 style='color:red'>{channel['channel_name']}</h2>", unsafe_allow_html=True)
                        st.image(channel['thumbnail'])
                        st.write(f"**<span style='color:orange'>Channel ID:</span>** {channel['channel_id']}", unsafe_allow_html=True)
                        st.write(f"**<span style='color:orange'>Subscription Count:</span>** {channel['subscription_count']}", unsafe_allow_html=True)
                        st.write(f"**<span style='color:orange'>Channel Views:</span>** {channel['channel_views']}", unsafe_allow_html=True)
                        st.write(f"**<span style='color:orange'>Playlist ID:</span>** {channel['playlist_id']}", unsafe_allow_html=True)
                        st.write(f"**<span style='color:orange'>Status:</span>** {channel['status']}", unsafe_allow_html=True)
                        st.write('---')
                else:
                    st.warning("Enter channel details")
        with tab2: 
            st.title(":orange[Store In SQL]")
            channel_data = get_channel_data(channel_id)

            ch_id = channel_id.split(', ')
                
            pl_data = get_playlist_data(ch_id)
            pl_ids = get_playlist_id(ch_id)

            vdo_data = []
            for p_ids in pl_ids:
                v_data = get_video_data(p_ids)
                vdo_data.append(v_data)

            video_ids = [item["Video_id"] for sublist in vdo_data for item in sublist]
            cmt_data = get_comment_data(video_ids)
            
            st.write("## Channel Data")
            ch_df = pd.DataFrame(channel_data)
            st.dataframe(ch_df)

            st.write("## Video Data")
            videos =[]
            for video_list in vdo_data:
                for video_details in video_list:
                    videos.append(video_details)
            
            video_df = pd.DataFrame(videos)
            st.dataframe(video_df)

            st.write("## Comment Data")
            cmt_df = pd.DataFrame(cmt_data)
            st.dataframe(cmt_df)

            if st.button('upload to SQL'):
                ch_status = insert_channel_details(connection, channel_data)
                if ch_status != True:
                    st.warning(ch_status)

                pl_status = insert_playlist_details(connection, pl_data)
                if pl_status != True:
                    st.warning(pl_status)

                vdo_status = insert_video_details(connection, vdo_data)
                if vdo_status != True:
                    st.warning(vdo_status)
                
                
                cmt_status = insert_comment_data(connection,cmt_data)
                if cmt_status != True:
                    st.warning(cmt_status)

                if ch_status == pl_status == vdo_status == cmt_status == True:
                    success_placeholder = st.empty()
                    success_placeholder.success("Data inserted successfully!")
                    time.sleep(3)
                    success_placeholder.empty()

        
    if selected == "Query Zone":
            mycursor = connection.cursor()
            st.write("## :orange[Select any question to get Insights]")
            questions = st.selectbox('Questions',
            ['1. What are the names of all the videos and their corresponding channels?',
            '2. Which channels have the most number of videos, and how many videos do they have?',
            '3. What are the top 10 most viewed videos and their respective channels?',
            '4. How many comments were made on each video, and what are their corresponding video names?',
            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
            '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
            '8. What are the names of all the channels that have published videos in the year 2022?',
            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
            '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
            
            if questions == '1. What are the names of all the videos and their corresponding channels?':
                mycursor.execute("""SELECT v.video_id, c.channel_name
                                    FROM video v
                                    JOIN playlist p ON v.playlist_id = p.playlist_id
                                    JOIN channel c ON p.channel_id = c.channel_id;
                                    """)
                df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                st.write(df)
                
            elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
                mycursor.execute("""SELECT c.channel_name, COUNT(v.video_id) AS video_count
                                    FROM video v
                                    JOIN playlist p ON v.playlist_id = p.playlist_id
                                    JOIN channel c ON p.channel_id = c.channel_id
                                    GROUP BY c.channel_name
                                    ORDER BY video_count DESC;
                                    """)
                df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                st.write(df)
                
            elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
                mycursor.execute("""SELECT v.video_name, c.channel_name, v.view_count
                                    FROM video v
                                    JOIN playlist p ON v.playlist_id = p.playlist_id
                                    JOIN channel c ON p.channel_id = c.channel_id
                                    ORDER BY v.view_count DESC
                                    LIMIT 10;
                                    """)
                df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                st.write(df)
                
            elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
                mycursor.execute("""SELECT v.video_name, COUNT(c.comment_id) AS num_comments
                                    FROM video v
                                    LEFT JOIN comment c ON v.video_id = c.video_id
                                    GROUP BY v.video_name;
                                    """)
                df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                st.write(df)
                
            elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
                mycursor.execute("""SELECT v.video_name, COUNT(c.comment_id) AS num_comments
                                    FROM video v
                                    LEFT JOIN comment c ON v.video_id = c.video_id
                                    GROUP BY v.video_name;
                                    """)
                df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                st.write(df)
                
                
            elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
                mycursor.execute("""SELECT v.video_name, SUM(v.like_count) AS total_likes, SUM(v.dislike_count) AS total_dislikes 
                                 FROM video v GROUP BY v.video_name;
                                """)
                df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                st.write(df)
                
            elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
                mycursor.execute("""SELECT c.channel_name, SUM(v.view_count) AS total_views FROM channel c 
                                 INNER JOIN playlist p ON c.channel_id = p.channel_id 
                                 INNER JOIN video v ON p.playlist_id = v.playlist_id 
                                 GROUP BY c.channel_name;
                                """)
                df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                st.write(df)
                
                
            elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
                mycursor.execute("""SELECT DISTINCT c.channel_name FROM channel c 
                                 INNER JOIN playlist p ON c.channel_id = p.channel_id 
                                 INNER JOIN video v ON p.playlist_id = v.playlist_id 
                                 WHERE YEAR(v.published_date) = 2022;
                                """)
                df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                st.write(df)
                
            elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                mycursor.execute("""SELECT c.channel_name, AVG(v.duration) AS average_duration 
                                 FROM channel c 
                                 INNER JOIN playlist p ON c.channel_id = p.channel_id 
                                 INNER JOIN video v ON p.playlist_id = v.playlist_id 
                                 GROUP BY c.channel_name;
                                """)
                df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                st.write(df)
                
                
            elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
                mycursor.execute("""SELECT c.channel_name, v.video_name, v.comment_count 
                                 FROM video v INNER JOIN playlist p ON v.playlist_id = p.playlist_id INNER JOIN channel c 
                                 ON p.channel_id = c.channel_id
                                 WHERE v.comment_count = (SELECT MAX(comment_count) FROM video);
                                 """)
                df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
                st.write(df)
                
                        