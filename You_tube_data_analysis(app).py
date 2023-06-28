import streamlit as st 
import pymongo
import os
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import json
import re
from sqlalchemy.sql import text
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from PIL import Image

st.title("Guvi's You Tube Video Analysis")
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
image = Image.open('guvi_image.jpg')
st.image(image, caption='Guvi Brand Amabassador')

def youtube_api_channel(channel_id):
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"

    # Get credentials and create an API client
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        client_secrets_file, scopes)
    #credentials = flow.run_console()
    credentials= flow.run_local_server(port=0)
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        #id="UCnoqvTW4YZExfDeq7_Wmd-w",
        id=channel_id
        #,maxResults=30
    )
    response = request.execute()

    #json_formatted_str = json.dumps(response, indent=4)
    #print(json_formatted_str)
    #st.write(json_formatted_str)
    return response

def mongo_insert_channel(json_resp):
    #st.write("insert started")
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    my_db=myclient['YouTubeAnalysis']
    my_collection = my_db['channel']
    my_collection.insert_one(json_resp)
    st.write("mongo insert completed")


#taken from playlists list api 
def youtube_api_playlist(channel_id_input):
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"

    # Get credentials and create an API client
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        client_secrets_file, scopes)
    #credentials = flow.run_console()
    credentials= flow.run_local_server(port=0)
   
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id_input,
        maxResults=2
    )
    response = request.execute()
    for item in response['items']:
            item_id = item['id'] # each playlist id of channel 
            youtube_api_videos(item_id)
    return response
    

def youtube_api_videos(playlist_id):
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"

    # Get credentials and create an API client
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        client_secrets_file, scopes)
    credentials= flow.run_local_server(port=0)
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        maxResults=2,
        #playlistId="PLOU2XLYxmsIJ094e8YVdDqLfqwv2XTPyy"
        playlistId=playlist_id
    )
    response = request.execute()
    for item in response['items']:
            content = item['contentDetails']
            youtube_api_each_video(content['videoId'])
    mongo_insert_allvideos(response)
    
def youtube_api_each_video(video_id):
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"
    
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        client_secrets_file, scopes)
    #credentials = flow.run_console()
    credentials= flow.run_local_server(port=0)
   
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        #id="Ks-_Mh1QhMc"
        id=video_id
    )
    response = request.execute()
    mongo_insert_each_video(response)
    #print(response)

def mongo_insert_each_video(json_resp):
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    my_db=myclient['YouTubeAnalysis']
    my_collection = my_db['Video']
    my_collection.insert_one(json_resp)
    st.write("mongo insert of each video completed")


def mongo_insert_allvideos(json_resp):
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    my_db=myclient['YouTubeAnalysis']
    my_collection = my_db['all_videos']
    my_collection.insert_one(json_resp)
    st.write("mongo all videos insert completed")


def mongo_insert_playlist(json_resp):
    #st.write("insert started")
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    my_db=myclient['YouTubeAnalysis']
    my_collection = my_db['Playlist']
    my_collection.insert_one(json_resp)
    st.write("mongo insert completed")


def return_chanel_details(channel_id):
    st.write('sql insert start')
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    my_db=myclient['YouTubeAnalysis']
    my_collection = my_db['channel']

    for ch_response in my_collection.find({"items.id": channel_id}):
        st.write(ch_response)
        view_count = ch_response['items'][0]['statistics']['viewCount']
        channel_name= ch_response['items'][0]['snippet']['title']
        st.write('sql insert complete')
        return channel_id , channel_name, view_count
    
    
def insert_sql_channel(channel_id , channel_name, view_count):
    conn = st.experimental_connection('mysql', type='sql')  
    with conn.session as s:
        s.execute(
            text('INSERT INTO youtube.channel (channel_id, channel_name, channel_view_cnt ) VALUES (:owner, :pet, :dog);'),
            params=dict(owner=channel_id, pet=channel_name , dog=view_count)
                )
        s.commit()

def return_playlist_details(channel_id):
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    my_db=myclient['YouTubeAnalysis']
    my_collection = my_db['Playlist']
    playlist_id=[]
    playlist_name=[]
    #channel_id= 'UC_x5XG1OV2P6uZZ5FSM9Ttw'
    for ch_response in my_collection.find({"items.snippet.channelId": channel_id}):
        for item in ch_response['items']:
            playlist_id.append(item['id'])
            playlist_name.append(item['snippet']['title'])
    return channel_id, playlist_id,playlist_name


def insert_sql_playlist(channel_id , playlist_id, playlist_name):
    conn = st.experimental_connection('mysql', type='sql')  
    length=len(playlist_id)
    with conn.session as s:
        for i in range(length):
            s.execute(
                text('INSERT INTO youtube.playlist (channel_id, playlist_id, playlist_name ) VALUES (:arg1, :arg2, :arg3);'),
                params=dict(arg1=channel_id, arg2=playlist_id[i] , arg3=playlist_name[i])
                    )
            s.commit()

def return_ownerids_details(channel_id):
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    my_db=myclient['YouTubeAnalysis']
    my_collection = my_db['all_videos']
    owner_ids=[]
    for ch_response in my_collection.find({"items.snippet.channelId": channel_id}):
        for item in ch_response['items']:
            if owner_ids.count(item['snippet']['videoOwnerChannelId'])==0:
                owner_ids.append(item['snippet']['videoOwnerChannelId'])
    return owner_ids 


def return_video_details(channel_id):
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    my_db=myclient['YouTubeAnalysis']
    my_collection = my_db['Video']
    Video_id=[]
    Video_name=[]
    views_cnt=[]
    likes_cnt=[]
    duration_min=[]
    comments_cnt=[]
    publish_time=[]
    #channel_id= 'UC_x5XG1OV2P6uZZ5FSM9Ttw'
    for ch_response in my_collection.find({"items.snippet.channelId": channel_id}):
        for item in ch_response['items']:
            
            Video_id.append(item['id'])
            Video_name.append(item['snippet']['title'])
            views_cnt.append(int(item['statistics']['viewCount']))
            likes_cnt.append(int(item['statistics']['likeCount']))
            s=item['contentDetails']['duration']
            st.write(s)
            min = re.search('T(.*)M', s)        
            sec = re.search('M(.*)S', s)                    
            if min==None:                                 # if time is of format PT50S only seconds are presnt 
                sec=re.search('T(.*)S', s)
                val=round( int(sec.group(1))/60 , 2)
            elif sec == None:    #if only min are present PT1M
                val= round( int(min.group(1)),2) 
            else:                                         # if time is of format PT16M5S both min and sec are present
                val= round( int(min.group(1)) + int(sec.group(1))/60 , 2)
            print(min)
            #val= round( int(min.group(1)) + int(sec.group(1))/60 , 2)
            duration_min.append(val)
            comments_cnt.append(int(item['statistics']['commentCount']))
            time=item['snippet']['publishedAt']
            publish_time.append(time[0:19])
    return Video_id , Video_name, views_cnt, likes_cnt, duration_min, comments_cnt, publish_time

def insert_sql_video(Video_id , Video_name, views_cnt, likes_cnt, duration_min, comments_cnt, publish_time):
    conn = st.experimental_connection('mysql', type='sql')  
    length=len(Video_id)
    with conn.session as s:
        for i in range(length):
            s.execute(
                text('INSERT INTO youtube.video (Video_id , Video_name, views_cnt, likes_cnt, duration_min, comments_cnt, publish_time ) VALUES (:arg1 , :arg2,  :arg3, :arg4 , :arg5 ,  :arg6 , :arg7 );'),
                #params=dict(owner=channel_id, pet=playlist_id[i] , dog=playlist_name[i])
                params=dict(arg1 = Video_id[i] , arg2 = Video_name[i], arg3 =  views_cnt[i], arg4 = likes_cnt[i], arg5 = duration_min[i], arg6= comments_cnt[i], arg7 = publish_time[i])               
                    )
            s.commit()


def return_playstid_videoid_details(channel_id):
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    my_db=myclient['YouTubeAnalysis']
    my_collection = my_db['all_videos']
    playlist_ids=[]
    video_ids=[]
    #channel_id= 'UC_x5XG1OV2P6uZZ5FSM9Ttw'
    for ch_response in my_collection.find({"items.snippet.channelId": channel_id}):
        for item in ch_response['items']:
            playlist_ids.append(item['snippet']['playlistId'])
            #print(item['contentDetails']['videoId'])
            video_ids.append(item['contentDetails']['videoId'])
    return playlist_ids, video_ids


def insert_sql_playlist_video_id_details(playlist_ids, video_ids):
    conn = st.experimental_connection('mysql', type='sql')
    length=len(playlist_ids)  
    with conn.session as s:
        for i in range(length):
            s.execute(
                text('INSERT INTO youtube.playlist_video_id_details (playlist_id, video_id ) VALUES (:arg1, :arg2);'),
                params=dict(arg1=playlist_ids[i], arg2 =video_ids[i] )
                    )
            s.commit()


def update_sql_playlist_video_id_details():
    conn = st.experimental_connection('mysql', type='sql')  
    with conn.session as s:
        s.execute(
            text('UPDATE video a SET a.playlist_id = (select distinct playlist_id from playlist_video_id_details b WHERE a.video_id = b.video_id);'),
                )
        s.commit()
    



def question1():
    conn = st.experimental_connection('mysql', type='sql')
    st.subheader('Names of all videos and their corresponding channels')
    df = conn.query('SELECT  Video_name "Video Name" , channel_name "Channel Name" FROM channel a INNER JOIN playlist b ON a.channel_id = b.channel_id INNER JOIN video c ON b.playlist_id = c.playlist_id;', ttl=600)
    df.index = np.arange(1, len(df) + 1)
    st.dataframe(df)

def question2():
    conn = st.experimental_connection('mysql', type='sql')
    st.subheader('Channels have the most number of videos')
    df = conn.query('SELECT  channel_name "Channel Name", count(*) "No of Videos" FROM channel a INNER JOIN playlist b ON a.channel_id = b.channel_id INNER   JOIN video c ON b.playlist_id = c.playlist_id group by a.channel_id , channel_name order by 2 desc limit 1 ;', ttl=600)
    df.index = np.arange(1, len(df) + 1)
    st.dataframe(df)

def question3():
    conn = st.experimental_connection('mysql', type='sql')
    st.subheader('Top 10 most viewed videos')
    df = conn.query(' SELECT channel_name "Channel Name", video_name "Video Name" , views_cnt "No of Views" FROM channel a INNER JOIN playlist b ON a.channel_id = b.channel_id INNER JOIN video c ON b.playlist_id = c.playlist_id order by views_cnt desc limit 10;', ttl=600)
    df.index = np.arange(1, len(df) + 1)
    st.dataframe(df)

def question4():
    conn = st.experimental_connection('mysql', type='sql')
    st.subheader('Comments made on each video')
    df = conn.query('SELECT video_name "Video Name", comments_cnt "No of comments"  FROM channel a INNER JOIN playlist b ON a.channel_id = b.channel_id INNER JOIN video c ON b.playlist_id = c.playlist_id order by 2 desc ;', ttl=600)
    df.index = np.arange(1, len(df) + 1)
    st.dataframe(df)

def question5():
    conn = st.experimental_connection('mysql', type='sql')
    st.subheader('Video with highest number of likes')
    df = conn.query(' SELECT channel_name "Channel Name", video_name "Video Name" , likes_cnt "No of Likes"   FROM channel a INNER JOIN playlist b ON a.channel_id = b.channel_id INNER JOIN video c ON b.playlist_id = c.playlist_id order by 3 desc limit 1;', ttl=600)
    df.index = np.arange(1, len(df) + 1)
    st.dataframe(df)

def question6():
    conn = st.experimental_connection('mysql', type='sql')
    st.subheader('Total number of likes and Disklikes of each Videos')
    df = conn.query(' SELECT channel_name "Channel Name" , video_name "Video Name" , (likes_cnt+ IFNULL(dislikes_cnt, 0) ) cnt   FROM channel a INNER JOIN playlist b ON a.channel_id = b.channel_id INNER JOIN video c ON b.playlist_id = c.playlist_id order by cnt desc ;', ttl=600)
    df.index = np.arange(1, len(df) + 1)
    st.dataframe(df)


def question7():
    conn = st.experimental_connection('mysql', type='sql')
    st.subheader('Total number of views for each channel in (millions)')
    df = conn.query('select channel_name as "channle name" , (channel_view_cnt/1000000) as  "view count" from channel order by 2 desc;', ttl=600)
    df.index = np.arange(1, len(df) + 1)
    st.write(df)
    df=  pd.DataFrame(df)
    df=df.set_index("channle name")
    st.subheader('Total number of views for each channel in (millions)')
    st.bar_chart(data= df, width=0.1, height=0, use_container_width=True)    
    
def question8():
    conn = st.experimental_connection('mysql', type='sql')
    st.subheader('List of Channels published Videos in Year 2022')
    df = conn.query('SELECT distinct channel_name   FROM channel a INNER JOIN playlist b ON a.channel_id = b.channel_id INNER JOIN video c ON b.playlist_id = c.playlist_id  where YEAR(publish_time) = "2022" order by channel_name asc ;', ttl=600)
    df.index = np.arange(1, len(df) + 1)
    st.dataframe(df)

def question9():
    conn = st.experimental_connection('mysql', type='sql')
    st.subheader('Average duration of Videos in chnnels')
    df = conn.query('SELECT channel_name as "channel name", round(avg(duration_min),2) "Average Duration in min"  FROM channel a INNER JOIN playlist b ON a.channel_id = b.channel_id INNER JOIN video c ON b.playlist_id = c.playlist_id group by a.channel_id , channel_name order by 2 desc  ', ttl=600)
    df.index = np.arange(1, len(df) + 1)
    st.write(df)
    df=  pd.DataFrame(df)
    df=df.set_index("channel name")
    st.subheader('Average duration of Videos in chnnels in min')
    st.bar_chart(data= df, width=0.1, height=0, use_container_width=True)        
    st.dataframe(df)

def question10():
    conn = st.experimental_connection('mysql', type='sql')
    st.subheader('Vidoes with highest number of comments')
    df = conn.query('SELECT channel_name as "channel name", comments_cnt  as " comments count"  FROM channel a INNER JOIN playlist b ON a.channel_id = b.channel_id INNER JOIN video c ON b.playlist_id = c.playlist_id order by comments_cnt desc limit 1 ;', ttl=600)
    df.index = np.arange(1, len(df) + 1)
    st.dataframe(df)


if __name__ == "__main__":
    channel_id_input = st.text_input("Provide Channel ID", '')
    
    result = st.button('click here For mongo insert')
    if result == True:
        st.write('Data Lake insert Started')
        channel_json_responce = youtube_api_channel(channel_id_input)
        mongo_insert_channel(channel_json_responce)
        playlist_json_responce = youtube_api_playlist(channel_id_input)
        mongo_insert_playlist(playlist_json_responce)
        st.write('Data Lake insert Completed ')
    result_sql = st.button('click here For sql insert')
    if result_sql == True:
        st.write('Data Base insert Started')
        channel_id , channel_name, view_count = return_chanel_details(channel_id_input)
        insert_sql_channel(channel_id , channel_name, int(view_count))
        channel_id , playlist_id, playlist_name = return_playlist_details(channel_id_input)
        insert_sql_playlist(channel_id , playlist_id, playlist_name)
        owner_channel_ids_input  = return_ownerids_details(channel_id_input)
        for owner_channel_id_input in owner_channel_ids_input:
            Video_id , Video_name, views_cnt, likes_cnt, duration_min, comments_cnt, publish_time = return_video_details(owner_channel_id_input)
            insert_sql_video(Video_id , Video_name, views_cnt, likes_cnt, duration_min, comments_cnt, publish_time)
        playlist_ids, video_ids = return_playstid_videoid_details(channel_id_input)
        insert_sql_playlist_video_id_details(playlist_ids, video_ids)
        update_sql_playlist_video_id_details()
        st.write('Data Base insert Completed')
  
    final_result = st.button('Click Here For Data Analysis Results')
    if final_result == True:
        question1()
        question2()
        question3()
        question4()
        question5()
        question6()
        question7()
        question8()
        question9()
        question10()
