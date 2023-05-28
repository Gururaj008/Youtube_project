from googleapiclient.discovery import build
import streamlit as st
import re
import pandas as pd
import pymongo
import psycopg2
import plotly.express as px
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")
api_key_5 ='AIzaSyDgbNdzkZY-_g5MVlkRELsVAkavSos-A8M'
ytb = build('youtube','v3',developerKey=api_key_5)

def get_channel_stats(ytb,c_id):
    request = ytb.channels().list(part="snippet,contentDetails,statistics",id=c_id)
    response = request.execute()
    channel_data = dict(channel_id = c_id,
                        channel_name = response['items'][0]['snippet']['title'],
                        channel_type = response['items'][0]['kind'],
                        channel_view = response['items'][0]['statistics']['viewCount'],
                        subscribers_count = response['items'][0]['statistics']['subscriberCount'],
                        video_count = response['items'][0]['statistics']['videoCount'],
                        view_count = response['items'][0]['statistics']['viewCount'],
                        channel_description = response['items'][0]['snippet']['description'],
                        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    return channel_data

def get_video_ids(ytb, playlist_id):
    try:
        request = ytb.playlistItems().list(part = 'contentDetails',playlistId = playlist_id, maxResults=50)
        response = request.execute()
        video_ids= []
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])
        next_page_token = response['nextPageToken']
        more_pages = True

        while more_pages:
            if next_page_token is None:
                more_pages = False
            else:
                request = ytb.playlistItems().list(part = 'contentDetails',playlistId = playlist_id, 
                             maxResults=50,pageToken = next_page_token)
                response = request.execute()
                for i in range(len(response['items'])):
                    video_id = response['items'][i]['contentDetails']['videoId']
                    video_ids.append(video_id)
                next_page_token = response.get('nextPageToken')
    except:
        pass        
    return video_ids                 

def get_video_stats(ytb,video_id):
    try:
        request = ytb.videos().list(part="snippet,contentDetails,statistics",id=video_id)
        response = request.execute()
        video_data = dict( video_id = response['items'][0]['id'],
                            channel_id = response['items'][0]['snippet']['channelId'],
                            video_name = response['items'][0]['snippet']['title'],
                            video_description = response['items'][0]['snippet']['description'],
                            published_date = re.findall(r'(\d{4}-\d{2}-\d{2})',response['items'][0]['snippet']['publishedAt'])[0],
                            channel_title = response['items'][0]['snippet']['channelTitle'],
                            category_id = response['items'][0]['snippet']['categoryId'],
                            view_count = response['items'][0]['statistics']['viewCount'],
                            like_count = response['items'][0]['statistics']['likeCount'],
                            #favorite_count = response['items'][0]['statistics']['favoriteCount'],
                            comment_count = response['items'][0]['statistics']['commentCount'],
                            duration = response['items'][0]['contentDetails']['duration'],
                            thumbnail = response['items'][0]['snippet']['thumbnails']['default']['url'],
                            caption_status = response['items'][0]['contentDetails']['caption'])
    except:
        pass
    return video_data

def get_all_video_stats(video_ids):
    all_video_stats = []
    for i in video_ids:
        video_stats = get_video_stats(ytb,i)
        all_video_stats.append(video_stats)
    return all_video_stats

def get_comment_details(ytb,video_id):
    try:
        comments_data = []
        request = ytb.commentThreads().list(part="snippet,replies",videoId=video_id)
        response = request.execute()
        for i in range(len(response['items'])):
            comment_data = dict(video_id = response['items'][i]['snippet']['videoId'],
                                comment_id = response['items'][i]['id'],
                                comment_text = response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                                comment_author = response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                published_date = re.findall(r'(\d{4}-\d{2}-\d{2})',response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'])[0])
            comments_data.append(comment_data)
    except:
        pass
    return comments_data

def get_all_comments_stats(video_ids):
    all_comments_stats = []
    for i in video_ids:
        comments_stats = get_comment_details(ytb,i)
        all_comments_stats.append(comments_stats)
    return all_comments_stats

def create_channel_df(channel_data):
    df_channel_data = pd.DataFrame(channel_data,index=[0])
    df_channel_data['channel_id'] = df_channel_data['channel_id'].astype(str)
    df_channel_data['channel_name'] = df_channel_data['channel_name'].astype(str)
    df_channel_data['channel_type'] = df_channel_data['channel_type'].astype(str)
    df_channel_data['channel_view'] = df_channel_data['channel_view'].astype(int)
    df_channel_data['subscribers_count'] = df_channel_data['subscribers_count'].astype(int)
    df_channel_data['video_count'] = df_channel_data['video_count'].astype(int)
    df_channel_data['view_count'] = df_channel_data['view_count'].astype(int)
    df_channel_data['channel_description'] = df_channel_data['channel_description'].astype(str)
    df_channel_data['playlist_id'] = df_channel_data['playlist_id'].astype(str)
    return df_channel_data

def create_video_df(video_data):
    df_video_data = pd.DataFrame(video_data)
    df_video_data['video_id'] = df_video_data['video_id'].astype(str)
    df_video_data['channel_id'] = df_video_data['channel_id'].astype(str)
    df_video_data['video_name'] = df_video_data['video_name'].astype(str)
    df_video_data['video_description'] = df_video_data['video_description'].astype(str)
    df_video_data['published_date'] = pd.to_datetime(df_video_data['published_date']) 
    df_video_data['channel_title'] = df_video_data['channel_title'].astype(str)
    df_video_data['category_id'] = df_video_data['category_id'].astype(str)
    df_video_data['view_count'] = df_video_data['view_count'].astype(int)
    df_video_data['like_count'] = df_video_data['like_count'].astype(int)
    #df_video_data['favorite_count'] = df_video_data['favorite_count'].astype(int)
    df_video_data['comment_count'] = df_video_data['comment_count'].astype(int)
    df_video_data['duration'] = pd.to_timedelta(df_video_data['duration'])
    df_video_data['duration'] = df_video_data['duration'].astype(str)
    df_video_data['thumbnail'] = df_video_data['thumbnail'].astype(str)
    df_video_data['caption_status'] = df_video_data['caption_status'].astype(str)
    return df_video_data

def create_comment_df(comments_data):
    df_comments_data = pd.DataFrame(comments_data)
    df_comments_data['video_id'] = df_comments_data['video_id'].astype(str)
    df_comments_data['comment_id'] = df_comments_data['comment_id'].astype(str)
    df_comments_data['comment_text'] = df_comments_data['comment_text'].astype(str)
    df_comments_data['comment_author'] = df_comments_data['comment_author'].astype(str)
    df_comments_data['published_date'] = pd.to_datetime(df_comments_data['published_date'])
    return df_comments_data

def get_channel_info():
    channel_data = get_channel_stats(ytb,c_id)
    df_channel_data = create_channel_df(channel_data)
    playlist_id = channel_data['playlist_id']
    video_ids = get_video_ids(ytb, playlist_id)
    video_data = get_all_video_stats(video_ids)
    df_video_data = create_video_df(video_data)
    comments_data_1 = get_all_comments_stats(video_ids)
    comments_data = sum(comments_data_1, [])
    df_comments_data = create_comment_df(comments_data)
    channel_information = dict (channel_info =channel_data, video_info = video_data,comments_info = comments_data)
    return df_channel_data, df_video_data, df_comments_data, channel_information

def question1():
    engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
    pd.set_option('display.max_rows', None)
    df = pd.read_sql_query('SELECT * from video', engine)
    df_2 = df[['channel_title','video_name']]
    df_2 = df_2.sort_values(by='channel_title', ascending = True)
    df_2 = df_2.reset_index(drop = True)
    return df_2

def question2():
        engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
        df = pd.read_sql_query('SELECT * from video', engine)
        df_2 = df.groupby('channel_title')['video_id'].count().sort_values(ascending = False)
        df_res = df_2.reset_index()
        df_res = df_res.rename(columns={'channel_title':'channel_title','video_id':'video_count'})
        return df_res

def question3():
    engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
    df = pd.read_sql_query('SELECT * from video', engine)
    df_2 = df.sort_values(by='view_count', ascending = False)
    df_2 = df_2[['channel_title','video_name','view_count']]
    df_2 = df_2.reset_index(drop= True)
    return df_2.head(10)

def question4():
    engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
    df_1 = pd.read_sql_query('SELECT * from video', engine)
    df_2 = pd.read_sql_query('SELECT * from comment', engine)
    dummy = df_2.groupby('video_id')['comment_id'].count()
    df_new = dummy.reset_index()
    df_new = df_new.rename(columns={'video_id':'video_id','comment_id':'comment_count'}).sort_values(by='comment_count', ascending =False)
    df_new = df_new.reset_index(drop=True)
    df_new['video_name'] = ''
    for i in range(len(df_new)):
        for j in range(len(df_1)):
            if df_new['video_id'][i] == df_1['video_id'][j]:
                df_new['video_name'][i] = df_1['video_name'][j]
    return df_new

def question5():
    engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
    df_1 = pd.read_sql_query('SELECT * from video', engine)
    df_new = df_1.sort_values(by='like_count', ascending = False)
    df_res = df_new[['video_name','channel_title','like_count']].head(10)
    df_res = df_res.reset_index(drop = True)
    return df_res

def question6():
    engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
    pd.set_option('display.max_rows', None)
    df_1 = pd.read_sql_query('SELECT * from video', engine)
    df_new = df_1[['video_name','like_count']]
    df_new = df_new.sort_values(by='like_count', ascending = False)
    df_new = df_new.reset_index(drop = True)
    return df_new


def question7():
    engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
    df_1 = pd.read_sql_query('SELECT * from channel', engine)
    df_new = df_1.sort_values(by='channel_view', ascending = False)
    df_res = df_new.reset_index(drop = True)
    return df_res[['channel_name', 'channel_view']]


def question8():
    engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
    df = pd.read_sql_query('SELECT * from video', engine)
    df['published_date'] = pd.to_datetime(df['published_date'])
    df['Year'] = df['published_date'].dt.year
    df = df[df['Year'] == 2022]
    df_2 = pd.value_counts(df['channel_title'])
    df_res = pd.DataFrame(df_2).reset_index()
    df_res = df_res.rename(columns={'index':'channel_title','channel_title':'videos_uploaded_in_2022'})
    return df_res


def question9():
    engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
    df = pd.read_sql_query('SELECT * from video', engine)
    df['duration'] = df['duration'].astype(str)
    df['new_duration'] = ''
    for i in range(len(df['duration'])):
        df['new_duration'][i] = str(df['duration'][i]).split(' ')[2]
    df['new_duration'] = pd.to_datetime(df['new_duration'])
    df['final_duration'] = pd.to_datetime(df['new_duration']).dt.strftime('%H:%M:%S')
    df['final_duration'] = pd.to_timedelta(df['final_duration'])
    df_new = df.groupby('channel_title')['final_duration'].mean()
    df_new = df_new.reset_index()
    df_res = df_new.rename(columns={'channel_title':'channel_title','final_duration':'average_duration'})
    df_res = df_res.sort_values(by='average_duration', ascending = False)
    df_res = df_res.reset_index(drop = True)
    return df_res


def question10():
    engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
    df_1 = pd.read_sql_query('SELECT * from video', engine)
    df_2 = pd.read_sql_query('SELECT * from comment', engine)
    dummy = df_2.groupby('video_id')['comment_id'].count()
    df_new = dummy.reset_index()
    df_new = df_new.rename(columns={'video_id':'video_id','comment_id':'comment_count'}).sort_values(by='comment_count', ascending =False)
    df_new = df_new.reset_index(drop=True)
    df_new['channel_title'] = ''
    df_new['video_title'] = ''
    for i in range(len(df_new)):
        for j in range(len(df_1)):
            if df_new['video_id'][i] == df_1['video_id'][j]:
                df_new['channel_title'][i] = df_1['channel_title'][j]
                df_new['video_title'][i] = df_1['video_name'][j]
    df_res = df_new[['video_id','video_title','channel_title','comment_count']]
    return df_res.head(50)

if __name__=='__main__':
    st.set_page_config(layout="wide")
    col1, col2, col3 = st.columns([1,8,1])
    with col2:
        st.title(':blue[Youtube data harvesting and warehousing]')
    st.write('')
    st.write('')
    st.subheader(":orange[**About the project**]")
    st.write('')
    st.write('')
    st.markdown('<div style="text-align: justify"> YouTube is a widely popular online video-sharing platform owned by Google, allowing users to upload, watch, and share videos. With over 2 billion logged-in monthly active users, YouTube has a global reach and supports multiple languages. It receives an astonishing amount of video content, with approximately 500 hours of video uploaded per minute, resulting in over 1 billion hours of daily video views. YouTube\'s influence on culture and society is significant, providing a platform for content creators to share knowledge, entertainment, and diverse perspectives. It has enabled individuals to build successful careers and monetize their content through ads, sponsorships, and partnerships. YouTube has also played a crucial role in supporting educational initiatives, activism, and community engagement. Additionally, YouTube offers a premium subscription service called YouTube Premium, which provides ad-free viewing, offline access, and exclusive original content. Overall, YouTube continues to shape the digital landscape, impacting how we consume and interact with video content. </div>', unsafe_allow_html=True)
    st.write('')
    st.markdown('<div style="text-align: justify"> Data harvesting, in simple terms, refers to the process of collecting or gathering large amounts of data from various sources, often using automated methods or tools. It involves extracting, compiling, and storing data from different websites, databases, or digital platforms. Data harvesting can be performed for various purposes, such as market research, data analysis, creating databases, or building machine learning models. The collected data may include information like text, images, videos, user profiles, product details, or any other relevant data points depending on the objective. </div>', unsafe_allow_html=True)
    st.write('')
    st.markdown('<div style="text-align: justify"> Data warehousing refers to the process of collecting, organizing, and storing large volumes of structured and/or unstructured data from various sources into a centralized repository. It involves combining data from different operational systems, databases, and external sources to create a comprehensive and integrated view of an organization\'s data. The purpose of a data warehouse is to provide a unified, consistent, and historical view of data that can be used for business intelligence, analytics, reporting, and decision-making. It serves as a central repository where data from multiple sources is extracted, transformed, and loaded (ETL) to ensure consistency, quality, and accessibility. </div>', unsafe_allow_html=True)
    st.write('')
    st.markdown('<div style="text-align: justify"> The objective of this project is to make API calls to YouTube and fetch channels, videos and comments information. User is provided with 15 different channel ids, out of which he/she can choose upto 10 channel ids for information retrieval.  The data retrieved is in NoSQL format, which is displayed and user is provided with option to push the data to MongDB and PostgreSQL. Later, SQL queries are made to retrieve, display and visualize some interesting facts about the channels, videos and comments. </div>', unsafe_allow_html=True)
    st.divider()
    tab1, tab2 = st.tabs(["Fetch and upload the channel details", "Analyse and visualise the channel details"])
    with tab1:
        col4,col5,col6 = st.columns([1,6,1])
        with col5:
            st.header(':orange[out of 15, you can select upto 10 channels to fetch the details regarding channel, videos and comments  ]')
            st.write('')
            st.write('')
            option = st.selectbox('Select a channelid',
                                ('UCQqmjKQBKQkRbWbSntYJX0Q','UCgv5dpyaTyS9Zx9n2HZntLA','UCK4u0NaAHtxhsXT2dii53hA',
                                'UCqs6PlKh57qAeHHJOZQZp_g','UCzMAHa94JN27woC7XP1nveA','UCUrhXJKb-kUiuRF4gbky40A',
                                'UCiXoCpQkpR9E2UnT9nnj7AA','UCSN91fnT-LblYx-sdiazmdA' ,'UC3WabT_iJFpTyhPa4i0lmmg',
                                'UCUvf-PKuKSvUTqpw-WWwHaQ','UCKA95NHDi4gkizwYtljYdbw','UCr9OLp62JB_vnbxdiNYufMg',
                                'UCL_XDU60cGJxlWastwGro8w','UCJx3m2bt4QdAQ1dZ-CREomw','UCkzo5BM3sJFhgR5NFzT9dwA'
                                ))
            c_id = option
            st.write('')
            st.write('')
            col7, col8, col9 = st.columns([2,5,2])
            with col8:
                if st.button('Fetch the channel, video and comment details for the chosen channel-id'):
                    st.write('Hang on.......')  
                    df_channel_data,df_video_data, df_comment_data, channel_data = get_channel_info()
                    st.write(channel_data['channel_info'])
                st.write('')
                st.write('')
                st.write('')
                if st.button('Push the above channel, video and comment data to MongoDB'):
                    st.write('Pushing........')
                    df_channel_data,df_video_data, df_comment_data, channel_data = get_channel_info()
                    client = pymongo.MongoClient("mongodb://gururaj008:mongo12345@ac-fytzrey-shard-00-00.gogkstt.mongodb.net:27017,ac-fytzrey-shard-00-01.gogkstt.mongodb.net:27017,ac-fytzrey-shard-00-02.gogkstt.mongodb.net:27017/?ssl=true&replicaSet=atlas-4xbsfj-shard-0&authSource=admin&retryWrites=true&w=majority")
                    db = client['ytb_database']
                    coll = db['ytb_collection']
                    coll.insert_one(channel_data)
                    st.markdown(':green[**Data successfully pushed to MongoDB**]')
                st.write('')
                st.write('')
                st.write('')
                if st.button('Push the above channel, video and comment data to PostgreSQL'):
                    st.write('Pushing........')
                    df_channel_data,df_video_data, df_comment_data, channel_data = get_channel_info()
                    engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
                    df_channel_data.to_sql('channel', engine, if_exists='append', index=False)
                    df_video_data.to_sql('video', engine, if_exists='append', index=False)
                    df_comment_data.to_sql('comment', engine, if_exists='append', index=False)
                    st.markdown(':green[**Data successfully pushed to PostgreSQL**]')
                st.write('')
                st.write('')
                st.markdown(':orange[**Lets move to the** __Analyse and visualize channel details__ **tab**]')
        with tab2:
            st.subheader(':orange[**Welcome to youtube channel data analysis and visualization page**]')
            st.write('')
            st.markdown(':green[**Names of all the videos and their corresponding channels**]')
            if st.button('Fetch the details....',key=4):
                dataframe = question1()
                st.table(dataframe)
            st.write('')
            st.write('')
            st.markdown(':green[**Channels along with the number of videos**]')
            if st.button('Fetch the details....',key=5):
                dataframe = question2()
                col10,col11 = st.columns([2,6])
                with col10:
                    st.table(dataframe)
                with col11:
                    fig = px.bar(dataframe,color='channel_title',width=950, height=450)
                    st.write(fig)
            st.write('')
            st.write('')
            st.markdown(':green[**Top 10 most viewed videos with thier respective channels**]')
            if st.button('Fetch the details....',key=6):
                dataframe = question3()
                col12,col13 = st.columns([3,2])
                with col12:
                    st.table(dataframe)
                with col13:
                    fig = px.pie(values=dataframe['view_count'], names=dataframe['video_name'],width=500, height=500)
                    fig.update_layout(showlegend=False)
                    st.write(fig)
            st.write('')
            st.write('')
            st.markdown(':green[**Number of comments made on each video and their corresponding video names**]')
            if st.button('Fetch the details....',key=7):
                dataframe = question4()
                st.table(dataframe)
            st.write('')
            st.write('')
            st.markdown(':green[**Top 10 videos with highest number of likes, and their corresponding channel names**]')
            if st.button('Fetch the details....',key=8):
                dataframe = question5()
                col14,col15 = st.columns([3,3])
                with col14:
                    st.table(dataframe)
                with col15:
                    df = dataframe[['video_name','like_count']]
                    fig = px.bar(df,color='video_name',width=1000, height=450)
                    st.write(fig)
            st.write('')
            st.write('')
            st.markdown(':green[**Total number of likes for each video and their corresponding video names**]')
            if st.button('Fetch the details....',key=9):
                dataframe = question6()
                st.table(dataframe)
            st.write('')
            st.write('')
            st.markdown(':green[**Total number of views for each channel and their corresponding channel names**]')
            if st.button('Fetch the details....',key=10):
                dataframe = question7()
                col16,col17 = st.columns([2,2])
                with col16:
                    st.table(dataframe)
                with col17:
                    fig = px.pie(values=dataframe['channel_view'], names=dataframe['channel_name'],width=500, height=500)
                    fig.update_layout(showlegend=False)
                    st.write(fig)
            st.write('')
            st.write('')
            st.markdown(':green[**Names of all the channels that have published videos in the year 2022**]')
            if st.button('Fetch the details....',key=11):
                dataframe = question8()
                col14,col15 = st.columns([3,3])
                with col14:
                    st.table(dataframe)
                with col15:
                    fig = px.bar(dataframe,color='channel_title',width=1000, height=450)
                    st.write(fig)
            st.write('')
            st.write('')
            st.markdown(':green[**Average duration of all videos in each channel and their corresponding channel names**]')
            if st.button('Fetch the details....',key=12):
                dataframe = question9()
                dataframe_2 = dataframe.copy()
                col12,col13 = st.columns([2,2])
                with col12:
                    dataframe_2['average_duration'] = dataframe_2['average_duration'].astype(str)
                    st.table(dataframe_2)
                with col13:
                    fig = px.pie(values=dataframe['average_duration'], names=dataframe['channel_title'],width=500, height=500)
                    fig.update_layout(showlegend=False)
                    st.write(fig)
            st.write('')
            st.write('')
            st.markdown(':green[**Videos with highest number of comments and their corresponding channel names**]')
            if st.button('Fetch the details....',key=13):
                dataframe = question10()
                st.table(dataframe)

st.divider()    
st.subheader(':orange[About the developer]')
st.write('')
st.markdown('<div style="text-align: justify">Gururaj H C is passionate about Machine Learning and fascinated by its myriad real world applications. Possesses work experience with both IT industry and academia. Currently pursuing “IIT-Madras Certified Advanced Programmer with Data Science Mastery Program” course as a part of his learning journey.  </div>', unsafe_allow_html=True)
st.divider()
st.markdown('_An effort by_ :blue[**MAVERICK_GR**]')
st.markdown(':green[**DEVELOPER CONTACT DETAILS**]')
st.markdown(":orange[email id:] gururaj008@gmail.com")
st.markdown(":orange[Personal webpage hosting other Datascience projects :] http://gururaj008.pythonanywhere.com/")
st.markdown(":orange[LinkedIn profile :] https://www.linkedin.com/in/gururaj-hc-machine-learning-enthusiast/")
st.markdown(":orange[Github link:] https://github.com/Gururaj008 ")

            
          




    
    
    
    
    
    
    
    
    
    
    
    

















    # c_id = 'UCUvf-PKuKSvUTqpw-WWwHaQ'
    # df_channel_data,df_video_data, df_comment_data, channel_data = get_channel_info()
    # st.write(channel_data['channel_info'])
    # if st.button('Upload to mongoDb'):
    #    client = pymongo.MongoClient("mongodb://gururaj008:mongo12345@ac-fytzrey-shard-00-00.gogkstt.mongodb.net:27017,ac-fytzrey-shard-00-01.gogkstt.mongodb.net:27017,ac-fytzrey-shard-00-02.gogkstt.mongodb.net:27017/?ssl=true&replicaSet=atlas-4xbsfj-shard-0&authSource=admin&retryWrites=true&w=majority")
    #    db = client['ytb_database']
    #    coll = db['ytb_collection']
    #    coll.insert_one(channel_data)

    # if st.button('Push to PostgreSQL'):
    #     engine = create_engine('postgresql://postgres:Postgres123$@localhost:5432/youtube_db')
    #     df_channel_data.to_sql('channel', engine, if_exists='append', index=False)
    #     df_video_data.to_sql('video', engine, if_exists='append', index=False)
    #     df_comment_data.to_sql('comment', engine, if_exists='append', index=False)
    