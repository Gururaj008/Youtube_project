# Youtube_project
Project Title : 	YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

Technologies	: Python scripting, Data Collection, MongoDB, Streamlit, API integration, Data Managment using MongoDB   
                                                 (Atlas) and SQL

Domain : 	Social Media

Problem Statement:
The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. 

Packages used in the project:
Pandas, Json, Streamlit, Plotly, Psycopg, Sqlalchemy

Steps involved in the project:

Set up a Streamlit app:
Used  Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.

Connect to the YouTube API: 
Used the YouTube API to retrieve channel and video data. 
Made an API call to the backend of YouTube and obtain channel, video and comment info for the chosen channel.

Store data in a MongoDB data lake: 
Upon retrieval the data, can be stored it in a MongoDB data lake with the click of a button. 

Migrate data to a SQL data warehouse: 
NoSQL data obtained is converted into pandas dataframe and later pushed to PostgreSQL tables.

Query the SQL data warehouse: 
Used SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. 

Display data in the Streamlit app: 
Used Streamlit's data visualization features to create charts and graphs to help users analyze the data.
