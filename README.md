# YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

Problem Statement
Creating Streamlit application that allows users to give YouTube channel Details and provides various insights on the data and channels. Using MongoDB as Data Ware house to store all the API related information and then into MSSQL DB and generate various visualisations and Tables 


# Languages and technologies used:

Python scripting, 

Data Collection,

MongoDB, 

Streamlit, 

Data Visualization,

API integration, 

Data Managment using MongoDB  and SQL


# Code Overview:

1	We have to setup the google API configuration to accessing Youtube API. And create creadential data file and made of the google API code to fetch the data related to youtube channel

2	Streamlet configuration is done and it is good choice for building data visualization and analysis tools easily. Users can enter a YouTube channel ID, Then YouTube API is called and channel and video data is retrieved with help of Google API client library for Python to make requests to the API.

3	Retrieved data from the YouTube API is stored in MongoDB as Data Warehouse MongoDB is a great choice as it can handle unstructured and semi-structured data easily. And we can find the required documents from collections easily 

4	Then once user clicks on migrate to SQL database button the required data is taken from mongodb and stored in MYSQL database which makes us easy to query and get required insights on data

5	Finally, the retrieved data in the Streamlit app. Snf Streamlit's data visualization features to create bar graphs to help users analyze the data.

On a bird’s eye view project involves building a simple UI with Streamlit, retrieving data from the YouTube API, and store Response in MongoDB and migrating it to a MSSQL database and querying DB, and displaying the data in the Streamlit app.
