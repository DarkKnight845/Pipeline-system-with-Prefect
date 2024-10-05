import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime
import numpy as np

from prefect import task, flow, Flow
from prefect.client.schemas.schedules import IntervalSchedule
from datetime import timedelta

import zipfile
import shutil

"""
Data Ingestion- Read movie data from google sheet using gsprad library
"""
def read_movie_data(movie_data):
    #  define scope and authentictate service account and share worrksheet to credential file 
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/drive']
    # Json file
    json_file = r"C:\Users\ayemi\Downloads\resolute-button-436012-j3-c8d859cdaa54.json"
    # access credential file
    credentials = Credentials.from_service_account_file(json_file, scopes=scopes)
    client = gspread.authorize(credentials)
    # Open google sheet(movie) to fetch all records
    sheet = client.open(movie_data).sheet1
    data = sheet.get_all_records()
    # return a dataframe of the google sheet movie file
    return pd.DataFrame(data)

MovieData = read_movie_data('movie')

# DATA CLEANING
"""
Clean data bby checking for different inconsitencies, mismathed data types, filterr data by voting avg
"""
def clean_data(MovieData):
    MovieData = MovieData.dropna() # drop row with missing values
    
    # MovieData = MovieData.drop(columns=['Unnamed: 0'])
    
    MovieData = MovieData[MovieData["vote_average"] >= 7.0]
    # MovieData = MovieData[MovieData["popularity"] > np.mean(MovieData["popularity"])]
    return MovieData


# DATA ANALYSIS
'''
Data Analysis function
The metric used to measure the success of a movie for me is popularity
This function returns top 10 movies based on my personalized metric-popularity
It also returns the average populararity rating of movies in this dataset
It also returns the movies released by year
'''
def analyze_data(MovieData):
    # top 10 movies by popularity
    top_10 = MovieData.nlargest(10, "popularity")
    # average movie rating by poularity
    avg_rating = np.mean(MovieData["popularity"])
    # movie released in each year
    movies_by_year = MovieData.groupby("release_date").size()
    # title of movies with their popularity rating
    movie_based_on_popularity = MovieData[["title", "popularity"]]

    return top_10, avg_rating, movies_by_year, movie_based_on_popularity


# DATA EXPORT
""" 
Exporting data into a csv file with time stamped date
"""
def export_to_csv(MovieData, filename="processed_movies"):
    timestamp = datetime.now().strftime('%Y_%m_%d')
    movie_file = f"{filename}_{timestamp}.csv"
    # move/export the cleaned data to local computer in csv format
    MovieData.to_csv(movie_file, index=False)
    return movie_file

# Using prefect to monetrize tasks and regullarize the flow of each tasks
@task(name="Read movie data")
def read_movie_data_task():
    # read movie data task
    return read_movie_data('movie')

@task(name="clean movie data")
def clean_movie_data_task(data):
    # clean movie dataset task
    return clean_data(data)

@task(name="analyze movie data")
def analyze_data_task(data):
    # analyzing movie dataset task
    return analyze_data(data)

@task(name="export data to csv")
def export_to_csv_task(data):
    # exporting task
    return export_to_csv(data)

@flow(name="Movie data processing")
def movie_data_processing():
    movie_data = read_movie_data_task()
    clean_movie_data = clean_movie_data_task(movie_data)
    top_10, avg_rating, movies_by_year, movie_based_on_popularity = analyze_data(movie_data)
    csv_file = export_to_csv_task(clean_movie_data)
    # schedule flow to run once everyday at midnight
    schedule = IntervalSchedule(interval=timedelta(days=1))
    flow.schedule = schedule

    return movie_data, clean_movie_data, top_10, avg_rating, movies_by_year, movie_based_on_popularity, csv_file

# Automating file system operations
def move_compressed_file(file_name, folder_path):
    # use shutil to move the movie file to folder_path/destinattion folder
    shutil.move(file_name, folder_path)

    # compress all files to a zip file
    zip_file_name = file_name.replace(".csv", ".zip")
    with zipfile.ZipFile(f"{folder_path}/{zip_file_name}", 'w') as zipf:
      zipf.write(f"{folder_path}/{file_name}", arcname=file_name)  

if __name__ == "__main__":
    movie_data_processing()
    move_compressed_file("processed_movies_2024_10_05", r"C:\Users\ayemi\OneDrive\Documents\Web Flask'" )