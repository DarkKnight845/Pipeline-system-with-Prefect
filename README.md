# **Movie Data Processing Automation with Prefect and GSpreead**    
## **Overview**     
The purpose of this week 14 project is to automate mmovie data processing tasks using Prefect and Gspread, allowing one to orchestrate data workflow. The entire workflow involves **reading task**, **cleaning task**, **analyzing task**, **exporting processied movie dataset**. The tasks are scheduleed to rrun periodically, and the processed daata is exported to a CSV fille on the local machine. The project also performs filee system operations such as renaming, moving and compressing the processed files.     

## **Data Processing Steps**    
### Data Ingestion
1. Read movie data from google sheet using GSpread library  
2. Task: ```Read movie data```     

### Data cleaning   
1. Remove rows with missing values  
2. Filter the movies dataset by vote avg > 7.0 
3. Convert columns with mismatched data types to the the right data types   
4. Task: ```clean movie data```

### Data Analysis   
1. Find the top 10 popular moovies  
2. Get the avergae rating of popular movies 
3. Get movie titles with their individual release date  
4. Task: ```analyze movie data``` 

### Data Export     
1. Save the processed data into a csv file  
2. Processed data is named with a timestamp     
3. Task: ```export data to csv```     

## Flow     
The data processing is managed using Prefect.Every task is now orchestrated into one work flow(```Movie Data Processing```). The flow is also scheduled to run at a specific interval using Prefect's scheduling functionality. The work flow is structured in this manner:   
```shell
@flow(name="Movie data processing")
def movie_data_processing():
    movie_data = read_movie_data_task()
    clean_movie_data = clean_movie_data_task(movie_data)
    top_10, avg_rating, movies_by_year, movie_based_on_popularity = analyze_data(movie_data)
    csv_file = export_to_csv_task(clean_movie_data)

    schedule = IntervalSchedule(interval=timedelta(days=1))
    flow.schedule = schedule

    return movie_data, clean_movie_data, top_10, avg_rating, movies_by_year, movie_based_on_popularity, csv_file
```

## Fily system operation    
After all data processing tasks and flow are complete, a funvtion is written to perform the following:  
1. ```Rename the file```  
2. ```Move the file to a new folder```    
3. ```Compress the file using zipfile module```   

## Libraries    
1. GSpread: For Automating Gooogle sheets   
2. Prefect: For workflow orchestration  
3. Pandas - For Data manipulation   
4. Numpy: For a numerical computation such as mean  
5. Datime: for creating timestampp for dataset  
6. Shutil: For movuing processed data to new folder path    

```Source of Dataset: Kaggle```