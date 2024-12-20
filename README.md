# Final Project: Big Data Analysis and Visualization with Databricks
# Description
This project demonstrates the process of extracting, cleaning, and analyzing a large dataset using Databricks, a powerful distributed big data application. The dataset is a csv file consists of over 104,905 rows and 8 columns, obtained through TMDB(the movie dtabase) API. The project follows a structured approach to data processing, organized into three layers: Bronze (raw data ingestion), Silver (data cleaning), and Gold (aggregated datasets and visualizations using Matplotlib).
# The primary objectives
- To showcase the capabilities of Databricks for handling large datasets.
- To implement best practices in data cleaning and analysis.
- To create insightful visualizations with Matplotlib based on the cleaned and aggregated.
# Key Technologies
- Databricks: For distributed data processing and analytics
- Apache Spark: For large-scale data processing
- DBFS (Databricks File System): For data storage
- Parquet: For optimized data storage format
- TMDB API: For data ingestion
- Python/PySpark: For data transformation and analysis
- Matplotlib/Seaborn: For data visualization.
# Data Collection Process
- API Data Fetching
- Parameters
Year Range: 2000 to 2010.
Pagination Limit: 500 movies per year
Total Records: Approximately 104905 movies with  relevant details: id, title, release_date, vote_average, popularity, vote_count, genre_ids, and original_language.
- Python Script for API Fetching: 
import requests
import csv
import os
from env import API_BASE_URL, API_KEY
endpoint = f'{API_BASE_URL}/discover/movie'
params = {'api_key': API_KEY, 'language': 'en-US', 'per_page': 40} import requests
import csv
import os
from env import API_BASE_URL, API_KEY
endpoint = f'{API_BASE_URL}/discover/movie'
params = {'api_key': API_KEY, 'language': 'en-US', 'per_page': 40} # API Setup

csv_filename = 'tmdb_API_movies_data_2000_2010.csv'
csv_headers = ['id', 'title', 'release_date', 'vote_average', 'popularity', 'vote_count', 'genre_ids', 'original_language']

with open(csv_filename, 'a', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file)
    if not os.path.isfile(csv_filename) or os.stat(csv_filename).st_size == 0:
        csv_writer.writerow(csv_headers)

    for year in range(2000, 2011):
        for page in range(1, 501):
            params['page'] = page
            params['primary_release_date.gte'] = f'{year}-01-01'
            params['primary_release_date.lte'] = f'{year}-12-31'
            response = requests.get(endpoint, params=params)
            if response.status_code == 200:
                for movie in response.json()['results']:
                    csv_writer.writerow([movie[field] for field in csv_headers])


csv_filename = 'tmdb_API_movies_data_2000_2010.csv'
csv_headers = ['id', 'title', 'release_date', 'vote_average', 'popularity', 'vote_count', 'genre_ids', 'original_language']

with open(csv_filename, 'a', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file)
    if not os.path.isfile(csv_filename) or os.stat(csv_filename).st_size == 0:
        csv_writer.writerow(csv_headers)

    for year in range(2000, 2011):
        for page in range(1, 501):
            params['page'] = page
            params['primary_release_date.gte'] = f'{year}-01-01'
            params['primary_release_date.lte'] = f'{year}-12-31'
            response = requests.get(endpoint, params=params)
            if response.status_code == 200:
                for movie in response.json()['results']:
                    csv_writer.writerow([movie[field] for field in csv_headers])
# Data Processing Layers
- Steps:
Intially upload the csv file in DBFS. Then copy the path of the csv file in dbfs.
- Initialize Spark session
spark = SparkSession.builder.appName("LoadCSV").getOrCreate()
- Load the CSV file into a DataFrame named df
df = (
spark.read.format("csv")
.option("header", True)
.option("inferSchema", True)
.option("nullValue", "null")
.load("dbfs:/FileStore/tmdb_API__movies_data_2000_2010.csv")
)
# Bronze Layer
- Ingestion: Data fetched via API and stored in DBFS.
- Schema Validation: Infer schema and validate column types.
- display total no of ows and columns.
# Silver Layer
- Standardization Process
- Clean column names.
- Remove duplicate rows.
- Replacing the emptygenre column values to unknown genre
- Convert release_date to standardized formats.
- Replace null values in key columns.
- Performance analysis based on popularity.
- Standard enconding
- Save as Parquet file in dbfs.
# Gold Layer
- Intially read the file from saves parquet file in siver layer . final_df = spark.read.parquet("dbfs:/Users/your_user/silver_layer_data")
- Aggregations and Visualization
- Transform Silver Layer data for analysis.
- Generate metrics like top-rated genres or yearly trends.
- Save as a Delta table in Spark SQL.
- final_df.write.format("delta").mode("overwrite").saveAsTable("tmbdApi_movies_data")
# Storage Paths
- Raw Data: dbfs:/FileStore/tmdb_API__movies_data_2000_2010.csv
- Silver Layer: dbfs:/Users/simhad76@students.rowan.edu/silver_layer_data
- Gold Layer: Table name: tmbdApi_movies_data
# Python Libraries
pandas.
matplotlib.pyplot .
seaborn.
re.
datetime.
# PySpark Functions
SparkSession.
SparkSession.builder.appName(). 
DataFrame and SQL Functions.
read.format("delta").table().
col().
count().
when().
lit().
isnan().
coalesce().
trim().
lower().
initcap().
split().
size().
substring().
current_timestamp().
# Python Data Manipulation
to_datetime().
value_counts().
groupby().
unstack().
# Matplotlib and Seaborn
plt.figure().
plt.barh(.)
plt.pie().
plt.plot().
plt.text().
sns.heatmap().

## Directory Structure

```bash
.
project/
├── data/
│   └── tmdb_API_movies_data_2000_2010.csv      # Dataset containing movie data from TMDB API.
├── notebooks/
│   ├── 01_data_collection.ipynb               # Databricks notebook for fetching data from the TMDB API.
│   ├── 02_bronze_layer.ipynb                  # Databricks notebook for loading and inspecting raw data.
│   ├── 03_silver_layer.ipynb                  # Databricks notebook for cleaning and transforming data.
│   └── 04_gold_layer.ipynb                    # Databricks notebook for creating aggregated datasets and visualizations.
├── scripts/
│   ├── API DATA.py      # Python notebook for fetching TMDB API data (if used externally).
│   └── visualization.ipynb                   # Notebook for generating visualizations programmatically.
├── config/
│   └── env.py                                 # Python file containing API keys and configuration parameters.
├── README.md                                  # Documentation explaining the project workflow.


```


## Running the Notebook in Databricks Community Edition

To run the notebook:

1. Open Databricks Community Edition:
 - Go to
```bash
[Databricks Community Edition](https://community.cloud.databricks.com/) and log in (or sign up if you haven't already).
 ```
- Navigate to your Workspace or where you want to upload the notebook.
2. Import the `.ipynb` Notebook:
- In Databricks, click on the "Workspace" section in the sidebar.
- Navigate to the folder where you want to import the notebook.
- Click the Import button at the top and select "Import File". Choose your `.ipynb` notebook file from your local machine.
- Once imported, the notebook will appear in your Databricks workspace.

3. Open the Notebook:
- Find and click on the newly imported notebook in your workspace to open it.

4. Execute the Notebook:
- Make sure to execute the cells in order to ensure dependencies between them are resolved (e.g., variables initialized in one cell are used in subsequent ones).




