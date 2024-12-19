import requests
import csv
import os
from env import API_BASE_URL, API_KEY

# API setup
endpoint = f'{API_BASE_URL}/discover/movie'

# Parameters for the request
params = {
    'api_key': API_KEY,
    'language': 'en-US',
    'per_page': 40,
}

# Prepare the CSV file to save the data (in append mode)
csv_filename = 'tmdb_API_movies_data_2000_2010.csv'
csv_headers = ['id', 'title', 'release_date', 'vote_average', 'popularity', 'vote_count', 'genre_ids',
               'original_language']

# Check if the file exists and initialize row count
file_exists = os.path.isfile(csv_filename)
total_rows = 0

# Open the CSV file for appending
with open(csv_filename, 'a', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file)

    # If the file doesn't exist or is empty, write the headers
    if not file_exists or os.stat(csv_filename).st_size == 0:
        csv_writer.writerow(csv_headers)

    # Loop through years from 2000 to 2010
    start_year = 2000
    end_year = 2010
    for year in range(start_year, end_year + 1):
        print(f"Fetching data for the year {year}...")

        # Set date filters for the current year
        params['primary_release_date.gte'] = f'{year}-01-01'
        params['primary_release_date.lte'] = f'{year}-12-31'

        # Loop through pages
        for page in range(1, 501):
            print(f"Fetching page {page} for year {year}...")

            # Update the page number in parameters
            params['page'] = page

            # Make the GET request
            response = requests.get(endpoint, params=params)

            # Check for a successful response
            if response.status_code == 200:
                data = response.json()

                # Loop through the results and write each movie to the CSV file
                for movie in data['results']:
                    csv_writer.writerow([
                        movie['id'],
                        movie['title'],
                        movie['release_date'],
                        movie['vote_average'],
                        movie['popularity'],
                        movie['vote_count'],
                        movie['genre_ids'],
                        movie['original_language']
                    ])
                    total_rows += 1

            else:
                print(f"Error: {response.status_code} - {response.text}")
                break

print(f"Data has been downloaded and saved to '{csv_filename}'. Total rows: {total_rows}.")
