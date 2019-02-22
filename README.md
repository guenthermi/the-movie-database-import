# the-movie-database-import
This script to import data from the The Movie Database (Data URL: https://www.kaggle.com/rounakbanik/the-movies-dataset) to a PostgreSQL database. It creates 15 tables containing information about movies, keywords, production companies, production countries, actors as well as credits data.

# Run the Skript
In order to run the script you have to download and extract the datset available at https://www.kaggle.com/rounakbanik/the-movies-dataset.
The script uses the `movies_metadata.csv`, `credits.csv` and `keywords.csv` file from the dataset.

Then you have to define the *database connection information* in `db_config.json`.

Afterwards you can run the `loader.py` to import the data to your Postgres database
```
python3 loader.py path/to/your/dataset/folder
```
