#!/usr/bin/python3

import sys
import json
import pandas as pd
import psycopg2
import json
from collections import Counter
from collections import defaultdict

HELP_TEXT = ('USAGE: \033[1mloader.py\033[0m dataset_base_path\n'
             + '\tdataset_base_path: path to the extracted movie dataset folder')

# dataset url:
# https://www.kaggle.com/rounakbanik/the-movies-dataset#movies_metadata.csv

# file names without base path
CREDITS = 'credits.csv'
MOVIES = 'movies_metadata.csv'
KEYWORDS = 'keywords.csv'
RATINGS = 'ratings.csv'  # use 'ratings_small.csv' if ratings are unimportant for you
DB_CONFIG_PATH = 'db_config.json'

# schemes of the database tables
TABLE_SCHEMA_FILE = 'db_schema.json'


def get_named_entity(x): return x.replace(' ', '_')


def is_valid_str(term):
    if type(term) == str:
        if len(term) > 0:
            return True


def is_positive_integer(number):
    try:
        value = int(number)
        return value > 0
    except:
        return False


def is_positive_float(number):
    try:
        value = float(number)
        return value > 0.0
    except:
        return False


def create_connection(db_config):
    con = None
    cur = None
    # create db connection
    try:
        con = psycopg2.connect(
            "dbname='" + db_config['db_name'] + "' user='"
            + db_config['username'] + "' host='" + db_config['host']
            + "' password='" + db_config['password'] + "'")
    except:
        print('ERROR: Can not connect to database')
        return
    cur = con.cursor()
    return con, cur


def disable_triggers(schema_info, con, cur):
    for table_name in schema_info.keys():
        cur.execute('ALTER TABLE ' + table_name + ' DISABLE trigger ALL;')
        con.commit()
    return


def enable_triggers(schema_info, con, cur):
    for table_name in schema_info.keys():
        cur.execute('ALTER TABLE ' + table_name + ' ENABLE trigger ALL;')
        con.commit()
    return


def create_schema(schema_info, con, cur):
    query_drop = "DROP TABLE IF EXISTS " + ', '.join(
        [key for key in schema_info]) + ';'
    queries_create = []
    for (name, schema) in schema_info.items():
        queries_create.append("CREATE TABLE " + name + " " + schema + ";")

    # run queries
    for query in [query_drop] + queries_create:
        cur.execute(query)
        con.commit()


# Takes the DataFrame from the movie file and extract all relevant information.
def extract_movie_data(df_movies):
    # define columns which information is useful
    RELEVANT_COLUMNS = [
        'id', 'original_title', 'belongs_to_collection', 'original_language',
        'spoken_languages', 'production_companies', 'production_countries',
        'release_date', 'genres', 'budget', 'popularity', 'revenue', 'runtime'
    ]

    # reduce data frame to relevant columns
    movies_reduced = df_movies[RELEVANT_COLUMNS]

    # extract data and create output dictonary
    extracted_movies = dict()
    extracted_genres = dict()
    extracted_languages_iso_key = dict()
    extracted_languages_id_key = dict()
    extracted_countries_iso_key = dict()
    extracted_countries_id_key = dict()
    extracted_collections = dict()
    extracted_production_companies = dict()

    current_lang_id = 0
    current_country_id = 0

    for line in movies_reduced.iterrows():
        # line[0]: line number  line[1]: content
        id = None
        try:
            id = int(line[1]['id'])
        except ValueError:
            print('Wrong id in:', line[1])
            continue
        # add simple values
        values = dict()
        values['title'] = line[1]['original_title']
        values['release_date'] = line[1]['release_date']
        values['budget'] = int(line[1]['budget']) if is_positive_integer(
            line[1]['budget']) else None
        values['popularity'] = float(
            line[1]['popularity']) if is_positive_float(
                line[1]['popularity']) else None
        values['revenue'] = int(line[1]['budget']) if is_positive_integer(
            line[1]['budget']) else None
        values['runtime'] = int(line[1]['runtime']) if is_positive_integer(
            line[1]['runtime']) else None

        # add entity values
        values['genres'] = set()
        for genre in eval(line[1]['genres']):
            if not genre['id'] in extracted_genres:
                extracted_genres[genre['id']] = {'name': genre['name']}
            if genre['id'] != None:
                values['genres'].add(genre['id'])

        values['collection'] = None
        if is_valid_str(line[1]['belongs_to_collection']):
            collection = eval(line[1]['belongs_to_collection'])
            if not collection['id'] in extracted_collections:
                extracted_collections[collection['id']] = {
                    'name': collection['name']
                }
            values['collection'] = collection['id']

        lang = line[1]['original_language']
        lang_id = None
        if not lang in extracted_languages_iso_key:
            entry = {'id': current_lang_id, 'key': lang, 'name': None}
            extracted_languages_id_key[current_lang_id] = entry
            extracted_languages_iso_key[lang] = entry
            lang_id = current_lang_id
            current_lang_id += 1
        else:
            lang_id = extracted_languages_iso_key[lang]['id']
        values['original_language'] = lang_id

        values['spoken_languages'] = set()
        if is_valid_str(line[1]['spoken_languages']):
            for lang in eval(line[1]['spoken_languages']):
                lang_id = None
                if not lang['iso_639_1'] in extracted_languages_iso_key:
                    entry = {
                        'id': current_lang_id,
                        'key': lang['iso_639_1'],
                        'name': lang['name']
                    }
                    extracted_languages_id_key[current_lang_id] = entry
                    extracted_languages_iso_key[lang['iso_639_1']] = entry
                    lang_id = current_lang_id
                    current_lang_id += 1
                else:
                    if extracted_languages_iso_key[lang['iso_639_1']]['name'] == None:
                        extracted_languages_iso_key[lang['iso_639_1']]['name'] = lang['name']
                    lang_id = extracted_languages_iso_key[lang['iso_639_1']][
                        'id']
                if lang_id != None:
                    values['spoken_languages'].add(lang_id)

        values['production_companies'] = set()
        if is_valid_str(line[1]['production_companies']):
            for company in eval(line[1]['production_companies']):
                if not company['id'] in extracted_production_companies:
                    extracted_production_companies[company['id']] = {
                        'name': company['name']
                    }
                if company['id'] != None:
                    values['production_companies'].add(company['id'])

        values['production_countries'] = set()
        if is_valid_str(line[1]['production_countries']):
            for country in eval(line[1]['production_countries']):
                country_id = None
                if not country['iso_3166_1'] in extracted_countries_iso_key:
                    entry = {
                        'id': current_country_id,
                        'key': country['iso_3166_1'],
                        'name': country['name']
                    }
                    extracted_countries_id_key[current_country_id] = entry
                    extracted_countries_iso_key[country['iso_3166_1']] = entry
                    country_id = current_country_id
                    current_country_id += 1
                else:
                    country_id = extracted_countries_iso_key[country[
                        'iso_3166_1']]['id']
                if country_id != None:
                    values['production_countries'].add(country_id)

        extracted_movies[id] = values

    return {
        'extracted_movies': extracted_movies,
        'extracted_genres': extracted_genres,
        'extracted_languages': extracted_languages_id_key,
        'extracted_countries': extracted_countries_id_key,
        'extracted_collections': extracted_collections,
        'extracted_production_companies': extracted_production_companies
    }


# Takes the DataFrame from the credits file and extract all relevant information.
def extract_credits_data(df_credits):
    # define columns which information is useful
    RELEVANT_COLUMNS = ['id', 'cast', 'crew']

    # reduce data frame to relevant columns
    credits_reduced = df_credits[RELEVANT_COLUMNS]

    # extract data and create output dictonary
    extracted_crew_data = dict()
    extracted_cast_data = dict()
    extracted_persons = dict()
    for line in credits_reduced.iterrows():
        movie_id = None
        try:
            movie_id = int(line[1]['id'])
        except ValueError:
            print('Wrong id in:', line[1])
            continue

        crew = dict()
        for person in eval(line[1]['crew']):
            if 'job' in person:
                if person['job'] in crew:
                    crew[person['job']].add(person['id'])
                else:
                    crew[person['job']] = set({person['id']})
            if not person['id'] in extracted_persons:
                extracted_persons[person['id']] = person['name']
        if len(crew.keys()) > 0:
            extracted_crew_data[movie_id] = crew

        cast = []
        for person in eval(line[1]['cast']):
            cast.append({'id': person['id'], 'order': person['order']})
            if not person['id'] in extracted_persons:
                extracted_persons[person['id']] = person['name']

        if len(cast) > 0:
            extracted_cast_data[movie_id] = cast

    return {
        'extracted_crew_data': extracted_crew_data,
        'extracted_persons': extracted_persons,
        'extracted_cast_data': extracted_cast_data
    }


def extract_keyword_data(df_keywords):
    # define columns which information is useful
    RELEVANT_COLUMNS = ['id', 'keywords']

    # reduce data frame to relevant columns
    keywords_reduced = df_keywords[RELEVANT_COLUMNS]

    # extract data and create output dictonary
    extracted_keywords = dict()
    for line in keywords_reduced.iterrows():
        # line[0]: line number  line[1]: content
        movie_id = None
        try:
            movie_id = int(line[1]['id'])
        except ValueError:
            print('Wrong movie id in:', line[1])
            continue
        for keyword in eval(line[1]['keywords']):
            if not keyword['id'] in extracted_keywords:
                extracted_keywords[keyword['id']] = {
                    'name': keyword['name'],
                    'movies': {movie_id}
                }
            else:
                extracted_keywords[keyword['id']]['movies'].add(movie_id)
    return extracted_keywords


def extract_rating_data(df_ratings):
    # define columns which information is useful
    RELEVANT_COLUMNS = ['movieId', 'rating']

    # reduce data to relevant columns
    ratings_reduced = df_ratings[RELEVANT_COLUMNS]

    # extract data and create output directory
    ratings_for_movies = defaultdict(list)
    count = 0
    for line in ratings_reduced.iterrows():
        count += 1
        if (count % 100000 == 0):
            print('Proceed %d ratings' % (count,))
        try:
            movie_id = int(line[1]['movieId'])
            rating = float(line[1]['rating'])
            ratings_for_movies[movie_id].append(rating)
        except ValueError:
            print('Problems with parsing: ', line)
    for key in ratings_for_movies:
        ratings_for_movies[key] = sum(
            ratings_for_movies[key]) / len(ratings_for_movies[key])
    return ratings_for_movies


def process_buffers(buffers, con, cur, batch_size):
    for buffer, query in buffers.values():
        if len(buffer) >= batch_size:
            cur.executemany(query, buffer)
            con.commit()
            buffer.clear()
    return


def flush_buffers(buffers, con, cur, batch_size):
    for buffer, query in buffers.values():
        cur.executemany(query, buffer)
        con.commit()
        buffer.clear()
    return


def get_db_literal(value):
    if value == None:
        return None
    else:
        return str(value)


def insert_movie_meta_data(data, rating_data, con, cur, batch_size):

    # query templates
    QUERY_INSERT_MOVIES = (
        "INSERT INTO movies (id, title, release_date, budget, "
        + "revenue, popularity, runtime, rating, original_language, "
        + "belongs_to_collection) VALUES %s")
    QUERY_INSERT_GENRES_RELATION = (
        "INSERT INTO movies_genres (movie_id, genre_id) VALUES %s")
    QUERY_INSERT_PRODUCTION_COMPANY_RELATION = (
        "INSERT INTO movies_production_companies (movie_id, production_company_id) VALUES %s"
    )
    QUERY_INSERT_PRODUCTION_COUNTRIES_RELATION = (
        "INSERT INTO production_countries (movie_id, country_id) VALUES %s")
    QUERY_INSERT_SPOKEN_LANGUAGES_RELATION = (
        "INSERT INTO spoken_languages (movie_id, language_id) VALUES %s")

    QUERY_INSERT_GENRES = "INSERT INTO genres (id, name) VALUES %s"
    QUERY_INSERT_COLLECTIONS = "INSERT INTO collections (id, name) VALUES %s"
    QUERY_INSERT_PRODUCTION_COMPANIES = "INSERT INTO production_companies (id, name) VALUES %s"
    QUERY_INSERT_COUNTRIES = "INSERT INTO countries (id, code, name) VALUES %s"
    QUERY_INSERT_LANGUAGES = "INSERT INTO languages (id, lang_key, name) VALUES %s"

    # query buffers
    buffers = {
        'movies_content': (list(), QUERY_INSERT_MOVIES),
        'genres_relation': (list(), QUERY_INSERT_GENRES_RELATION),
        'production_companies_relation':
        (list(), QUERY_INSERT_PRODUCTION_COMPANY_RELATION),
        'production_countries_relation':
        (list(), QUERY_INSERT_PRODUCTION_COUNTRIES_RELATION),
        'spoken_languages_relation': (list(),
                                      QUERY_INSERT_SPOKEN_LANGUAGES_RELATION)
    }

    movies_data = data['extracted_movies']

    for movie_id, movie_values in movies_data.items():
        # keys: 'title', 'release_date', 'budget', 'popularity', 'revenue',
        #       'runtime', 'genres', 'collection', 'original_language',
        #       'spoken_languages', 'production_companies', 'production_countries'
        # DB-Columns: id, title, release_date, budget, revenue, popularity,
        #             runtime, original_language, belongs_to_collection
        rating = rating_data[movie_id] if movie_id in rating_data else None
        buffers['movies_content'][0].append(
            [(str(movie_id), get_db_literal(movie_values['title']),
              get_db_literal(movie_values['release_date']),
              get_db_literal(movie_values['budget']),
              get_db_literal(movie_values['revenue']),
              get_db_literal(movie_values['popularity']),
              get_db_literal(movie_values['runtime']),
              get_db_literal(rating),
              get_db_literal(movie_values['original_language']),
              get_db_literal(movie_values['collection']))])
        for value in movie_values['genres']:
            buffers['genres_relation'][0].append([(str(movie_id), str(value))])
        for value in movie_values['production_companies']:
            buffers['production_companies_relation'][0].append([(str(movie_id),
                                                                 str(value))])
        for value in movie_values['production_countries']:
            buffers['production_countries_relation'][0].append([(str(movie_id),
                                                                 str(value))])
        for value in movie_values['spoken_languages']:
            buffers['spoken_languages_relation'][0].append([(str(movie_id),
                                                             str(value))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    genres_data = data['extracted_genres']
    buffers = {'genres_content': (list(), QUERY_INSERT_GENRES)}
    for genre_id, genre_values in genres_data.items():
        buffers['genres_content'][0].append([(str(genre_id),
                                              get_db_literal(
                                                  genre_values['name']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    collections_data = data['extracted_collections']
    buffers = {'collections_content': (list(), QUERY_INSERT_COLLECTIONS)}
    for collection_id, collection_values in collections_data.items():
        buffers['collections_content'][0].append(
            [(str(collection_id), get_db_literal(collection_values['name']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    production_companies_data = data['extracted_production_companies']
    buffers = {
        'production_companies_content': (list(),
                                         QUERY_INSERT_PRODUCTION_COMPANIES)
    }
    for comp_id, comp_values in production_companies_data.items():
        buffers['production_companies_content'][0].append(
            [(str(comp_id), get_db_literal(comp_values['name']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    countries_data = data['extracted_countries']
    buffers = {'countries_content': (list(), QUERY_INSERT_COUNTRIES)}
    for country_id, country_values in countries_data.items():
        buffers['countries_content'][0].append([(str(country_id),
                                                 get_db_literal(
                                                     country_values['key']),
                                                 get_db_literal(
                                                     country_values['name']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    languages_data = data['extracted_languages']
    buffers = {'languages_content': (list(), QUERY_INSERT_LANGUAGES)}
    for lang_id, lang_values in languages_data.items():
        buffers['languages_content'][0].append([(str(lang_id),
                                                 get_db_literal(
                                                     lang_values['key']),
                                                 get_db_literal(
                                                     lang_values['name']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    return


def print_all_jobs(extracted_crew_data):
    all_jobs = [
        item for movie_id, crew_data in extracted_crew_data.items()
        for item in crew_data
    ]
    print(Counter(all_jobs))
    return


def insert_credits_data(data, con, cur, batch_size):
    QUERY_INSERT_PERSONS = "INSERT INTO persons (id, name) VALUES %s"
    QUERY_INSERT_DIRECTORS = "INSERT INTO directors (movie_id, director_id) VALUES %s"
    QUERY_INSERT_ACTORS = "INSERT INTO actors (movie_id, person_id, order_id) VALUES %s"

    buffers = {'persons': (list(), QUERY_INSERT_PERSONS)}
    for person_id, person_name in data['extracted_persons'].items():
        buffers['persons'][0].append([(str(person_id),
                                       get_db_literal(person_name))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    buffers = {'directors': (list(), QUERY_INSERT_DIRECTORS)}
    for movie_id, crew_data in data['extracted_crew_data'].items():
        if 'Director' in crew_data:
            for person_id in crew_data['Director']:
                buffers['directors'][0].append([(str(movie_id),
                                                 str(person_id))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    buffers = {'actors': (list(), QUERY_INSERT_ACTORS)}
    for movie_id, cast_data in data['extracted_cast_data'].items():
        for person in cast_data:
            buffers['actors'][0].append([(str(movie_id), str(person['id']),
                                          get_db_literal(person['order']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    return


def insert_keywords(data, con, cur, batch_size):
    QUERY_INSERT_KEYWORDS = "INSERT INTO keywords (id, keyword) VALUES %s"
    QUERY_INSERT_MOVIES_RELATION = (
        "INSERT INTO movies_keywords (movie_id, keyword_id) VALUES %s")

    buffers = {
        'keywords': (list(), QUERY_INSERT_KEYWORDS),
        'movies_keywords': (list(), QUERY_INSERT_MOVIES_RELATION)
    }

    for keyword_id, values in data.items():
        buffers['keywords'][0].append([(str(keyword_id),
                                        get_db_literal(values['name']))])
        for movie_id in values['movies']:

            buffers['movies_keywords'][0].append([(str(movie_id),
                                                   str(keyword_id))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    return


def main(argc, argv):

    if argc != 2:
        print(HELP_TEXT)
        return

    dataset_base_path = argv[1] + '/'
    movies_path = dataset_base_path + MOVIES
    keywords_path = dataset_base_path + KEYWORDS
    credits_path = dataset_base_path + CREDITS
    ratings_path = dataset_base_path + RATINGS

    df_movies = pd.read_csv(movies_path)
    print('Read', df_movies.size, 'movies')
    df_credits = pd.read_csv(credits_path)
    print('Read', df_credits.size, 'credits')
    df_keywords = pd.read_csv(keywords_path)
    print('Read', df_keywords.size, 'keyword assignments')
    df_ratings = pd.read_csv(ratings_path)
    print('Read', df_ratings.size, 'ratings')

    print('Extract movie data from csv ...')
    extracted_movie_data = extract_movie_data(df_movies)
    print('Extract credits data from csv ...')
    extracted_credits_data = extract_credits_data(df_credits)
    print('Extract keywords data from csv ...')
    extracted_keywords = extract_keyword_data(df_keywords)
    print('Extract rating data from csv ...')
    extracted_ratings = extract_rating_data(df_ratings)

    print('Connect to database ...')
    f_db_config = open(DB_CONFIG_PATH, 'r')
    db_config = json.loads(f_db_config.read())
    f_db_config.close()

    con, cur = create_connection(db_config)

    batch_size = db_config['batch_size']

    # get schema
    print('Read schema file ...')
    schema_file = open(TABLE_SCHEMA_FILE, 'r')
    schema_info = json.load(schema_file)
    schema_file.close()
    print('Create Schema ...')
    create_schema(schema_info, con, cur)

    print('Insert data into database ...')
    disable_triggers(schema_info, con, cur)
    print('Insert movie meta data ...')
    insert_movie_meta_data(extracted_movie_data,
                           extracted_ratings, con, cur, batch_size)
    print('Insert credits data ...')
    insert_credits_data(extracted_credits_data, con, cur, batch_size)
    print('Insert keywords data ...')
    insert_keywords(extracted_keywords, con, cur, batch_size)
    enable_triggers(schema_info, con, cur)

    print('Done.')


if __name__ == "__main__":
    main(len(sys.argv), sys.argv)
