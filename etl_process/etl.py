import psycopg2
from sqlalchemy import create_engine

import pandas as pd
import numpy as np

import requests

import random
import logging

logging.getLogger().setLevel(logging.INFO)

"""

EXTRACT

"""


def data_extraction(locations: list) -> pd.DataFrame:
    # When iterating through the cities to invoke the API for each city, the data will be appended in these lists
    ids = []
    cities = []
    avg_ratings = []
    star_ratings = []
    reviews_counts = []
    listing_obj_types = []
    space_types = []
    room_type_categories = []
    listing_guest_labels = []
    bathrooms = []
    bedrooms = []
    beds = []
    check_ins = []
    check_outs = []
    are_super_hosts = []
    images = []
    prices = []

    # The maximum number of records that can be collected with a single API call is 40
    total_records_per_api_call = "40"

    # The currency of the accommodation prices
    currency = "EUR"

    # The Airbnb API URL
    url = "https://airbnb19.p.rapidapi.com/api/v1/searchPropertyByPlace"

    headers = {
        "X-RapidAPI-Key": "dbfa51d40dmsh4dbbdee2ca0e4d1p1edaecjsn6b284911b39d",  # insert your api key
        "X-RapidAPI-Host": "airbnb19.p.rapidapi.com"
    }

    for loc in locations:
        querystring = {
            "id": loc,
            "totalRecords": total_records_per_api_call,
            "currency": currency}

        try:
            response = requests.request("GET", url, headers=headers, params=querystring)
            data = response.json()
            # print(data)
            for accommodation in data["data"]:
                ids.append(accommodation["id"])
                cities.append(accommodation["city"])
                avg_ratings.append(accommodation["avgRating"])
                star_ratings.append(accommodation["starRating"])
                reviews_counts.append(accommodation["reviewsCount"]),
                listing_obj_types.append(accommodation["listingObjType"]),
                space_types.append(accommodation["spaceType"]),
                room_type_categories.append(accommodation["roomTypeCategory"]),
                listing_guest_labels.append(accommodation["listingGuestLabel"]),
                bathrooms.append(accommodation["bathrooms"]),
                bedrooms.append(accommodation["bedrooms"]),
                beds.append(accommodation["beds"]),
                check_ins.append(accommodation["checkin"]),
                check_outs.append(accommodation["checkout"]),
                are_super_hosts.append(accommodation["isSuperhost"]),
                images.append(random.choice(accommodation["images"]))

                try:
                    prices.append(accommodation["price"])
                    # logging.info("Trying to get the price from the Json object: " + accommodation["price"])
                except:
                    prices.append(accommodation["originalPrice"])
                    # logging.info("The price key is not present, thus let's take the key Original price: " + accommodation["originalPrice"])
                    continue

            logging.info(f"Acquired data for accommodations in {loc}")

        except Exception as error:
            logging.error("Check if the number of free monthly API calls is exceeded.")
            print(error)
            break

    # Creating a dictionary from that will contain the filled lists as values
    accommodations_dict = {
        "id": ids,
        "city": cities,
        "avg_rating": avg_ratings,
        "star_rating": star_ratings,
        "reviews_count": reviews_counts,
        "listing_obj_type": listing_obj_types,
        "space_type": space_types,
        "room_type_category": room_type_categories,
        "listing_guest_label": listing_guest_labels,
        "bathrooms": bathrooms,
        "bedrooms": bedrooms,
        "beds": beds,
        "check_in": check_ins,
        "check_out": check_outs,
        "is_super_host": are_super_hosts,
        "image": images,
        "price": prices
    }

    # Converting the dictionary into a DataFrame for better representation
    accommodations_df = pd.DataFrame(accommodations_dict, columns=["id", "city", "avg_rating", "star_rating",
                                                                   "reviews_count", "listing_obj_type", "space_type",
                                                                   "room_type_category", "listing_guest_label",
                                                                   "bathrooms", "bedrooms", "beds", "check_in",
                                                                   "check_out", "is_super_host", "image", "price"])
    print(accommodations_df)

    return accommodations_df


"""

TRANSFORM

"""


# Data Validation & Basic Preprocessing
def data_validation(df: pd.DataFrame) -> list:
    # Check if the dataframe is empty
    if df.empty:
        logging.warning("The vendor did not send any data or there were no listings for the required location/s.")
        logging.warning("Finishing execution.")
        return [False, None]

    # Primary Key check
    if not pd.Series(df['id']).is_unique:
        logging.warning("Primary Key check is violated.")
        df = df.drop_duplicates(subset='id', keep="first")
        logging.info("Dropped row with duplicate Primary key.")

    # Check for null values
    if df.isnull().values.any():
        logging.warning("Null values found.")

        df.loc[(df['avg_rating'].isna()) & (df['star_rating'].isna()) & (df["reviews_count"] == 0)] = df.loc[
            (df['avg_rating'].isna()) & (df['star_rating'].isna()) & (df["reviews_count"] == 0)].replace(np.nan, 0)

        # calculating overall percentage of missing values
        total_cells = np.product(df.shape)
        missing_values_count = df.isna().sum()
        total_missing = missing_values_count.sum()

        # if the overall percentage is less than 5%, it is acceptable to ignore (drop) them
        if (total_missing / total_cells) * 100 < 5:
            df = df.dropna(axis=0)
            logging.info("Null values handled.")
        else:
            raise Exception("There is a high percentage of Null values in the dataframe.")

    new = df.copy()

    # e.g. "1 guest", "3 guests"
    new["listing_guest_label"] = df["listing_guest_label"].str.replace("guest", "", regex=True).str.replace("s", "", regex=True).str.strip()

    # e.g. € 100
    new["price"] = df["price"].str.replace("€", "", regex=True).str.strip()

    return [True, new]


"""

LOAD

"""


def load_data_to_database(df: pd.DataFrame):
    # SQLAlchemy needs a needs a database driver
    # - in my case psycopg2 to connect with a database (potentially create it and make changes to it)
    # SQLAlchemy will map the dataframe to the database created by the driver


    # The extracted data will be stored in a PostgreSQL database
    connection_string = "postgresql://postgres:mypassword@localhost:5432/airbnb_accommodations"  # insert your password

    adapter = None

    try:
        # Create an engine instance
        alchemy_engine = create_engine(connection_string)

        # Connect to PostgreSQL server
        db_connection = alchemy_engine.connect()

        # giving credentials to the psycopg2 driver
        adapter = psycopg2.connect(
            database="airbnb_accommodations",
            user='postgres',
            password='mypassword',  # insert your password
            host='127.0.0.1',
            port='5432'
        )

        # commands have immediate effect and are not run as a set of operations within a transaction
        adapter.autocommit = True

        # the cursor allows interaction with the database
        cursor = adapter.cursor()

        # Drop the table if it already exists
        # Since I run the function multiple times, I commented this out
        #cursor.execute('DROP TABLE IF EXISTS airbnb_accommodations')

        # Create the table if it doesn't exist already
        sql_query = """
            CREATE TABLE IF NOT EXISTS airbnb_accommodations(
                id varchar(30),
                city varchar(7),
                avg_rating float(2),
                star_rating float(2),
                reviews_count integer,
                listing_obj_type varchar(30),
                space_type varchar(30),
                room_type_category varchar(30),
                listing_guest_label integer,
                bathrooms integer,
                bedrooms integer,
                beds integer,
                check_in date,
                check_out date,
                is_super_host boolean,
                image varchar(200),
                price float(2),
                CONSTRAINT primary_key_constraint PRIMARY KEY (id)
            )
        """
        # create the table by executing the SQL query
        cursor.execute(sql_query)

        # map the dataframe into the database created with the adapter
        df.to_sql('airbnb_accommodations', db_connection, index=False, if_exists='append')

        # read query result
        sql_query_fetch = """
            SELECT * FROM airbnb_accommodations;
        """
        cursor.execute(sql_query_fetch)
        cursor.fetchall()

        # save the changes to the database
        adapter.commit()

        # close the connection
        adapter.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if adapter is not None:
            adapter.close()


"""

WHOLE ETL PROCESS

"""


def whole_etl_process():
    # The collected data will refer to 5 cities in Macedonia
    # the city ids are obtained from the get_destination url from the same Airbnb API
    cities = ["ChIJw0rXGxGKJRMRAIE4sppPCQM", "ChIJY3a9eKD4KhMRYIDk45AsCAM", "ChIJlduiODLChkcRUH5mLgJ4BgM",
              # ROME, FLORENCE, MILAN,
              "ChIJiT3W8dqxfkcRLxCSvfDGo3s", "ChIJiz7_SbFb0xIRAH48R33mBQM", "ChIJoevPBfDSLxMRjR-fmaydjk8",
              # VENICE, GENOA, L'AQUILA,
              "ChIJ3ZO5jSvkNhMRj3wk-HC6bzk", "ChIJxbNI7omXLRMRAIDk45AsCAM", "ChIJtSUVdt3iExMR8DzIUWGH_lg",
              # PUGLIA, ANCONA, CATANIA,
              "ChIJC8RR6ZjUf0cRQZSkWwF84aI", "ChIJG9IQp2iXOxMRMd7OYygL3ow", "ChIJTcQ_uTA0eEcR86sIUvWW8XQ"]
              # BOLOGNA, POSITANO, CORTINA D'AMPEZZO

    logging.info("EXTRACTION")
    # Maximum 40 data samples will be extracted for each of the cities
    df = data_extraction(cities)

    logging.info("Data extracted from Airbnb's API.\nProceed to TRANSFORMATION.")
    is_valid, df_validated = data_validation(df)

    if is_valid:
        logging.info("Data validated.\nProceed to LOAD.")
        print(df_validated)
        load_data_to_database(df_validated)
        logging.info("Data loaded to the database.")


whole_etl_process()
