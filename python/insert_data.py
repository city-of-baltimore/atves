"""
Uses the Axsis library to insert data into the database with the following structure:

CREATE TABLE [dbo].[traffic_counts](
    [locationcode] [nchar](10) NULL,
    [date] [date] NULL,
    [count] [int] NULL
)

The camera location database looks like this
CREATE TABLE [dbo].[traffic_cam_locations](
    [locationcode] [nchar](10) NOT NULL,
    [locationdescription] [nchar](100) NOT NULL,
    [lat] [decimal](18, 0) NOT NULL,
    [long] [decimal](18, 0) NOT NULL
)
"""

import argparse
import json
import math
from datetime import datetime, date, timedelta
import requests

from axsis import Axsis
import creds
import pyodbc

CONN = pyodbc.connect('Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
CURSOR = CONN.cursor()
AXSIS_INTERFACE = Axsis(username=creds.AXSIS_USERNAME, password=creds.AXSIS_PASSWORD)


def build_location_db():
    """

    :return:
    """
    # Get the list of location codes in the traffic count database
    CURSOR.execute("SELECT DISTINCT [locationcode] FROM [DOT_DATA].[dbo].[traffic_counts]")
    location_codes_needed = [x[0].strip() for x in CURSOR.fetchall()]

    CURSOR.execute("SELECT DISTINCT [locationcode] FROM [DOT_DATA].[dbo].[traffic_cam_locations]")
    location_codes_existing = [x[0].strip() for x in CURSOR.fetchall()]
    # location_codes_existing = [x[0].strip() for x in CURSOR.fetchall()]

    diff = list(set(location_codes_needed) - set(location_codes_existing))
    for location_code in diff:
        # Get a date when this camera existed
        CURSOR.execute("SELECT * FROM traffic_counts WHERE locationcode = ?", location_code)
        traffic_counts = CURSOR.fetchall()

        cam_date = datetime.strptime(traffic_counts[0][1], "%Y-%m-%d")
        axsis_data = AXSIS_INTERFACE.get_traffic_counts(cam_date, cam_date)
        location = [x for x in axsis_data.values.tolist() if x[0] == location_code][0][1]
        lookup_addr = location.replace('BLK', '')\
                              .replace('EB', '')\
                              .replace('SB', '')\
                              .replace('NB', '')\
                              .replace('WB', '')
        lat, long = get_geo(lookup_addr)

        CURSOR.execute("""
            INSERT INTO traffic_cam_locations (locationcode, locationdescription, lat, long) VALUES (?, ?, ?, ?)""",
                       location_code, location, lat, long)
        CURSOR.commit()


def get_geo(street_address):
    """
    Pulls the latitude and longitude of an address, either from the internet, or the cached version
    """
    geo_lookup = ("https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?"
                  "singleLine={}&"
                  "f=json&"
                  "outFields=Match_addr,Addr_type")

    req = requests.get(geo_lookup.format("{} Baltimore, MD".format(street_address)))
    try:
        geo = req.json()
    except json.decoder.JSONDecodeError:
        print("JSON ERROR: %s", req)

    latitude = geo['candidates'][0]['location']['y']
    longitude = geo['candidates'][0]['location']['x']
    return latitude, longitude


def start_from_cmd_line():
    """Starts from the command line"""
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description='Traffic count importer')
    parser.add_argument('-m', '--month', type=int, default=yesterday.month,
                        help=('Optional: Month of date we should start searching on (IE: 10 for Oct). Defaults to '
                              'yesterday if not specified'))
    parser.add_argument('-d', '--day', type=int, default=yesterday.day,
                        help=('Optional: Day of date we should start searching on (IE: 5). Defaults to yesterday if '
                              'not specified'))
    parser.add_argument('-y', '--year', type=int, default=yesterday.year,
                        help=('Optional: Year of date we should start searching on (IE: 2020). Defaults to yesterday '
                              'if not specified'))
    parser.add_argument('-n', '--numofdays', default=1, type=int,
                        help='Optional: Number of days to search, including the start date.')

    args = parser.parse_args()

    build_location_db()

    for i in range(args.numofdays):
        search_date = date(args.year, args.month, args.day) + timedelta(days=i)
        print("Processing {}".format(search_date))
        data = AXSIS_INTERFACE.get_traffic_counts(search_date, search_date)
        data = data.values.tolist()
        data = [(i[0].strip(), search_date.strftime('%Y-%m-%d'), i[4]) for i in data if not math.isnan(i[4])]

        if not data:
            continue

        CURSOR.executemany("""
                MERGE traffic_counts USING (
                VALUES
                    (?, ?, ?)
                ) AS vals (locationcode, [date], count)
                ON (traffic_counts.locationcode = vals.locationcode AND
                    traffic_counts.date = vals.date)
                WHEN MATCHED THEN
                    UPDATE SET
                    count = vals.count
                WHEN NOT MATCHED THEN
                    INSERT (locationcode, [date], count)
                    VALUES (vals.locationcode, vals.date, vals.count);
                """, data)
        CURSOR.commit()


if __name__ == '__main__':
    start_from_cmd_line()
