"""
Scrapes data for the ATVES program's speed, overheight and redlight cameras.

Table holding the traffic counts from the speed cameras
CREATE TABLE [dbo].[traffic_counts](
    [locationcode] [nchar](10) NULL,
    [date] [date] NULL,
    [count] [int] NULL
)

Table holding the ticket counts for the red light and overheight cameras
CREATE TABLE [dbo].[ticket_cameras](
    [id] [int] NOT NULL,
    [start_time] [datetime] NOT NULL,
    [end_time] [datetime] NOT NULL,
    [location] [varchar](max) NOT NULL,
    [officer] [varchar](max) NULL,
    [equip_type] [varchar](max) NULL,
    [issued] [int] NOT NULL,
    [rejected] [int] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

The camera location database looks like this
CREATE TABLE [dbo].[atves_cam_locations](
    [locationcode] [nchar](100),
    [locationdescription] [nchar](100) NOT NULL,
    [lat] [decimal](2, 4) NOT NULL,
    [long] [decimal](2, 4) NOT NULL,
    [cam_type] [nchar](2) NOT NULL
)
"""

import argparse
import json
import logging
import math
from datetime import datetime, date, timedelta
import requests

import pyodbc
from axsis import Axsis
import citeweb
import creds


CONN = pyodbc.connect('Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
CURSOR = CONN.cursor()
AXSIS_INTERFACE = Axsis(username=creds.AXSIS_USERNAME, password=creds.AXSIS_PASSWORD)

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def build_location_db():
    """
    Builds the location database with each camera and their lat/long
    :return: None
    """
    # Get the list of location codes in the traffic count database
    CURSOR.execute("SELECT DISTINCT [locationcode] FROM [DOT_DATA].[dbo].[traffic_counts]")
    location_codes_needed = [x[0].strip() for x in CURSOR.fetchall()]

    # Get the list of locations for the red light/overheight cameras
    CURSOR.execute("SELECT DISTINCT [location] FROM [DOT_DATA].[dbo].[ticket_cameras]")
    location_codes_needed += [x[0].strip() for x in CURSOR.fetchall()]

    CURSOR.execute("SELECT DISTINCT [locationcode] FROM [DOT_DATA].[dbo].[atves_cam_locations]")
    location_codes_existing = [x[0].strip() for x in CURSOR.fetchall()]
    # location_codes_existing = [x[0].strip() for x in CURSOR.fetchall()]

    diff = list(set(location_codes_needed) - set(location_codes_existing))

    data_list = []
    for location_code in diff:
        if not location_code:
            continue
        if location_code.startswith("BAL"):
            # Get a date when this camera existed
            CURSOR.execute("SELECT * FROM [DOT_DATA].[dbo].[traffic_counts] WHERE locationcode = ?", location_code)
            traffic_counts = CURSOR.fetchall()

            cam_date = datetime.strptime(traffic_counts[0][1], "%Y-%m-%d")
            axsis_data = AXSIS_INTERFACE.get_traffic_counts(cam_date, cam_date)
            location = [x for x in axsis_data.values.tolist() if x[0] == location_code][0][1]
            cam_type = 'SC'
        else:
            CURSOR.execute("SELECT [equip_type] FROM [DOT_DATA].[dbo].[ticket_cameras] WHERE location = ?",
                           location_code)
            equipment_type = CURSOR.fetchall()[0][0].strip()
            if equipment_type in ['Gen3']:
                cam_type = 'RL'
            elif equipment_type in ['OH']:
                cam_type = 'OH'
            else:
                logging.error("Invalid equipment type in location code: %s (equipment type: %s)",
                              location_code,
                              equipment_type)
            location = location_code

        lookup_addr = location.replace('BLK', '')\
                              .replace('EB', '')\
                              .replace('SB', '')\
                              .replace('NB', '')\
                              .replace('WB', '')
        if not lookup_addr:
            continue

        lat, long = get_geo(lookup_addr)
        data_list.append((location_code, location, lat, long, cam_type))

    if data_list:
        CURSOR.executemany("""
                MERGE [atves_cam_locations] USING (
                VALUES
                    (?, ?, ?, ?, ?)
                ) AS vals (locationcode, locationdescription, lat, long, cam_type)
                ON (atves_cam_locations.locationcode = vals.locationcode)
                WHEN NOT MATCHED THEN
                    INSERT (locationcode, locationdescription, lat, long, cam_type)
                    VALUES (locationcode, locationdescription, lat, long, cam_type);
                """, data_list)
        CURSOR.commit()


def get_geo(street_address):
    """
    Pulls the latitude and longitude of an address, either from the internet, or the cached version
    :param street_address: (str) Street address to look up. Only the street number and name, as Baltimore is appended
    :return: Tuple, with format (float, float) that is the latitude, longitude
    """
    assert street_address
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


def process_citeweb_data(year, month, day, quantity, cam_type=citeweb.ALLCAMS):
    """
    Inserts data into the database from red light and overheight cameras
    :param year: (int) Four digit year (used as the range start if 'quantity' is specified)
    :param month: (int) Month to pull (used as the range start if 'quantity' is
        specified)
    :param day: (int) Day to pull (used as the range start if 'quantity' is specified)
    :param quantity: (int) Number of days to pull, with the year/month/day args used as the start of the range
    :param cam_type: (int) Type of camera data to pull (use the constants citeweb.REDLIGHT or citeweb.OVERHEIGHT or
        citeweb.ALLCAMS (default: citeweb.ALLCAMS)
    :return: None
    """
    cw_interface = citeweb.CiteWeb(creds.CITEWEB_USERNAME, creds.CITEWEB_PASSWORD)
    otp = input("Enter OTP value: ")
    cw_interface.login_otp(otp)

    print('Processing {}/{}'.format(month, year))

    data = cw_interface.get_deployment_data(year, month, day, quantity, cam_type)
    if not data:
        return

    data_list = []
    for row in data:
        data_list.append((row['id'], row['start_time'], row['end_time'], row['location'], row['officer'],
                          row['equip_type'], row['issued'], row['rejected']))

    CURSOR.executemany("""
    MERGE [ticket_cameras] USING (
    VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    ) AS vals (id, start_time, end_time, location, officer, equip_type, issued, rejected)
    ON (ticket_cameras.start_time = vals.start_time AND
        ticket_cameras.location = vals.location)
    WHEN NOT MATCHED THEN
        INSERT (id, start_time, end_time, location, officer, equip_type, issued, rejected)
        VALUES (id, start_time, end_time, location, officer, equip_type, issued, rejected);
    """, data_list)

    CURSOR.commit()


def process_axsis_data(year, month, day, quantity):
    """
    Processes the red light and overhead camera data from Axsis
    :param year: (int) Four digit year (used as the range start if 'quantity' is specified)
    :param month: (int) Month to pull (used as the range start if 'quantity' is specified)
    :param day: (int) Day to pull (used as the range start if 'quantity' is specified)
    :param quantity: (int) Number of days to pull, with the year/month/day args used as the start of the range
    :return:
    """
    start_date = date(year, month, day)
    end_date = date(year, month, day) + timedelta(days=quantity)
    data = AXSIS_INTERFACE.get_traffic_counts(start_date, end_date)
    data = [(i[0].strip(), i[3].strftime('%Y-%m-%d'), i[4])
            for i in data.values.tolist()
            if not math.isnan(i[4])]

    if data:
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
                        help=('Optional: Four digit year we should start searching on (IE: 2020). Defaults to '
                              'yesterday if not specified'))
    parser.add_argument('-n', '--numofdays', default=1, type=int,
                        help='Optional: Number of days to search, including the start date.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-a', '--allcams', action='store_true', help="Process all camera types")
    group.add_argument('-o', '--oh', action='store_true', help="Process only overheight cameras")
    group.add_argument('-r', '--rl', action='store_true', help="Process only redlight cameras")
    group.add_argument('-t', '--tc', action='store_true', help="Process only traffic counts")

    args = parser.parse_args()
    build_location_db()

    if args.tc or args.allcams:
        # Process traffic counts from speed cameras
        process_axsis_data(args.year, args.month, args.day, args.numofdays)

    # Process red light and overheight cameras
    if args.oh:
        cam_type = citeweb.OVERHEIGHT
    elif args.rl:
        cam_type = citeweb.REDLIGHT
    elif args.allcams:
        cam_type = citeweb.ALLCAMS
    else:
        return

    process_citeweb_data(args.year, args.month, args.day, args.numofdays, cam_type)


if __name__ == '__main__':
    start_from_cmd_line()
