"""
Code that pulls data through the Conduent and Axsis libraries, and inserts it into the database

Table holding the traffic counts from the speed cameras
CREATE TABLE [dbo].[atves_traffic_counts](
    [locationcode] [nchar](10) NULL,
    [date] [date] NULL,
    [count] [int] NULL
)

Table holding the ticket counts for the red light and overheight cameras
CREATE TABLE [dbo].[atves_ticket_cameras](
    [id] [int] NOT NULL,
    [start_time] [datetime2] NOT NULL,
    [end_time] [datetime2] NOT NULL,
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
    [lat] [decimal](6, 4) NOT NULL,
    [long] [decimal](6, 4) NOT NULL,
    [cam_type] [nchar](2) NOT NULL,
    [effective_date] [date],
    [speed_limit] [int],
    [status] [bit]
)

get_amber_time_rejects_report (rlo)
CREATE TABLE [dbo].[atves_amber_time_rejects](
    [location_code] [int] NOT NULL,
    [deployment_no] [int] NOT NULL,
    [violation_date] [datetime2] NOT NULL,
    [amber_time] [decimal](5, 3) NOT NULL,
    [amber_reject_code] [nchar](100),
    [event_number] [int] NOT NULL PRIMARY KEY
)

get_approval_by_review_date_details (rlo)
CREATE TABLE [dbo].[atves_approval_by_review_date_details](
    [disapproved] [int] NOT NULL,
    [approved] [int] NOT NULL,
    [officer] [nvarchar](100),
    [citation_no] [nvarchar](20) NOT NULL PRIMARY KEY,
    [violation_date] [datetime2],
    [review_status] varchar(20),
    [review_datetime] [datetime2]
)

get_client_summary_by_location
CREATE TABLE [dbo].[atves_by_location](
    [date] [date] NOT NULL,
    [location_code] [int] NOT NULL,
    [section] [nvarchar](20),
    [details] [nvarchar](100),
    [percentage_desc] [nvarchar](50),
    [issued] [int] NOT NULL,
    [in_process] [int] NOT NULL,
    [non_violations] [int] NOT NULL,
    [controllable_rejects] [int] NOT NULL,
    [uncontrollable_rejects] [int] NOT NULL,
    [pending_initial_approval] [int] NOT NULL,
    [pending_reject_approval] [int] NOT NULL,
    [vcDescription] [nvarchar](100),
    [detail_count] [int],
    [order_by] [int]
)

# not importing yet
get_approval_summary_by_queue
get_expired_by_location
get_in_city_vs_out_of_city
get_daily_self_test
get_pending_client_approval
"""

import math
import re
from datetime import datetime

import pyodbc  # type: ignore
from loguru import logger

from balt_geocoder.geocoder import APIFatalError, Geocoder
from atves.axsis import Axsis
from atves.conduent import Conduent, ALLCAMS
from atves.creds import AXSIS_USERNAME, AXSIS_PASSWORD, CONDUENT_USERNAME, CONDUENT_PASSWORD
from atves.creds import GAPI


class AtvesDatabase:
    """ Helper class for the Coduent and Axsis classes that inserts data into the relevant databases"""

    def __init__(self):
        conn = pyodbc.connect('Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
        self.cursor = conn.cursor()
        self.axsis_interface = Axsis(username=AXSIS_USERNAME, password=AXSIS_PASSWORD)
        self.conduent_interface = Conduent(CONDUENT_USERNAME, CONDUENT_PASSWORD)

    def build_location_db(self) -> None:
        """
        Builds the location database with each camera and their lat/long
        :return: None
        """
        self._build_db_conduent_red_light()
        self._build_db_conduent_overheight()
        self._build_db_speed_cameras()

        # Look for missing elements
        self.cursor.execute("SELECT DISTINCT [location_code] FROM [DOT_DATA].[dbo].[atves_amber_time_rejects]")
        location_codes = [str(x[0]).strip() for x in self.cursor.fetchall()]

        self.cursor.execute("SELECT DISTINCT [location_code] FROM [DOT_DATA].[dbo].[atves_by_location]")
        location_codes += [str(x[0]).strip() for x in self.cursor.fetchall()]

        self.cursor.execute("SELECT DISTINCT locationcode FROM [DOT_DATA].[dbo].[atves_traffic_counts]")
        location_codes += [str(x[0]).strip() for x in self.cursor.fetchall()]

        self.cursor.execute("SELECT DISTINCT [locationcode] FROM [DOT_DATA].[dbo].[atves_cam_locations]")
        existing_location_codes = [str(x[0]).strip() for x in self.cursor.fetchall()]
        diff = list(set(location_codes) - set(existing_location_codes))
        if diff:
            logger.warning("Missing location codes: {}", diff)

        self.cursor.execute("SELECT DISTINCT [location] FROM [DOT_DATA].[dbo].[atves_ticket_cameras]")
        locations = [str(x[0]).strip() for x in self.cursor.fetchall()]

        self.cursor.execute("SELECT DISTINCT [locationdescription] FROM [DOT_DATA].[dbo].[atves_cam_locations]")
        existing_locations = [str(x[0]).strip() for x in self.cursor.fetchall()]
        diff = list(set(locations) - set(existing_locations))
        if diff:
            logger.warning("Missing locations: {}", diff)

    def _build_db_conduent_red_light(self) -> None:
        """Builds the camera location database for red light cameras"""
        failures = 0
        loc_id = 0
        data_list = []
        geocoder = Geocoder(GAPI)

        while failures <= 50:
            loc_id += 1
            ret = self.conduent_interface.get_location_by_id(loc_id)
            if ret is None:
                failures += 1
                continue

            try:
                geo = geocoder.geocode("{}, Baltimore, MD".format(ret['Location']))

                data_list.append((str(ret['site_code']), str(ret['location']), float(geo.get('latitude')),
                                  float(geo.get('longitude')), str(ret['cam_type']), ret['effective_date'],
                                  int(ret['speed_limit']), bool(ret['status'] == 'Active')))
            except (RuntimeError, APIFatalError) as err:
                logger.warning("Geocoder error: {}", err)

        # Insert the known good ones
        if data_list:
            self._insert_location_table_elements(data_list)

    def _build_db_conduent_overheight(self) -> None:
        """Builds the camera location database for over height cameras"""
        oh_list = self.conduent_interface.get_overheight_cameras()
        data_list = []
        geocoder = Geocoder(GAPI)

        for location_code, location in oh_list:
            geo = geocoder.geocode("{}, Baltimore, MD".format(location))
            lat = geo.get('latitude') if geo else None
            lng = geo.get('longitude') if geo else None
            data_list.append((location_code, location, lat, lng, 'OH', None, None, None))

        if data_list:
            self._insert_location_table_elements(data_list)

    def _build_db_speed_cameras(self) -> None:
        """Builds the camera location database for speed cameras"""
        # Get the list of location codes in the traffic count database (AXSIS)
        self.cursor.execute("SELECT DISTINCT [locationcode] "
                            "FROM [DOT_DATA].[dbo].[atves_traffic_counts]"
                            "WHERE locationcode LIKE 'BAL%'")
        location_codes_needed = [str(x[0]).strip() for x in self.cursor.fetchall()]

        self.cursor.execute("SELECT DISTINCT [locationcode] "
                            "FROM [DOT_DATA].[dbo].[atves_cam_locations]"
                            "WHERE locationcode LIKE 'BAL%'")
        location_codes_existing = [str(x[0]).strip() for x in self.cursor.fetchall()]

        diff = list(set(location_codes_needed) - set(location_codes_existing))
        data_list = []
        geocoder = Geocoder(GAPI)

        for location_code in diff:
            if not location_code:
                continue

            # First, lets get a date when this camera existed
            self.cursor.execute("SELECT * FROM [DOT_DATA].[dbo].[atves_traffic_counts] WHERE locationcode = ?",
                                location_code)
            traffic_counts = self.cursor.fetchall()

            cam_date = datetime.strptime(traffic_counts[0][1], "%Y-%m-%d")
            axsis_data = self.axsis_interface.get_traffic_counts(cam_date, cam_date)
            location = [x for x in axsis_data.values.tolist() if x[0] == location_code][0][1]

            if not location:
                continue

            geo = geocoder.geocode("{}, Baltimore, MD".format(location))
            lat = geo.get('latitude') if geo else None
            lng = geo.get('longitude') if geo else None
            data_list.append((location_code, location, lat, lng, 'SC', cam_date, None, None))

        if data_list:
            self._insert_location_table_elements(data_list)

    def _insert_location_table_elements(self, data_list):
        self.cursor.executemany("""
            MERGE [atves_cam_locations] USING (
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            ) AS vals (locationcode, locationdescription, lat, long, cam_type, effective_date, speed_limit,
                status)
            ON (atves_cam_locations.locationcode = vals.locationcode)
            WHEN NOT MATCHED THEN
                INSERT (locationcode, locationdescription, lat, long, cam_type, effective_date, speed_limit,
                    status)
                VALUES (locationcode, locationdescription, lat, long, cam_type, effective_date, speed_limit,
                    status);
            """, data_list)
        self.cursor.commit()

    def process_conduent_reject_numbers(self, start_date: datetime, end_date: datetime, cam_type=ALLCAMS):
        """
        Inserts data into the database from conduent rejection numbers
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :param cam_type: (int) Type of camera data to pull (use the constants conduent.REDLIGHT or conduent.OVERHEIGHT
            or conduent.ALLCAMS (default: conduent.ALLCAMS)
        :return: None
        """
        logger.info('Processing conduent reject reports from {} to {}', start_date.strftime("%m/%d/%y"),
                     end_date.strftime("%m/%d/%y"))
        data = self.conduent_interface.get_deployment_data(start_date, end_date, cam_type)

        if not data:
            return

        data_list = []
        for row in data:
            data_list.append((int(row['id']), row['start_time'], row['end_time'], str(row['location']),
                              str(row['officer']), str(row['equip_type']), int(row['issued']), int(row['rejected'])))

        self.cursor.executemany("""
        MERGE [atves_ticket_cameras] USING (
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
        ) AS vals (id, start_time, end_time, location, officer, equip_type, issued, rejected)
        ON (atves_ticket_cameras.start_time = vals.start_time AND
            atves_ticket_cameras.location = vals.location)
        WHEN NOT MATCHED THEN
            INSERT (id, start_time, end_time, location, officer, equip_type, issued, rejected)
            VALUES (id, start_time, end_time, location, officer, equip_type, issued, rejected);
        """, data_list)

        self.cursor.commit()

    def process_conduent_data_amber_time(self, start_date: datetime, end_date: datetime):
        """

        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :return:
        """
        logger.info('Processing conduent amber time report from {} to {}}', start_date.strftime("%m/%d/%y"),
                     end_date.strftime("%m/%d/%y"))

        data = self.conduent_interface.get_amber_time_rejects_report(start_date, end_date)

        if data.empty:
            return

        data_list = []
        for _, row in data.iterrows():
            data_list.append((int(row['iLocationCode']), int(row['Deployment Number']), row['VioDate'],
                              float(row['Amber Time']), str(row['Amber Reject Code']), int(row['Event Number'])))

        self.cursor.executemany("""
            MERGE [atves_amber_time_rejects] USING(
            VALUES
                (?, ?, ?, ?, ?, ?)
            ) AS vals (location_code, deployment_no, violation_date, amber_time, amber_reject_code, event_number)
            ON atves_amber_time_rejects.event_number = vals.event_number
            WHEN NOT MATCHED THEN
                INSERT (location_code, deployment_no, violation_date, amber_time, amber_reject_code, event_number)
                VALUES (location_code, deployment_no, violation_date, amber_time, amber_reject_code, event_number);
        """, data_list)

        self.cursor.commit()

    def process_conduent_data_approval_by_review_date(self, start_date: datetime, end_date: datetime, cam_type: int):
        """

        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :param cam_type: (int) Either conduent.REDLIGHT or conduent.OVERHEIGHT
        :return:
        """
        logger.info('Processing conduent data approval report from {} to {}', start_date.strftime("%m/%d/%y"),
                     end_date.strftime("%m/%d/%y"))
        data = self.conduent_interface.get_approval_by_review_date_details(start_date, end_date, cam_type)

        if data.empty:
            return

        data_list = []
        for _, row in data.iterrows():
            review_date = "{} {}".format(row['Review Date'], row['st'])
            data_list.append((int(row['Disapproved']), int(row['Approved']), str(row['Officer']), str(row['CitNum']),
                              row['Vio Date'], str(row['Review Status']), review_date))

        self.cursor.executemany("""
            MERGE [atves_approval_by_review_date_details] USING(
            VALUES
                (?, ?, ?, ?, ?, ?, ?)
            ) AS vals (disapproved, approved, officer, citation_no, violation_date, review_status, review_datetime)
            ON atves_approval_by_review_date_details.citation_no = vals.citation_no
            WHEN NOT MATCHED THEN
                INSERT (disapproved, approved, officer, citation_no, violation_date, review_status, review_datetime)
                VALUES (disapproved, approved, officer, citation_no, violation_date, review_status, review_datetime);
        """, data_list)
        self.cursor.commit()

    def process_conduent_data_by_location(self, start_date: datetime, end_date: datetime, cam_type=ALLCAMS):
        """

        :param cam_type: (int) Either conduent.REDLIGHT or conduent.OVERHEIGHT
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :return:
        """

        def _get_int(value):
            """Gets int on the front of the string"""
            pattern = re.compile(r"(\d*)")
            ret = 0
            if value != 'All Locations':
                match = pattern.match(value)
                if match.lastindex < 1:
                    logger.error("Unable to parse location {}", value)
                    ret = 9999
                else:
                    ret = match.group(1)
            return int(ret)

        logger.info('Processing conduent location data reports from {} to {}', start_date.strftime("%m/%d/%y"),
                     end_date.strftime("%m/%d/%y"))
        data = self.conduent_interface.get_client_summary_by_location(start_date, end_date, cam_type)

        if data.empty:
            return

        data_list = []
        for _, row in data.iterrows():
            location_id = _get_int(row['Locations'])
            if location_id == 0:
                continue
            data_list.append((row['Date'], location_id, str(row['Section']), str(row['Details']),
                              str(row['PercentageDescription']), int(row['Issued']), int(row['InProcess']),
                              int(row['NonViolations']), int(row['ControllableRejects']),
                              int(row['UncontrollableRejects']), int(row['PendingInitialapproval']),
                              int(row['PendingRejectapproval']), str(row['vcDescription']), int(row['DetailCount']),
                              _get_int(row['iOrderBy'])))

        self.cursor.executemany("""
            MERGE [atves_by_location] USING(
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ) AS vals (date, location_code, section, details, percentage_desc, issued, in_process, non_violations,
            controllable_rejects, uncontrollable_rejects, pending_initial_approval, pending_reject_approval,
            vcDescription, detail_count, order_by)
            ON (atves_by_location.date = vals.date AND
                atves_by_location.location_code = vals.location_code AND
                atves_by_location.vcDescription = vals.vcDescription)
            WHEN NOT MATCHED THEN
                INSERT (date, location_code, section, details, percentage_desc, issued, in_process, non_violations,
                    controllable_rejects, uncontrollable_rejects, pending_initial_approval, pending_reject_approval,
                    vcDescription, detail_count, order_by)
                VALUES (date, location_code, section, details, percentage_desc, issued, in_process, non_violations,
                    controllable_rejects, uncontrollable_rejects, pending_initial_approval, pending_reject_approval,
                    vcDescription, detail_count, order_by);
        """, data_list)
        self.cursor.commit()

    def process_traffic_count_data(self, start_date: datetime, end_date: datetime):
        """
        Processes the traffic count camera data from Axsis and Conduent
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :return:
        """
        logger.info('Processing traffic count data from {} to {}', start_date.strftime("%m/%d/%y"),
                     end_date.strftime("%m/%d/%y"))

        # Get data from speed cameras
        axsis_data = self.axsis_interface.get_traffic_counts(start_date, end_date)
        axsis_data = axsis_data.to_dict('index')
        columns = axsis_data[0].keys() - ['Location code', 'Description', 'First Traf Evt', 'Last Traf Evt']

        data = [(row['Location code'], event_date, row[event_date])
                for row in axsis_data.values()
                for event_date in columns
                if not math.isnan(row[event_date])]

        # Get data from red light cameras
        conduent_data = self.conduent_interface.get_traffic_counts_by_location(start_date, end_date)
        data += [(str(row['iLocationCode']).strip(), row['Ddate'], int(row['VehPass']))
                 for index, row in conduent_data.iterrows()]

        if data:
            self.cursor.executemany("""
                        MERGE atves_traffic_counts USING (
                        VALUES
                            (?, ?, ?)
                        ) AS vals (locationcode, [date], count)
                        ON (atves_traffic_counts.locationcode = vals.locationcode AND
                            atves_traffic_counts.date = vals.date)
                        WHEN MATCHED THEN
                            UPDATE SET
                            count = vals.count
                        WHEN NOT MATCHED THEN
                            INSERT (locationcode, [date], count)
                            VALUES (vals.locationcode, vals.date, vals.count);
                        """, data)
            self.cursor.commit()
