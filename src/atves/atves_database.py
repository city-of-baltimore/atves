"""Pulls data through the Conduent and Axsis libraries, and inserts it into the database"""
import argparse
import math
import re
from datetime import date, datetime, timedelta
from sqlite3 import Connection as SQLite3Connection
from typing import Optional

from arcgis.geocoding import geocode  # type: ignore
from arcgis.gis import GIS  # type: ignore
from loguru import logger
from sqlalchemy import create_engine, event, inspect as sqlalchemyinspect  # type: ignore
from sqlalchemy.engine import Engine  # type: ignore
from sqlalchemy.exc import IntegrityError  # type: ignore
from sqlalchemy.ext.declarative import DeclarativeMeta  # type: ignore
from sqlalchemy.orm import Session  # type: ignore
from sqlalchemy.sql import text  # type: ignore

from atves.atves_schema import AtvesAmberTimeRejects, AtvesApprovalByReviewDateDetails, AtvesByLocation, \
    AtvesCamLocations, AtvesTicketCameras, AtvesTrafficCounts, AtvesViolations, AtvesViolationCategories, Base
from atves.axsis import Axsis
from atves.conduent import Conduent, ALLCAMS, REDLIGHT, OVERHEIGHT
from atves.creds import AXSIS_USERNAME, AXSIS_PASSWORD, CONDUENT_USERNAME, CONDUENT_PASSWORD

GIS()


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):  # pylint:disable=unused-argument
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


VIOLATION_TYPES = {
            # Conduent: 1- In Process || Axsis: Events still in WF
            1: 'In Process',
            # Conduent: 2- Conduent/City Non Violations || Axsis: Non Events, PD Non Event
            2: 'Non Violations',
            # Conduent: 3- Conduent/City Rejects(Controllable) || Axsis: Controllable, PD Controllable
            3: 'Controllable Reject',
            # Conduent: 4- Conduent/City Rejects(Uncontrollable) || Axsis: Uncontrollable, PD Uncontrollable
            4: 'Uncontrollable Reject',
            # Conduent: 5- Violations Mailed, || Axsis: Citations Issued, Nov Issued, Warning Issued
            5: 'Violation Issued'
        }


class AtvesDatabase:
    """ Helper class for the Conduent and Axsis classes that inserts data into the relevant databases"""

    def __init__(self, conn_str: str, axsis_user: Optional[str] = AXSIS_USERNAME,  # pylint:disable=too-many-arguments
                 axsis_pass: Optional[str] = AXSIS_PASSWORD, conduent_user: Optional[str] = CONDUENT_USERNAME,
                 conduent_pass: Optional[str] = CONDUENT_PASSWORD):
        """
        :param conn_str: sqlalchemy connection string (IE sqlite:///crash.db or
        Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;)
        """
        logger.info('Creating db with connection string: {}', conn_str)
        self.engine = create_engine(conn_str, echo=True, future=True)

        with self.engine.begin() as connection:
            Base.metadata.create_all(connection)

        self.axsis_interface = Axsis(username=axsis_user, password=axsis_pass) if axsis_user and axsis_pass else None
        self.conduent_interface = Conduent(conduent_user, conduent_pass) if conduent_user and conduent_pass else None

        self.location_db_built = False

    def build_location_db(self, force: bool = False) -> None:
        """
        Builds the location database with each camera and their lat/long
        :param force: If true, then it will rebuild the location db even if it was already built this session
        :return: None
        """
        if self.location_db_built and not force:
            # if we already built the db and we are not forcing a rebuild, then bail
            return

        self._build_db_conduent_red_light()
        self._build_db_conduent_overheight()
        self._build_db_speed_cameras()

        with Session(bind=self.engine, future=True) as session:
            # Look for missing location ids
            location_codes = session.query(AtvesAmberTimeRejects.location_code).all()
            location_codes += session.query(AtvesByLocation.location_code).all()
            location_codes += session.query(AtvesTrafficCounts.location_code).all()

            existing_location_codes = session.query(AtvesCamLocations.location_code).all()

            diff = set(location_codes) - set(existing_location_codes)
            if diff:
                raise AssertionError("Missing location codes: {}".format(diff))

            # look for missing locations
            locations = session.query(AtvesTicketCameras.location).all()
            existing_locations = session.query(AtvesCamLocations.locationdescription).all()
            diff = set(locations) - set(existing_locations)

            if diff:
                raise AssertionError("Missing locations: {}".format(diff))
        self.location_db_built = True

    def build_violation_lookup_db(self) -> None:
        """Builds a violation description lookup table"""
        for vio_key, vio_desc in VIOLATION_TYPES.items():
            self._insert_or_update(AtvesViolationCategories(violation_cat=vio_key, description=vio_desc))

    def _build_db_conduent_red_light(self) -> None:
        """Builds the camera location database for red light cameras"""
        if not self.conduent_interface:
            logger.warning('Unable to run _build_db_conduent_red_light. It requires a Conduent session, which is not '
                           'setup.')
            return

        failures = 0
        loc_id = 0

        # we use the 'failures' because we don't know the range of valid location ids
        while failures <= 50:
            loc_id += 1
            ret = self.conduent_interface.get_location_by_id(loc_id, REDLIGHT)
            if ret['site_code'] is None:
                failures += 1
                continue

            try:
                lat, lng = self.get_lat_long(ret['location'])
                self._insert_or_update(AtvesCamLocations(
                    location_code=str(ret['site_code']),
                    locationdescription=str(ret['location']),
                    lat=lat,
                    long=lng,
                    cam_type=str(ret['cam_type']),
                    effective_date=datetime.strptime(ret['effective_date'], '%b %d, %Y'),
                    speed_limit=int(ret['speed_limit']),
                    status=bool(ret['status'] == 'Active')))
            except RuntimeError as err:
                logger.warning("Geocoder error: {}", err)

    def _build_db_conduent_overheight(self) -> None:
        """Builds the camera location database for over height cameras"""
        if not self.conduent_interface:
            logger.warning('Unable to run _build_db_conduent_overheight. It requires a Conduent session, which is not '
                           'setup.')
            return

        oh_list = self.conduent_interface.get_overheight_cameras()

        for location_code, location in oh_list:
            lat, lng = self.get_lat_long(location)
            self._insert_or_update(AtvesCamLocations(location_code=location_code,
                                                     locationdescription=location,
                                                     lat=lat,
                                                     long=lng,
                                                     cam_type='OH',
                                                     effective_date=None,
                                                     speed_limit=None,
                                                     status=None))

    def _build_db_speed_cameras(self) -> None:
        """Builds the camera location database for speed cameras"""
        if not self.axsis_interface:
            logger.warning('Unable to run _build_db_speed_cameras. It requires a Axsis session, which is not setup.')
            return

        # Get the list of location codes in the traffic count database (AXSIS)
        with Session(bind=self.engine, future=True) as session:
            # get all cameras used in the last 30 days
            report_details = self.axsis_interface.get_reports_detail('LOCATION PERFORMANCE DETAIL')
            active_cams = [param_elem['Value'] for param in report_details['Parameters']
                           if param['ParmTitle'] == 'Violation Locations'
                           for param_elem in param['ParmList']]

            for location_code in active_cams:
                if not location_code:
                    continue

                # First, lets get a date when this camera existed
                traffic_counts = session.query(AtvesTrafficCounts.date). \
                    filter(AtvesTrafficCounts.location_code == location_code).all()

                try:
                    cam_date: Optional[datetime] = datetime.strptime(traffic_counts[0][1], "%Y-%m-%d")
                    axsis_data = self.axsis_interface.get_traffic_counts(cam_date, cam_date)
                    location = [x for x in axsis_data.values.tolist() if x[0] == location_code][0][1]
                except IndexError:
                    cam_date = None
                    location = self.axsis_interface.get_location_info(location_code)

                if not location:
                    continue

                lat, lng = self.get_lat_long(location)
                self._insert_or_update(AtvesCamLocations(location_code=location_code,
                                                         locationdescription=location,
                                                         lat=lat,
                                                         long=lng,
                                                         cam_type='SC',
                                                         effective_date=cam_date,
                                                         speed_limit=None,
                                                         status=None))

    def process_conduent_reject_numbers(self, start_date: date, end_date: date, cam_type=ALLCAMS) -> None:
        """
        Inserts data into the database from conduent rejection numbers
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param cam_type: Type of camera data to pull (use the constants conduent.REDLIGHT or conduent.OVERHEIGHT
            or conduent.ALLCAMS (default: conduent.ALLCAMS)
        :return: None
        """
        logger.info('Processing conduent reject reports from {} to {}', start_date.strftime("%m/%d/%y"),
                    end_date.strftime("%m/%d/%y"))

        if not self.conduent_interface:
            logger.warning('Unable to run process_conduent_reject_numbers. It requires a Conduent session, which is not'
                           ' setup.')
            return

        self.build_location_db()
        data = self.conduent_interface.get_deployment_data(start_date, end_date, cam_type)

        if not data:
            return

        for row in data:
            self._insert_or_update(AtvesTicketCameras(id=int(row['id']),
                                                      start_time=row['start_time'],
                                                      end_time=row['end_time'],
                                                      location=str(row['location']).strip(),
                                                      officer=str(row['officer']),
                                                      equip_type=str(row['equip_type']),
                                                      issued=int(row['issued']),
                                                      rejected=int(row['rejected'])))

    def process_conduent_data_amber_time(self, start_date: date, end_date: date) -> None:
        """

        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :return:
        """
        logger.info('Processing conduent amber time report from {} to {}', start_date.strftime("%m/%d/%y"),
                    end_date.strftime("%m/%d/%y"))

        if not self.conduent_interface:
            logger.warning('Unable to run process_conduent_data_amber_time. It requires a Conduent session, which is '
                           'not setup.')
            return

        self.build_location_db()
        data = self.conduent_interface.get_amber_time_rejects_report(start_date, end_date)

        if data.empty:
            return

        for _, row in data.iterrows():
            self._insert_or_update(AtvesAmberTimeRejects(
                location_code=int(row['iLocationCode']),
                deployment_no=int(row['Deployment Number']),
                violation_date=datetime.strptime(row['VioDate'], '%m/%d/%Y %I:%M:%S %p'),
                amber_time=float(row['Amber Time']),
                amber_reject_code=str(row['Amber Reject Code']),
                event_number=int(row['Event Number'])))

    def process_conduent_data_approval_by_review_date(self, start_date: date, end_date: date, cam_type: int) -> None:
        """

        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param cam_type: Either conduent.REDLIGHT or conduent.OVERHEIGHT
        :return:
        """
        logger.info('Processing conduent data approval report from {} to {}', start_date.strftime("%m/%d/%y"),
                    end_date.strftime("%m/%d/%y"))

        if not self.conduent_interface:
            logger.warning('Unable to run process_conduent_data_approval_by_review_date. It requires a Conduent '
                           'session, which is not setup.')
            return

        self.build_location_db()
        data = self.conduent_interface.get_approval_by_review_date_details(start_date, end_date, cam_type)

        if data.empty:
            return

        for _, row in data.iterrows():
            self._insert_or_update(AtvesApprovalByReviewDateDetails(
                disapproved=int(row['Disapproved']),
                approved=int(row['Approved']),
                officer=str(row['Officer']),
                citation_no=str(row['CitNum']),
                violation_date=datetime.strptime(row['Vio Date'], '%b %d %Y %I:%M%p'),
                review_status=str(row['Review Status']),
                review_datetime=datetime.strptime("{} {}".format(row['Review Date'], row['st']), '%m/%d/%Y %H:%M:%S')))

    def process_conduent_data_by_location(self, start_date: date, end_date: date, cam_type: int = ALLCAMS) -> None:
        """

        :param cam_type: Either conduent.REDLIGHT or conduent.OVERHEIGHT
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :return:
        """
        if not self.conduent_interface:
            logger.warning('Unable to run process_conduent_data_by_location. It requires a Conduent '
                           'session, which is not setup.')
            return

        self.build_location_db()

        if cam_type == ALLCAMS:
            self.process_conduent_data_by_location(start_date, end_date, REDLIGHT)
            self.process_conduent_data_by_location(start_date, end_date, OVERHEIGHT)
            return

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

        for _, row in data.iterrows():
            location_id = _get_int(row['Locations'])
            if location_id == 0:
                continue
            self._insert_or_update(AtvesByLocation(date=datetime.strptime(row['Date'], '%m/%d/%y').date(),
                                                   location_code=location_id,
                                                   section=str(row['Section']),
                                                   details=str(row['Details']),
                                                   percentage_desc=str(row['PercentageDescription']),
                                                   issued=int(row['Issued']),
                                                   in_process=int(row['InProcess']),
                                                   non_violations=int(row['NonViolations']),
                                                   controllable_rejects=int(row['ControllableRejects']),
                                                   uncontrollable_rejects=int(row['UncontrollableRejects']),
                                                   pending_initial_approval=int(row['PendingInitialapproval']),
                                                   pending_reject_approval=int(row['PendingRejectapproval']),
                                                   vcDescription=str(row['vcDescription']),
                                                   detail_count=int(row['DetailCount']),
                                                   order_by=_get_int(row['iOrderBy'])))

    def process_traffic_count_data(self, start_date: date, end_date: date) -> None:
        """
        Processes the traffic count camera data from Axsis and Conduent
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :return:
        """
        logger.info('Processing traffic count data from {} to {}', start_date.strftime("%m/%d/%y"),
                    end_date.strftime("%m/%d/%y"))

        self.build_location_db()
        self._process_traffic_count_data_axsis(start_date, end_date)
        self._process_traffic_count_data_conduent(start_date, end_date)

    def _process_traffic_count_data_axsis(self, start_date: date, end_date: date) -> None:
        if not self.axsis_interface:
            logger.warning('Unable to run _process_traffic_count_data_axsis. It requires a Axsis session, which is not '
                           'setup.')
            return

        # Get data from speed cameras. There are issues pulling more than 90 days of data, so we split if its larger
        tmp_start_date = start_date
        tmp_end_date = start_date + timedelta(days=90)
        while True:

            axsis_data = self.axsis_interface.get_traffic_counts(tmp_start_date, tmp_end_date)
            axsis_data = axsis_data.to_dict('index')
            columns = axsis_data[0].keys() - ['Location code', 'Description', 'First Traf Evt', 'Last Traf Evt']

            for row in axsis_data.values():
                for event_date in columns:
                    if not math.isnan(row[event_date]):
                        self._insert_or_update(AtvesTrafficCounts(location_code=str(row['Location code']).strip(),
                                                                  date=datetime.strptime(event_date,
                                                                                         '%m/%d/%Y').date(),
                                                                  count=int(row[event_date])))
            tmp_start_date = tmp_start_date + timedelta(days=91)
            if tmp_start_date > end_date:
                break

            # chunk the date range, unless we hit the end_date
            tmp_end_date = min(tmp_start_date + timedelta(days=90), end_date)

    def _process_traffic_count_data_conduent(self, start_date: date, end_date: date) -> None:
        if not self.conduent_interface:
            logger.warning('Unable to run _process_traffic_count_data_conduent. It requires a Conduent '
                           'session, which is not setup.')
            return

        # Get data from red light cameras
        conduent_data = self.conduent_interface.get_traffic_counts_by_location(start_date, end_date)
        for _, row in conduent_data.iterrows():
            self._insert_or_update(AtvesTrafficCounts(location_code=str(row['iLocationCode']).strip(),
                                                      date=datetime.strptime(row['Ddate'], '%m/%d/%Y').date(),
                                                      count=int(row['VehPass'])))

    def process_violations(self, start_date: date, end_date: date) -> None:
        """
        Processes the traffic count camera data from Axsis and Conduent
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :return:
        """
        logger.info('Processing violation data from {} to {}', start_date.strftime("%m/%d/%y"),
                    end_date.strftime("%m/%d/%y"))

        self.build_location_db()
        self.build_violation_lookup_db()
        self._process_violations_axsis(start_date, end_date)
        self._process_violations_conduent(start_date, end_date)

    def _process_violations_axsis(self, start_date: date, end_date: date) -> None:
        if not self.axsis_interface:
            logger.warning('Unable to run _process_traffic_count_data_axsis. It requires a Axsis session, which is not '
                           'setup.')
            return

        axsis_data = self.axsis_interface.get_location_summary_by_lane(start_date, end_date)
        for _, row in axsis_data.iterrows():
            for code, desc in ((1, 'Events still in WF'), (2, 'Non Events'), (2, 'PD Non Events'), (3, 'Controllable'),
                               (3, 'PD Controllable'), (4, 'Uncontrollable'), (4, 'PD Uncontrollable'),
                               (5, 'Citations Issued'), (5, 'Nov Issued'), (5, 'Warning Issued')):
                self._insert_or_update(AtvesViolations(date=row['Date'],
                                                       location_code=row['Location Code'],
                                                       count=row[desc],
                                                       violation_cat=code,
                                                       details=desc)
                                       )

    def _process_violations_conduent(self, start_date: date, end_date: date) -> None:
        violation_lookup = {
            '1- In Process': 1,
            '2- Conduent/City Non Violations': 2,
            '3- Conduent/City Rejects(Controllable)': 3,
            '4- Conduent/City Rejects(Uncontrollable)': 4,
            '5- Violations Mailed': 5
        }

        if not self.conduent_interface:
            logger.warning('Unable to run _process_traffic_count_data_conduent. It requires a Conduent '
                           'session, which is not setup.')
            return

        conduent_data = self.conduent_interface.get_client_summary_by_location(start_date, end_date)
        for _, row in conduent_data.iterrows():
            self._insert_or_update(AtvesViolations(date=datetime.strptime(row['Date'], '%m/%d/%y'),
                                                   location_code=row['Locations'],
                                                   count=row['DetailCount'],
                                                   violation_cat=violation_lookup[row['iOrderBy']],
                                                   details=row['vcDescription'])
                                   )

    def _insert_or_update(self, insert_obj: DeclarativeMeta, identity_insert=False) -> None:
        """
        A safe way for the sqlalchemy to insert if the record doesn't exist, or update if it does. Copied from
        trafficstat.crash_data_ingester
        :param insert_obj:
        :param identity_insert:
        :return:
        """
        session = Session(bind=self.engine, future=True)
        if identity_insert:
            session.execute(text('SET IDENTITY_INSERT {} ON'.format(insert_obj.__tablename__)))

        session.add(insert_obj)
        try:
            session.commit()
            logger.debug('Successfully inserted object: {}', insert_obj)
        except IntegrityError as insert_err:
            session.rollback()

            if '(544)' in insert_err.args[0]:
                # This is a workaround for an issue with sqlalchemy not properly setting IDENTITY_INSERT on for SQL
                # Server before we insert values in the primary key. The error is:
                # (pyodbc.IntegrityError) ('23000', "[23000] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]
                # Cannot insert explicit value for identity column in table <table name> when IDENTITY_INSERT is set to
                # OFF. (544) (SQLExecDirectW)")
                self._insert_or_update(insert_obj, True)

            elif '(2627)' in insert_err.args[0] or 'UNIQUE constraint failed' in insert_err.args[0]:
                # Error 2627 is the Sql Server error for inserting when the primary key already exists. 'UNIQUE
                # constraint failed' is the same for Sqlite
                cls_type = type(insert_obj)

                qry = session.query(cls_type)

                primary_keys = [i.key for i in sqlalchemyinspect(cls_type).primary_key]
                for primary_key in primary_keys:
                    qry = qry.filter(cls_type.__dict__[primary_key] == insert_obj.__dict__[primary_key])

                update_vals = {k: v for k, v in insert_obj.__dict__.items()
                               if not k.startswith('_') and k not in primary_keys}
                if update_vals:
                    qry.update(update_vals)
                    try:
                        session.commit()
                        logger.debug('Successfully inserted object: {}', insert_obj)
                    except IntegrityError as update_err:
                        logger.error('Unable to insert object: {}\nError: {}', insert_obj, update_err)

            else:
                raise AssertionError('Expected error 2627 or "UNIQUE constraint failed". Got {}'.format(insert_err)) \
                    from insert_err
        finally:
            if identity_insert:
                session.execute(text('SET IDENTITY_INSERT {} OFF'.format(insert_obj.__tablename__)))
            session.close()

    def get_lat_long(self, address):
        """
        Get the latitude and longitude for an address if the accuracy score is high enough
        :param address: Street address to search. The more complete the address, the better.
        """
        address = self._standardize_address(address)
        geo_dict = geocode("{}, Baltimore, MD".format(address))
        lat = None
        lng = None
        if geo_dict and geo_dict[0]['score'] > 90:
            lat = geo_dict[0]['location']['x']
            lng = geo_dict[0]['location']['y']
        return lat, lng

    @staticmethod
    def _standardize_address(street_address: str) -> str:
        """The original dataset has addresses formatted in various ways. This attempts to standardize them a bit"""
        street_address = street_address.upper()
        street_address = street_address.replace(' BLK ', ' ')
        street_address = street_address.replace(' BLOCK ', ' ')
        street_address = street_address.replace('JONES FALLS', 'I-83')
        street_address = street_address.replace('JONES FALLS EXPWY', 'I-83')
        street_address = street_address.replace('JONES FALLS EXPRESSWAY', 'I-83')
        street_address = street_address.replace(' HW', ' HWY')
        street_address = street_address.replace(' SB', '')
        street_address = street_address.replace(' NB', '')
        street_address = street_address.replace(' WB', '')
        street_address = street_address.replace(' EB', '')
        street_address = re.sub(r'^(\d*) N\.? (.*)', r'\1 NORTH \2', street_address)
        street_address = re.sub(r'^(\d*) S\.? (.*)', r'\1 SOUTH \2', street_address)
        street_address = re.sub(r'^(\d*) E\.? (.*)', r'\1 EAST \2', street_address)
        street_address = re.sub(r'^(\d*) W\.? (.*)', r'\1 WEST \2', street_address)

        return street_address


if __name__ == '__main__':
    lastmonth = date.today() - timedelta(days=30)
    parser = argparse.ArgumentParser(description='Traffic count importer')
    parser.add_argument('-m', '--month', type=int, default=lastmonth.month,
                        help=('Optional: Month of date we should start searching on (IE: 10 for Oct). Defaults to '
                              'yesterday if not specified'))
    parser.add_argument('-d', '--day', type=int, default=lastmonth.day,
                        help=('Optional: Day of date we should start searching on (IE: 5). Defaults to yesterday if '
                              'not specified'))
    parser.add_argument('-y', '--year', type=int, default=lastmonth.year,
                        help=('Optional: Four digit year we should start searching on (IE: 2020). Defaults to '
                              'yesterday if not specified'))
    parser.add_argument('-n', '--numofdays', default=30, type=int,
                        help='Optional: Number of days to search, including the start date.')
    parser.add_argument('-a', '--allcams', action='store_true', help="Process all camera types")
    parser.add_argument('-o', '--oh', action='store_true', help="Process only over height cameras")
    parser.add_argument('-r', '--rl', action='store_true', help="Process only red light cameras")
    parser.add_argument('-t', '--tc', action='store_true', help="Process only traffic counts")
    parser.add_argument('-b', '--builddb', action='store_true',
                        help="Rebuilds (or updates) the camera location database")

    args = parser.parse_args()
    ad = AtvesDatabase(conn_str='mssql+pyodbc://balt-sql311-prd/DOT_DATA?driver=ODBC Driver 17 for SQL Server',
                       axsis_user=AXSIS_USERNAME, axsis_pass=AXSIS_PASSWORD, conduent_user=CONDUENT_USERNAME,
                       conduent_pass=CONDUENT_PASSWORD)

    _start_date = date(args.year, args.month, args.day)
    _end_date = (date(args.year, args.month, args.day) + timedelta(days=args.numofdays - 1))

    all_cams = bool(args.allcams or not any([args.oh, args.rl, args.tc]))

    # Process traffic cameras
    if args.tc or all_cams:
        ad.process_traffic_count_data(_start_date, _end_date)

    # Process over height cameras
    if args.oh or all_cams:
        ad.process_conduent_reject_numbers(_start_date, _end_date, OVERHEIGHT)
        ad.process_conduent_data_by_location(_start_date, _end_date, OVERHEIGHT)
        ad.process_conduent_data_approval_by_review_date(_start_date, _end_date, OVERHEIGHT)

    # Process red light cameras
    if args.rl or all_cams:
        ad.process_conduent_reject_numbers(_start_date, _end_date, REDLIGHT)
        ad.process_conduent_data_by_location(_start_date, _end_date, REDLIGHT)
        ad.process_conduent_data_amber_time(_start_date, _end_date)
        ad.process_conduent_data_approval_by_review_date(_start_date, _end_date, REDLIGHT)
