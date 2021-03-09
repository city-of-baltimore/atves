"""Pulls data through the Conduent and Axsis libraries, and inserts it into the database"""
import math
import re
from datetime import date, datetime, timedelta
from typing import Optional

from balt_geocoder.geocoder import APIFatalError, Geocoder
from loguru import logger
from sqlalchemy import create_engine, event, inspect as sqlalchemyinspect  # type: ignore
from sqlalchemy.exc import IntegrityError  # type: ignore
from sqlalchemy.ext.declarative import DeclarativeMeta  # type: ignore
from sqlalchemy.orm import Session  # type: ignore
from sqlalchemy.sql import text  # type: ignore
from sqlalchemy.engine import Engine  # type: ignore
from sqlite3 import Connection as SQLite3Connection

from atves.atves_schema import AtvesAmberTimeRejects, AtvesApprovalByReviewDateDetails, AtvesByLocation, \
    AtvesCamLocations, AtvesTicketCameras, AtvesTrafficCounts, Base
from atves.axsis import Axsis
from atves.conduent import Conduent, ALLCAMS, REDLIGHT, OVERHEIGHT
from atves.creds import AXSIS_USERNAME, AXSIS_PASSWORD, CONDUENT_USERNAME, CONDUENT_PASSWORD, GAPI


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


class AtvesDatabase:
    """ Helper class for the Coduent and Axsis classes that inserts data into the relevant databases"""

    def __init__(self, conn_str: str, atves_user: str = AXSIS_USERNAME,  # pylint:disable=too-many-arguments
                 atves_pass: str = AXSIS_PASSWORD, conduent_user: str = CONDUENT_USERNAME,
                 conduent_pass: str = CONDUENT_PASSWORD, geocodio_api_key: str = GAPI[0]):
        """
        :param conn_str: sqlalchemy connection string (IE sqlite:///crash.db or
        Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;)
        """

        logger.info('Creating db with connection string: {}', conn_str)
        self.engine = create_engine(conn_str, echo=True, future=True)

        with self.engine.begin() as connection:
            Base.metadata.create_all(connection)

        self.axsis_interface = Axsis(username=atves_user, password=atves_pass)
        self.conduent_interface = Conduent(conduent_user, conduent_pass)
        self.geocoder = Geocoder(geocodio_api_key)

    def build_location_db(self) -> None:
        """
        Builds the location database with each camera and their lat/long
        :return: None
        """
        self._build_db_conduent_red_light()
        self._build_db_conduent_overheight()
        self._build_db_speed_cameras()

        with Session(bind=self.engine, future=True) as session:
            # Look for missing location ids
            location_codes = session.query(AtvesAmberTimeRejects.location_code).all()
            location_codes += session.query(AtvesByLocation.location_code).all()
            location_codes += session.query(AtvesTrafficCounts.location_code).all()

            existing_location_codes = session.query(AtvesCamLocations.location_code).all()

            diff = list(set(location_codes) - set(existing_location_codes))
            if diff:
                raise AssertionError("Missing location codes: {}".format(diff))

            # look for missing locations
            locations = session.query(AtvesTicketCameras.location).all()
            existing_locations = session.query(AtvesCamLocations.locationdescription).all()
            diff = list(locations - existing_locations)

            if diff:
                raise AssertionError("Missing locations: {}".format(diff))

    def _build_db_conduent_red_light(self) -> None:
        """Builds the camera location database for red light cameras"""
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
                geo = self.geocoder.geocode("{}, Baltimore, MD".format(ret['location']))
                self._insert_or_update(AtvesCamLocations(
                    location_code=str(ret['site_code']),
                    locationdescription=str(ret['location']),
                    lat=geo.get('latitude') if geo else None,
                    long=geo.get('longitude') if geo else None,
                    cam_type=str(ret['cam_type']),
                    effective_date=datetime.strptime(ret['effective_date'], '%b %d, %Y'),
                    speed_limit=int(ret['speed_limit']),
                    status=bool(ret['status'] == 'Active')))
            except (RuntimeError, APIFatalError) as err:
                logger.warning("Geocoder error: {}", err)

    def _build_db_conduent_overheight(self) -> None:
        """Builds the camera location database for over height cameras"""
        oh_list = self.conduent_interface.get_overheight_cameras()

        for location_code, location in oh_list:
            geo = self.geocoder.geocode("{}, Baltimore, MD".format(location))
            self._insert_or_update(AtvesCamLocations(location_code=location_code,
                                                     locationdescription=location,
                                                     lat=geo.get('latitude') if geo else None,
                                                     long=geo.get('longitude') if geo else None,
                                                     cam_type='OH',
                                                     effective_date=None,
                                                     speed_limit=None,
                                                     status=None))

    def _build_db_speed_cameras(self) -> None:
        """Builds the camera location database for speed cameras"""
        # Get the list of location codes in the traffic count database (AXSIS)
        with Session(bind=self.engine, future=True) as session:
            # get all cameras used in the last 30 days

            location_codes_needed = self.axsis_interface.get_traffic_counts(start_date=date.today()-timedelta(days=30),
                                                                            end_date=date.today())\
                .get('Location code').to_list()

            location_codes_needed += session.query(AtvesTrafficCounts.location_code). \
                filter(AtvesTrafficCounts.location_code.like('BAL%')).all()

            location_codes_existing = session.query(AtvesCamLocations.location_code). \
                filter(AtvesCamLocations.location_code.like('BAL%')).all()

            diff = set(location_codes_needed) - set(location_codes_existing)

            for location_code in diff:
                if not location_code:
                    continue

                # First, lets get a date when this camera existed
                traffic_counts = session.query(AtvesTrafficCounts.date). \
                    filter(AtvesTrafficCounts.location_code == location_code).all()

                if traffic_counts:
                    cam_date: Optional[datetime] = datetime.strptime(traffic_counts[0][1], "%Y-%m-%d")
                    axsis_data = self.axsis_interface.get_traffic_counts(cam_date, cam_date)
                    location = [x for x in axsis_data.values.tolist() if x[0] == location_code][0][1]
                else:
                    cam_date = None
                    location = self.axsis_interface.get_location_info(location_code)

                if not location:
                    continue

                geo = self.geocoder.geocode("{}, Baltimore, MD".format(location))
                lat = geo.get('latitude') if geo else None
                lng = geo.get('longitude') if geo else None
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
        data = self.conduent_interface.get_deployment_data(start_date, end_date, cam_type)

        if not data:
            return

        for row in data:
            self._insert_or_update(AtvesTicketCameras(id=int(row['id']),
                                                      start_time=row['start_time'],
                                                      end_time=row['end_time'],
                                                      location=str(row['location']),
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
                                                                  date=datetime.strptime(event_date, '%m/%d/%Y').date(),
                                                                  count=int(row[event_date])))
            tmp_start_date = tmp_start_date + timedelta(days=91)
            if tmp_start_date > end_date:
                break

            tmp_end_date = tmp_start_date + timedelta(days=90)
            if tmp_end_date > end_date:
                tmp_end_date = end_date

        # Get data from red light cameras
        conduent_data = self.conduent_interface.get_traffic_counts_by_location(start_date, end_date)
        for _, row in conduent_data.iterrows():
            self._insert_or_update(AtvesTrafficCounts(location_code=str(row['iLocationCode']).strip(),
                                                      date=datetime.strptime(row['Ddate'], '%m/%d/%Y').date(),
                                                      count=int(row['VehPass'])))

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
