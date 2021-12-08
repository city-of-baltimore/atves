"""Pulls data through the Conduent and Axsis libraries, and inserts it into the database"""
import argparse
import inspect
import math
import os
import re
import sys
import warnings
from datetime import date, datetime, timedelta
from json.decoder import JSONDecodeError
from sqlite3 import Connection as SQLite3Connection
from typing import Optional, Tuple

from arcgis.geocoding import geocode  # type: ignore
from arcgis.gis import GIS  # type: ignore
from databasebaseclass.base import DatabaseBaseClass
from loguru import logger
from sqlalchemy import create_engine, event  # type: ignore
from sqlalchemy.engine import Engine  # type: ignore
from sqlalchemy.orm import Session  # type: ignore
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from atves.constants import ALLCAMS, REDLIGHT, OVERHEIGHT, SPEED
from atves.atves_schema import AtvesAmberTimeRejects, AtvesCamLocations, AtvesFinancial, AtvesRejectReason, \
    AtvesTrafficCounts, AtvesViolations, AtvesViolationCategories, Base
from atves.axsis import Axsis
from atves.conduent import Conduent
from atves.creds import AXSIS_USERNAME, AXSIS_PASSWORD, CONDUENT_USERNAME, CONDUENT_PASSWORD, REPORT_USERNAME, \
    REPORT_PASSWORD
from atves.financial import CobReports

GIS()


@event.listens_for(Engine, 'connect')
def _set_sqlite_pragma(dbapi_connection, connection_record):  # pylint:disable=unused-argument
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute('PRAGMA foreign_keys=ON;')
        cursor.close()


VIOLATION_TYPES = {
    # To handle parse errors
    0: 'Unknown',
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


class AtvesDatabase(DatabaseBaseClass):
    """ Helper class for the Conduent and Axsis classes that inserts data into the relevant databases"""

    def __init__(self, conn_str: str, axsis_user: Optional[str] = AXSIS_USERNAME,  # pylint:disable=too-many-arguments
                 axsis_pass: Optional[str] = AXSIS_PASSWORD, conduent_user: Optional[str] = CONDUENT_USERNAME,
                 conduent_pass: Optional[str] = CONDUENT_PASSWORD, report_user: Optional[str] = REPORT_USERNAME,
                 report_pass: Optional[str] = REPORT_PASSWORD):
        """
        :param conn_str: sqlalchemy connection string (IE sqlite:///crash.db or
        Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;)
        :param axsis_user: username for https://webportal1.atsol.com/axsis.web/Account/Login
        :param axsis_pass: password for https://webportal1.atsol.com/axsis.web/Account/Login
        :param conduent_user: username for https://cw3.cite-web.com/loginhub/Main.aspx
        :param conduent_pass: password for https://cw3.cite-web.com/loginhub/Main.aspx
        :param report_user: username for https://cobrpt02.rsm.cloud/ReportServer
        :param report_pass: password for https://cobrpt02.rsm.cloud/ReportServer
        """
        logger.info('Creating db with connection string: {}', conn_str)
        self.engine = create_engine(conn_str, echo=True, future=True)

        with self.engine.begin() as connection:
            Base.metadata.create_all(connection)

        self.axsis_interface = Axsis(username=axsis_user, password=axsis_pass) if axsis_user and axsis_pass else None
        self.conduent_interface = Conduent(conduent_user, conduent_pass) if conduent_user and conduent_pass else None
        self.financial_interface = CobReports(report_user, report_pass) if report_user and report_pass else None

        self.location_db_built = False
        self.violation_lookup_db_built = False

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
            location_codes = session.query(AtvesViolations.location_code).all()
            location_codes += session.query(AtvesTrafficCounts.location_code).all()

            existing_location_codes = session.query(AtvesCamLocations.location_code).all()

            diff = set(location_codes) - set(existing_location_codes)
            if diff:
                raise AssertionError('Missing location codes: {diff}')

        self.location_db_built = True

    def build_violation_lookup_db(self) -> None:
        """Builds a violation description lookup table"""
        if self.violation_lookup_db_built:
            return
        for vio_key, vio_desc in VIOLATION_TYPES.items():
            self._insert_or_update(AtvesViolationCategories(violation_cat=vio_key, description=vio_desc))
        self.violation_lookup_db_built = True

    def _build_db_conduent_red_light(self) -> None:
        """Builds the camera location database for red light cameras"""
        self._build_db_conduent(REDLIGHT)

    def _build_db_conduent_overheight(self) -> None:
        """Builds the camera location database for over height cameras"""
        self._build_db_conduent(OVERHEIGHT)

    def _build_db_conduent(self, cam_type: int) -> None:
        """
        Builds the camera location database

        :param cam_type: Type of camera data to pull (use the constants conduent.REDLIGHT or conduent.OVERHEIGHT
        :return: None
        """
        if not self.conduent_interface:
            logger.warning('Unable to run {}. No Conduent session is setup.', inspect.stack()[0][3])
            return

        failures = 0
        loc_id = 0

        # we use the 'failures' because we don't know the range of valid location ids
        while failures <= 50:
            loc_id += 1
            ret = self.conduent_interface.get_location_by_id(loc_id, cam_type)
            if ret['site_code'] is None:
                failures += 1
                continue

            try:
                lat, lng = self.get_lat_long(ret['location'])
                cam_start_date, cam_end_date = self._get_cam_start_end(str(ret['site_code']))
                if ret['effective_date'] is not None:
                    cam_start_date = datetime.strptime(ret['effective_date'], '%b %d, %Y').date()

                days_active = (cam_end_date - cam_start_date).days if cam_start_date and cam_end_date else None

                speed_limit = int(ret['speed_limit']) if ret['speed_limit'] is not None else 0
                self._insert_or_update(AtvesCamLocations(
                    location_code=str(ret['site_code']),
                    locationdescription=str(ret['location']),
                    lat=lat,
                    long=lng,
                    cam_type=str(ret['cam_type']),
                    effective_date=cam_start_date,
                    last_record=cam_end_date,
                    days_active=days_active,
                    speed_limit=speed_limit,
                    status=bool(ret['status'] == 'Active')))
            except RuntimeError as err:
                logger.warning('Geocoder error: {}', err)

    def _build_db_speed_cameras(self) -> bool:
        """Builds the camera location database for speed cameras"""
        if not self.axsis_interface:
            logger.warning('Unable to run _build_db_speed_cameras. It requires a Axsis session, which is not setup.')
            return False

        # Get the list of location codes in the traffic count database (AXSIS) from the last 30 days
        report_details = self.axsis_interface.get_reports_detail('LOCATION PERFORMANCE DETAIL')
        if report_details is None or report_details.get('Parameters') is None:
            logger.error('Unable to get speed camera information')
            return False

        active_cams = [param_elem.get('Description').split(' - ')
                       for param in report_details['Parameters']
                       if param['ParmTitle'] == 'Violation Locations' and param['ParmList'] is not None
                       for param_elem in param['ParmList']]

        for location_code, location in active_cams:
            lat: Optional[float] = None
            lng: Optional[float] = None

            if not location_code:
                continue

            cam_start_date, cam_end_date = self._get_cam_start_end(location_code)

            # if the location was specified, then lets look it up
            if location:
                lat, lng = self.get_lat_long(location)
                if not (lat and lng):
                    continue

            days_active = (cam_end_date - cam_start_date).days if cam_start_date and cam_end_date else None

            self._insert_or_update(AtvesCamLocations(location_code=location_code,
                                                     locationdescription=location,
                                                     lat=lat,
                                                     long=lng,
                                                     cam_type='SC',
                                                     effective_date=cam_start_date,
                                                     last_record=cam_end_date,
                                                     days_active=days_active,
                                                     speed_limit=None,
                                                     status=None))

        return True

    def _get_cam_start_end(self, location_code: str) -> Tuple[Optional[date], Optional[date]]:
        """
        Gets the camera activity dates based on traffic data or violation data
        :param location_code: Camera location code that matches the camera location table
        :return: start and end date for the camera; active cameras are still given an end date
        """
        cam_start_date: Optional[date] = None
        cam_end_date: Optional[date] = None

        with Session(bind=self.engine, future=True) as session:
            # First, lets get a date when this camera existed... lets look for traffic counts first
            traffic_counts = session.query(AtvesTrafficCounts.date). \
                filter(AtvesTrafficCounts.location_code == location_code)

            if traffic_counts.order_by(AtvesTrafficCounts.date).first():
                cam_start_date = traffic_counts.order_by(AtvesTrafficCounts.date).first()[0]

            if traffic_counts.order_by(AtvesTrafficCounts.date.desc()).first():
                cam_end_date = traffic_counts.order_by(AtvesTrafficCounts.date.desc()).first()[0]

            # if there were no traffic counts, lets look for issued violations
            if not cam_start_date:
                ret = session.query(AtvesViolations.date) \
                    .filter(AtvesViolations.location_code == location_code)

                if ret.order_by(AtvesViolations.date).first():
                    cam_start_date = ret.first()[0]

                if ret.order_by(AtvesViolations.date.desc()).first():
                    cam_end_date = ret.order_by(AtvesViolations.date.desc()).first()[0]

        return cam_start_date, cam_end_date

    def process_conduent_data_amber_time(self, start_date: date, end_date: date, build_loc_db: bool = True,
                                         force: bool = False) -> None:
        """
        Pulls the amber time report

        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param build_loc_db: If true, then it will rebuild the location db even if it was already built this session
        :param force: Pulls data on the whole range of dates; by default it skips dates that already have data in the
        database
        :return: None
        """
        logger.info('Processing conduent amber time report from {} to {}', start_date.strftime('%m/%d/%y'),
                    end_date.strftime('%m/%d/%y'))

        if not self.conduent_interface:
            logger.warning('Unable to run process_conduent_data_amber_time. It requires a Conduent session, which is '
                           'not setup.')
            return

        self.build_location_db(build_loc_db)
        dates = self.get_dates_to_process(start_date, end_date, AtvesAmberTimeRejects.violation_date, force)
        for working_date in dates:
            if (data := self.conduent_interface.get_amber_time_rejects_report(working_date, working_date)).empty:
                # no data
                continue

            for _, row in data.iterrows():  # pylint:disable=no-member
                self._insert_or_update(AtvesAmberTimeRejects(
                    location_code=int(row['iLocationCode']),
                    deployment_no=int(row['Deployment Number']),
                    violation_date=row['VioDate'],
                    amber_time=float(row['Amber Time']),
                    amber_reject_code=str(row['Amber Reject Code']),
                    event_number=int(row['Event Number'])))

    def process_traffic_count_data(self, start_date: date, end_date: date, force: bool = False) -> None:
        """
        Processes the traffic count camera data from Axsis and Conduent
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param force: Pulls data on the whole range of dates; by default it skips dates that already have data in the
        database
        :return:
        """
        logger.info('Processing traffic count data from {} to {}', start_date.strftime('%m/%d/%y'),
                    end_date.strftime('%m/%d/%y'))

        self.build_location_db()
        self._process_traffic_count_data_axsis(start_date, end_date, force)
        self._process_traffic_count_data_conduent(start_date, end_date, force)

    def _process_traffic_count_data_axsis(self, start_date: date, end_date: date, force: bool = False) -> None:
        if not self.axsis_interface:
            logger.warning('Unable to run _process_traffic_count_data_axsis. It requires a Axsis session, which is not '
                           'setup.')
            return

        dates = self.get_dates_to_process(start_date, end_date, AtvesTrafficCounts.date, force)
        for working_date in dates:
            if (data := self.axsis_interface.get_traffic_counts(working_date, working_date)).empty:
                # no data
                continue

            data = data.to_dict('index')
            columns = data[0].keys() - ['Location code', 'Description', 'First Traf Evt', 'Last Traf Evt']

            for row in data.values():
                for event_date in columns:
                    if not math.isnan(row[event_date]):
                        self._insert_or_update(AtvesTrafficCounts(location_code=str(row['Location code']).strip(),
                                                                  date=datetime.strptime(event_date, '%m/%d/%Y').date(),
                                                                  count=int(row[event_date])))

    def _process_traffic_count_data_conduent(self, start_date: date, end_date: date, force: bool = False) -> None:
        if not self.conduent_interface:
            logger.warning('Unable to run _process_traffic_count_data_conduent. It requires a Conduent '
                           'session, which is not setup.')
            return

        dates = self.get_dates_to_process(start_date, end_date, AtvesTrafficCounts.date, force)
        for working_date in dates:
            if (data := self.conduent_interface.get_traffic_counts_by_location(working_date, working_date)).empty:
                # no data
                continue

            for _, row in data.iterrows():  # pylint:disable=no-member
                self._insert_or_update(AtvesTrafficCounts(location_code=str(row['iLocationCode']).strip(),
                                                          date=row['Ddate'],
                                                          count=int(row['VehPass'])))

    def process_violations(self, start_date: date, end_date: date, force: bool = False) -> None:
        """
        Processes the camera violations from Axsis and Conduent
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param force: Pulls data on the whole range of dates; by default it skips dates that already have data in the
        database
        :return:
        """
        logger.info('Processing violation data from {} to {}', start_date.strftime('%m/%d/%y'),
                    end_date.strftime('%m/%d/%y'))

        self.build_location_db()
        self.build_violation_lookup_db()

        dates = self.get_dates_to_process(start_date, end_date, AtvesViolations.date, force)
        for working_date in dates:
            self._process_violations_axsis(working_date, working_date)
            self._process_violations_conduent(working_date, working_date)

    def _process_violations_axsis(self, start_date: date, end_date: date) -> None:
        if not self.axsis_interface:
            logger.warning('Unable to run _process_violations_axsis. It requires a Axsis session, which is not '
                           'setup.')
            return

        if (data := self.axsis_interface.get_location_summary_by_lane(start_date, end_date)).empty:
            # no data
            return

        for _, row in data.iterrows():
            for code, desc in ((1, 'Events still in WF'),
                               (2, 'Non Events'),
                               (2, 'PD Non Events'),
                               (3, 'Controllable'),
                               (3, 'PD Controllable'),
                               (4, 'Uncontrollable'),
                               (4, 'PD Uncontrollable'),
                               (5, 'Citations Issued'),
                               (5, 'Nov Issued'),
                               (5, 'Warning Issued')):
                self._insert_or_update(AtvesViolations(date=row['Date'],
                                                       location_code=str(row['Location Code']),
                                                       count=row[desc],
                                                       violation_cat=code,
                                                       details=desc))

    def _process_violations_conduent(self, start_date: date, end_date: date, cam_type: int = ALLCAMS) -> None:
        """

        :param cam_type: Either conduent.REDLIGHT or conduent.OVERHEIGHT
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :return:
        """
        def _get_int(value) -> int:
            """Gets int on the front of the string"""
            pattern = re.compile(r'(\d*)')
            ret = 0
            if value != 'All Locations':
                match = pattern.match(value)
                if match is None or match.lastindex is None or match.lastindex < 1:
                    logger.error('Unable to parse location {}', value)
                    ret = 0
                else:
                    ret = int(match.group(1))
            return ret

        violation_lookup = {
            '1- In Process': 1,
            '2- Conduent/City Non Violations': 2,
            '3- Conduent/City Rejects(Controllable)': 3,
            '4- Conduent/City Rejects(Uncontrollable)': 4,
            '5- Violations Mailed': 5
        }

        if not self.conduent_interface:
            logger.warning('Unable to run _process_violations_conduent. It requires a Conduent session, which is not '
                           'setup.')
            return

        if cam_type == ALLCAMS:
            self._process_violations_conduent(start_date, end_date, REDLIGHT)
            self._process_violations_conduent(start_date, end_date, OVERHEIGHT)
            return

        logger.info('Processing conduent location data reports from {} to {}', start_date.strftime('%m/%d/%y'),
                    end_date.strftime('%m/%d/%y'))

        if (data := self.conduent_interface.get_client_summary_by_location(start_date, end_date)).empty:
            # no data
            return
        for _, row in data.iterrows():
            location_id = _get_int(row['Locations'])
            if location_id == 0:
                continue
            self._insert_or_update(AtvesViolations(date=row['Date'],
                                                   location_code=str(location_id),
                                                   count=int(row['DetailCount']),
                                                   violation_cat=violation_lookup[row['iOrderBy']],
                                                   details=str(row['vcDescription'])))

    def process_financials(self, start_date: date, end_date: date, cam_type: int = ALLCAMS,
                           force: bool = False) -> None:
        """
        Get the financial data for the ATVES program
        :param start_date: First date (inclusive) to process
        :param end_date: Last date (inclusive) to process
        :param cam_type: Indicates the camera type to pull, from `atves.constants`
        :param force: Pulls data on the whole range of dates; by default it skips dates that already have data in the
        database
        """
        dates = self.get_dates_to_process(start_date, end_date, AtvesFinancial.ledger_posting_date, force)
        for working_date in dates:
            if cam_type in [ALLCAMS, OVERHEIGHT]:
                self._process_overheight_financials(working_date, working_date)

            if cam_type in [ALLCAMS, REDLIGHT]:
                self._process_redlight_financials(working_date, working_date)

            if cam_type in [ALLCAMS, SPEED]:
                self._process_speed_financials(working_date, working_date)

    def _process_overheight_financials(self, start_date: date, end_date: date) -> None:
        """
        Get the data related to the overheight automated enforcement program and put it in the database
        Accounts: 1001-000000-2030-794100-403752 Commercial Truck Enforcement
        :param start_date: First date (inclusive) to process
        :param end_date: Last date (inclusive) to process
        """
        self._insert_financials_by_account('10010000002030794100403752', start_date, end_date)

    def _process_redlight_financials(self, start_date: date, end_date: date) -> None:
        """
        Get the data related to the redlight automated enforcement program and put it in the database
        Accounts:
            A001-191-203-00-0-00 Red Light Fines
            A001-192-203-00-0-00 Right Turn on Red Fines
            1001-697-006-00-3-51 General-n/a-Traffic Safety-Traffic Cameras-Payments/sub-Contractors:
                CONDUENT STATE & LOCAL SOLUTIONS, INC. (probably red light and overheight)
            1001-697-006-00-3-25 General-n/a-Traffic Safety-Traffic Cameras-Rental Of Operating Equipment
                (probably red light and overheight)
        :param start_date: First date (inclusive) to process
        :param end_date: Last date (inclusive) to process
        """
        for acct in ['A00119120300000', 'A00119220300000', '100169700600351', '100169700600325']:
            self._insert_financials_by_account(acct, start_date, end_date)

    def _process_speed_financials(self, start_date: date, end_date: date) -> None:
        """
        Get the data related to the overheight automated enforcement program and put it in the database
        Accounts:
            A001-193-203-00-0-00 General-n/a-Traffic-Speed Cameras-Speed Cameras
            1001-697-006-00-3-51 General-n/a-Traffic Safety-Traffic Cameras-Payments/sub-Contractors:
                American Traffic Solutions, Inc.
            1001-697-013-00-3-25 General-n/a-Traffic Safety-Speed Camera Violations-Rental Of Operating
                Equipment
        :param start_date: First date (inclusive) to process
        :param end_date: Last date (inclusive) to process
        """
        for acct in ['A00119320300000', '100169700600351', '100169701300325']:
            self._insert_financials_by_account(acct, start_date, end_date)

    def _insert_financials_by_account(self, account: str, start_date: date, end_date: date) -> None:
        if not self.financial_interface:
            logger.warning('Unable to insert financial data. It requires a reports session, which is not setup.')
            return

        if (data := self.financial_interface.get_general_ledger_detail(start_date, end_date, account, '55')).empty:
            # no data
            return
        for _, row in data.iterrows():  # pylint:disable=no-member
            self._insert_or_update(AtvesFinancial(
                journal_entry_no=row['JournalEntryNo'],
                ledger_posting_date=row['LedgerPostingDate'],
                account_no=row['AccountNo'],
                legacy_account_no=row['LegacyAccountNo'],
                amount=float(row['Amount']),
                source_journal=row['SourceJournal'],
                trx_reference=row['TrxReference'],
                TrxDescription=row['TrxDescription'],
                user_who_posted=row['UserWhoPosted'],
                trx_no=row['TrxNo'],
                vendorid_or_customerid=row['VendorIDOrCustomerID'],
                vendor_or_customer_name=row['VendorOrCustomerName'],
                document_no=row['DocumentNo'],
                Trx_source=row['TrxSource'],
                account_description=row['AccountDescription'],
                account_type=row['AccountType'],
                agency_or_category=row['AgencyOrCategory']))

    def process_officer_actions(self, start_date: date, end_date: date, force: bool = False) -> None:
        """
        Inserts the citation rejection information into the database
        :param start_date: First date (inclusive) to process
        :param end_date: Last date (inclusive) to process
        :param force
        """
        if not self.axsis_interface:
            logger.warning('Unable to run _process_violations_axsis. It requires a Axsis session, which is not '
                           'setup.')
            return

        dates = self.get_dates_to_process(start_date, end_date, AtvesRejectReason.date, force)
        for working_date in dates:
            if (data := self.axsis_interface.get_officer_actions(working_date, working_date))['1'].empty:
                # no data
                continue

            for _, row in data['1'].iterrows():
                self._insert_or_update(AtvesRejectReason(
                    date=row['Date'],
                    reject_reason=row['Reject Reason Factors'],
                    pd_review=row['PD Review'],
                    supervisor_review=row['Supervisor Review'],
                    total=row['Total Count']
                ))

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=(retry_if_exception_type(JSONDecodeError)))
    def get_lat_long(self, address) -> Tuple[Optional[float], Optional[float]]:
        """
        Get the latitude and longitude for an address if the accuracy score is high enough
        :param address: Street address to search. The more complete the address, the better.
        """
        logger.debug(f'Looking up {address}')
        address = self._standardize_address(address)
        with warnings.catch_warnings():  # https://github.com/Esri/arcgis-python-api/issues/1090
            warnings.simplefilter("ignore")
            geo_dict = geocode(f'{address}, Baltimore, MD')
        lat = None
        lng = None
        if geo_dict and geo_dict[0]['score'] > 80:
            lat = float(geo_dict[0]['location']['y'])
            lng = float(geo_dict[0]['location']['x'])
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
        street_address = street_address.replace('BLKLOCH', 'LOCH')
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


def setup_parser(help_str):
    """Factory that creates the base argument parser"""
    parser = argparse.ArgumentParser(description=help_str)
    parser.add_argument('-v', '--verbose', action='store_true', help='Increased logging level')
    parser.add_argument('-vv', '--debug', action='store_true', help='Print debug statements')
    parser.add_argument('-c', '--conn_str', help='Database connection string',
                        default='mssql+pyodbc://balt-sql311-prd/DOT_DATA?driver=ODBC Driver 17 for SQL Server')

    return parser


def setup_logging(debug: bool = False, verbose: bool = False) -> None:
    """
    Configures the logging level, and sets up file based logging

    :param debug: If true, the Debug logging level is used, and verbose is ignored
    :param verbose: If true and debug is false, then the info log level is used
    """
    # Setup logging
    log_level = 'WARNING'
    if debug:
        log_level = 'DEBUG'
    elif verbose:
        log_level = 'INFO'

    logger.add(sys.stdout, format="<green>{time}</green> <level>{message}</level>", colorize=True, backtrace=True,
               diagnose=True, level=log_level)
    logger.add(os.path.join('logs', 'file-{time}.log'), format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               serialize=True, backtrace=True, diagnose=True, rotation='1 week', retention='3 months',
               compression='zip', level=log_level)


def parse_args(_args):
    """Handles the argument parsing"""
    parser = setup_parser('Data importer from the ATVES data providers')
    start_date = date.today() - timedelta(days=90)
    end_date = date.today() - timedelta(days=1)
    parser.add_argument('-s', '--startdate', type=date.fromisoformat, default=start_date,
                        help='First date to process, inclusive (format YYYY-MM-DD). Defaults to 90 days ago')
    parser.add_argument('-e', '--enddate', type=date.fromisoformat, default=end_date,
                        help='Last date to process, inclusive (format YYYY-MM-DD). Defaults to yesterday.')
    parser.add_argument('-b', '--builddb', action='store_true',
                        help='Rebuilds (or updates) the camera location database')
    parser.add_argument('-f', '--force', action='store_true', help='By default, this will only pull data for dates that'
                                                                   ' have no data in the database. This will force it '
                                                                   'to pull all data again.')

    return parser.parse_args(_args)


if __name__ == '__main__':
    args = parse_args(sys.argv[1:])
    setup_logging(args.debug, args.verbose)

    ad = AtvesDatabase(conn_str=args.conn_str,
                       axsis_user=AXSIS_USERNAME,
                       axsis_pass=AXSIS_PASSWORD,
                       conduent_user=CONDUENT_USERNAME,
                       conduent_pass=CONDUENT_PASSWORD,
                       report_user=REPORT_USERNAME,
                       report_pass=REPORT_PASSWORD)

    ad.process_traffic_count_data(args.startdate, args.enddate, force=args.force)
    ad.process_violations(args.startdate, args.enddate, force=args.force)
    ad.process_financials(args.startdate, args.enddate, force=args.force)
    ad.process_conduent_data_amber_time(args.startdate, args.enddate, force=args.force)
    ad.process_officer_actions(args.startdate, args.enddate, force=args.force)
    ad.build_location_db(args.builddb)
