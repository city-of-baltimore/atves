"""Python wrapper around the Axsis Mobility Platform"""
import ast
from datetime import date, timedelta
from enum import Enum
from io import BytesIO
from typing import cast, Dict, Optional

import pandas as pd  # type: ignore
import requests
import xlrd  # type: ignore
from bs4 import BeautifulSoup  # type: ignore
from loguru import logger
from retry import retry
from urllib3 import util  # type: ignore

from atves.axsis_types import ReportsDetailType

ACCEPT_HEADER = ("text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/"
                 "signed-exchange;v=b3;q=0.9")

util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'


# Report types
class Reports(Enum):
    """Defines used for Axsis._get_report"""
    TRAFFIC_COUNTS = 1
    LOCATION_SUMMARY = 2


def log_and_validate_params(func):
    """Adds logging to the beginning of functions that call _get_report"""
    def wrapper(self, start_date, end_date, *args, **kwargs):
        logger.info('Getting data for {} from {} to {}', func.__name__, start_date, end_date)
        if (end_date - start_date).days > 90:
            logger.warning('Axsis has issues generating reports with over 90 days of content')
        return func(self, start_date, end_date, *args, **kwargs)

    return wrapper


class Axsis:
    """Interface to the Axsis Mobility Platform"""

    def __init__(self, username: str, password: str):
        """
        :param username: Case sensitive username used to log into Axsis
        :param password: Case sensitive password used to log into Axsis
        """
        logger.debug("Creating session for user {}", username)

        self.session = requests.Session()
        self.username = username
        self.password = password
        self.client_id = None
        self.client_code = None
        self._login()

    def _get_report(self, parameters: ReportsDetailType, report_type: Reports) -> requests.Response:
        reports = {
            Reports.TRAFFIC_COUNTS: {
                'desc': 'SITE ACTIVITY BY TRAFFIC EVENTS',
                'filename': ('http://biportal/enterprisereportingservices/Reports/'
                             'AXSIS Report/Site_Activity_by_Traffic_Events_AXSIS.rdl'),
            },
            Reports.LOCATION_SUMMARY: {
                'desc': 'LOCATION PERFORMANCE SUMMARY BY LANE -- XML',
                'filename': 'REPORT_LPSL.XML',
            }
        }

        response = self.session.post('https://webportal1.atsol.com/Axsis.Web/api/Report/PostCacheReportFile',
                                     headers={
                                         'Accept': 'application/json, text/javascript, */*; q=0.01',
                                         'Content-Type': 'application/json;charset=UTF-8',
                                         'Origin': 'https://webportal1.atsol.com',
                                     }, data=self._depythonify_literal(parameters))

        guid = response.content[1:-1]

        headers = {
            'Accept': ACCEPT_HEADER,
        }

        params = (
            ('user', self.username),
            ('guid', guid),
            ('filename', reports[report_type]['filename']),
            ('description', reports[report_type]['desc'])
        )

        return self.session.get('https://webportal1.atsol.com/Axsis.Web/Report/ReportFile',
                                headers=headers, params=params)

    @retry(exceptions=(requests.exceptions.ConnectionError, xlrd.biffh.XLRDError),
           tries=10,
           delay=10)
    @log_and_validate_params
    def get_traffic_counts(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Get the 'Site activity by traffic events' report
        :param start_date: First date to search, inclusive
        :param end_date: Last date to search, inclusive
        :return: Pandas data frame with the resulting data
        """
        parameters: ReportsDetailType = self.get_reports_detail("SITE ACTIVITY BY TRAFFIC EVENTS")
        parameters['Parameters'][1]["ParmValue"] = start_date.strftime("%m/%d/%Y")
        parameters['Parameters'][2]["ParmValue"] = end_date.strftime("%m/%d/%Y")

        response = self._get_report(parameters, Reports.TRAFFIC_COUNTS)

        columns = ['Location code', 'Description', 'First Traf Evt', 'Last Traf Evt'] + \
                  [(start_date + timedelta(days=x)).strftime("%m/%d/%Y")
                   for x in range((end_date - start_date).days + 1)]
        return pd.read_excel(response.content, skiprows=[0, 1], names=columns)

    @retry(exceptions=(requests.exceptions.ConnectionError, xlrd.biffh.XLRDError),
           tries=10,
           delay=10)
    def get_location_summary_by_lane(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Get the 'Location Performance Summary by Lane' report to get total violations
        :param start_date: First date to search, inclusive
        :param end_date: Last date to search, inclusive
        :return: Pandas data frame with the resulting data
        """
        parameters: ReportsDetailType = self.get_reports_detail("LOCATION PERFORMANCE SUMMARY BY LANE -- XML")
        parameters['Parameters'][1]["ParmValue"] = start_date.strftime("%m/%d/%Y")
        parameters['Parameters'][2]["ParmValue"] = end_date.strftime("%m/%d/%Y")
        parameters['Parameters'][3]["ParmValue"] = "ALL"

        response = self._get_report(parameters, Reports.LOCATION_SUMMARY)

        columns = ['Location Code', 'Location Description', 'Lane', 'Vehicle Count', 'Event (Violation Count)',
                   'Total Rejects (G+H+I+J+K+L)', 'Non Events', 'Controllable', 'Uncontrollable', 'PD Non Events',
                   'PD Controllable', 'PD Uncontrollable', 'Events still in WF', 'Total Docs Issued (O+P+Q)',
                   'Citations Issued', 'Nov Issued', 'Warning Issued', 'Last Violation Date']

        return pd.read_csv(BytesIO(response.content), skiprows=[0, 1], names=columns)

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def _login(self) -> None:
        """
        Logs into the Axsis system, which is required to do anything with the API
        :return: None
        """
        headers = {
            'Accept': ACCEPT_HEADER
        }

        response = self.session.get('https://webportal1.atsol.com/axsis.web', headers=headers)

        soup = BeautifulSoup(response.content, "html.parser")

        verification_token = soup.find('input', {'name': "__RequestVerificationToken"})['value']
        return_url = soup.find(id='ReturnUrl')['value']

        headers = {
            'Origin': 'https://sts.atsol.com',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': ACCEPT_HEADER
        }

        data = {
            'PassManagerUsed': 'false',
            'ReturnUrl': return_url,
            'Username': self.username,
            'Password': self.password,
            '__RequestVerificationToken': verification_token
        }

        response = self.session.post('https://sts.atsol.com/account/login',
                                     headers=headers,
                                     params=(('returnUrl', return_url),),
                                     data=data)

        soup = BeautifulSoup(response.content, "html.parser")
        if soup.find("input", {"name": "session_state"}) is None:
            raise Exception("Invalid AXSIS username or password")

        headers = {
            'Origin': 'https://sts.atsol.com',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': ACCEPT_HEADER,
        }

        data = {
            'code': soup.find("input", {"name": "code"})["value"],
            'id_token': soup.find("input", {"name": "id_token"})["value"],
            'scope': soup.find("input", {"name": "scope"})["value"],
            'state': soup.find("input", {"name": "state"})["value"],
            'session_state': soup.find("input", {"name": "session_state"})["value"],
            'access_token': soup.find("input", {"name": "access_token"})["value"]
        }

        self.session.post('https://webportal1.atsol.com/axsis.web/signin-oidc', headers=headers, data=data)

        list_of_cookies = requests.utils.dict_from_cookiejar(self.session.cookies)
        for cookie_name in ['idsrv', 'idsrv.session', 'f5-axsisweb-lb-cookie', '_mvc3authcougar']:
            if cookie_name not in list_of_cookies.keys():
                raise AssertionError("Cookie {} not in list of valid cookie: {}".format(
                    cookie_name, list_of_cookies.keys()))

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def _get_client_id(self) -> None:
        """
        Gets the client id and client code associated with self.username and assigns them to those attributes
        :return: None
        """
        if self.client_code and self.client_id:
            # if we already have the values, then skip is
            return

        headers = {
            'Accept': ACCEPT_HEADER,
        }

        response = self.session.get('https://webportal1.atsol.com/axsis.web/Account/Login', headers=headers)
        soup = BeautifulSoup(response.content, "html.parser")
        self.client_id = soup.find_all('input', id='clientId')[0]['value']
        self.client_code = soup.find_all('input', id='clientCode')[0]['value']

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def _get_reports(self, name: str) -> Optional[int]:
        """
        Take the response to GetReports and get the required report number

        :param name: The name of the report
        :return: The report_id. If the `name` was not valid, then the return will be None
        """
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        }

        self._get_client_id()
        params = (
            ('clientId', self.client_id),
            ('clientCode', self.client_code),
            ('userName', self.username),
        )

        response = self.session.get('https://webportal1.atsol.com/Axsis.Web/api/Report/GetReports',
                                    headers=headers,
                                    params=params)

        resp_list = self._pythonify_literal(response.content.decode())

        for report in resp_list:
            if report['ReportName'] == name:
                return int(report['ReportId'])

        return None

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def get_reports_detail(self, report_name: str) -> Optional[ReportsDetailType]:
        """
        Gets the ReportsDetailType structure of the report details from AXSIS.
        :param report_name: ReportDescription to get the parameter details for. Case sensitive, and is the string from
        the report page
        :return: List of dictionaries of the parameter definitions. If the report name isn't found, then return None.
        """
        logger.info("Getting report {}", report_name)
        self._get_client_id()
        report_id = self._get_reports(report_name)
        params = (
            ('clientId', self.client_id),
            ('clientCode', self.client_code),
            ('userName', self.username),
            ('ReportId', report_id),
            ('vioTypeCode', 'ALL'),
            ('excludeAll', 'true'),
        )

        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        }

        response = self.session.get('https://webportal1.atsol.com/Axsis.Web/api/Report/GetReportsDetail',
                                    headers=headers,
                                    params=params)
        ret: ReportsDetailType = cast(ReportsDetailType, self._pythonify_literal(response.content.decode()))
        if ret.get('Message') and 'No HTTP resource was found that matches the request URI' in ret['Message']:
            # We requested an invalid report name
            return None
        return ret

    def get_location_info(self, location_id: str) -> Optional[str]:
        """
        Gets the location information (address) of a camera based on its ID
        :param location_id: the location identifier of the camera (IE BAL101)
        """
        report = self.get_reports_detail('LOCATION PERFORMANCE DETAIL')
        for param_data in report.get('Parameters'):
            if param_data.get('ParmDataType') == 'PICKLIST':
                for param_list in param_data.get('ParmList'):
                    if param_list.get('Value') == location_id:
                        return param_list.get('Description').split(' - ')[1]
        return None

    @staticmethod
    def _pythonify_literal(obj_str: str) -> Dict:
        """
        Takes a string with a python dictionary and/or list and handles json->python
        :param obj_str: A string with a data structure in it
        :return: The native data structure
        """
        obj_str = obj_str.replace('false', 'False')
        obj_str = obj_str.replace('null', 'None')

        return ast.literal_eval(obj_str)

    @staticmethod
    def _depythonify_literal(obj: ReportsDetailType) -> str:
        """
        Takes a python object and converts it from python->json
        :param obj: Some python object that needs to be jsonified
        :return: String representation of the object
        """
        obj_str = str(obj)
        obj_str = obj_str.replace('None', 'null')
        obj_str = obj_str.replace('False', 'false')

        return obj_str
