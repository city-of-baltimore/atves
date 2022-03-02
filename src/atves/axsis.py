"""Python wrapper around the Axsis Mobility Platform"""
import ast
from datetime import date, timedelta
from enum import Enum
from io import BytesIO
from typing import cast, Dict, List, Optional

import pandas as pd  # type: ignore
import requests
import xlrd  # type: ignore
from bs4 import BeautifulSoup  # type: ignore
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential
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
    OFFICER_ACTION = 3


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
        self.username = username.upper()
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
            },
            Reports.OFFICER_ACTION: {
                'desc': 'OFFICER ACTION',
                'filename': '/EnterpriseReportingServices/Customer Reports/Officer Action'
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

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=(retry_if_exception_type(requests.exceptions.ConnectionError) |
                  retry_if_exception_type(xlrd.biffh.XLRDError)))
    @log_and_validate_params
    def get_traffic_counts(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Get the 'Site activity by traffic events' report
        :param start_date: First date to search, inclusive
        :param end_date: Last date to search, inclusive
        :return: Pandas data frame with the resulting data
        """
        parameters: Optional[ReportsDetailType] = self.get_reports_detail("SITE ACTIVITY BY TRAFFIC EVENTS")
        if not parameters:
            logger.error('Unable to get traffic counts')
            return pd.DataFrame()

        parameters['Parameters'][1]["ParmValue"] = start_date.strftime("%m/%d/%Y")
        parameters['Parameters'][2]["ParmValue"] = end_date.strftime("%m/%d/%Y")

        response = self._get_report(parameters, Reports.TRAFFIC_COUNTS)

        columns = ['Location code', 'Description', 'First Traf Evt', 'Last Traf Evt'] + \
                  [(start_date + timedelta(days=x)).strftime("%m/%d/%Y")
                   for x in range((end_date - start_date).days + 1)]
        return pd.read_excel(response.content, skiprows=[0, 1], names=columns)

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=(retry_if_exception_type(requests.exceptions.ConnectionError) |
                  retry_if_exception_type(xlrd.biffh.XLRDError)))
    def get_location_summary_by_lane(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Get the 'Location Performance Summary by Lane' report to get total violations
        :param start_date: First date to search, inclusive
        :param end_date: Last date to search, inclusive
        :return: Pandas data frame with the resulting data
        """
        delta = (end_date - start_date).days
        ret: List[pd.dataframe] = []
        if delta > 0:
            for i in range(delta + 1):
                cur_date = start_date + timedelta(days=i)
                ret.append(self.get_location_summary_by_lane(cur_date, cur_date))
            return pd.concat(ret)

        parameters: Optional[ReportsDetailType] = self.get_reports_detail("LOCATION PERFORMANCE SUMMARY BY LANE -- XML")
        if not parameters:
            logger.error("Unable to get location summary by lane")
            return pd.DataFrame()

        parameters['Parameters'][1]["ParmValue"] = start_date.strftime("%m/%d/%Y")
        parameters['Parameters'][2]["ParmValue"] = end_date.strftime("%m/%d/%Y")
        parameters['Parameters'][3]["ParmValue"] = "ALL"

        response = self._get_report(parameters, Reports.LOCATION_SUMMARY)
        # drop thousands separators or they cause issues when we convert to Int64 (to deal with null values)
        contents = response.content.replace(b',', b'')

        dtypes = {'Location Code': 'str',
                  'Location Description': 'str',
                  'Lane': 'int',
                  'Vehicle Count': 'Int64',
                  'Event (Violation Count)': 'Int64',
                  'Total Rejects (G+H+I+J+K+L)': 'Int64',
                  'Non Events': 'Int64',
                  'Controllable': 'Int64',
                  'Uncontrollable': 'Int64',
                  'PD Non Events': 'Int64',
                  'PD Controllable': 'Int64',
                  'PD Uncontrollable': 'Int64',
                  'Events still in WF': 'Int64',
                  'Total Docs Issued (O+P+Q)': 'Int64',
                  'Citations Issued': 'Int64',
                  'Nov Issued': 'Int64',
                  'Warning Issued': 'Int64'
                  }

        columns = list(dtypes.keys()) + ['Last Violation Date']
        dataframe = pd.read_csv(BytesIO(contents), skiprows=[0, 1], names=columns, sep='\t', thousands=',',
                                dtype=dtypes, parse_dates=['Last Violation Date'])

        agg = {
            'Date': 'first',
            'Location Code': 'first',
            'Location Description': 'first',
            'Vehicle Count': 'sum',
            'Event (Violation Count)': 'sum',
            'Total Rejects (G+H+I+J+K+L)': 'sum',
            'Non Events': 'sum',
            'Controllable': 'sum',
            'Uncontrollable': 'sum',
            'PD Non Events': 'sum',
            'PD Controllable': 'sum',
            'PD Uncontrollable': 'sum',
            'Events still in WF': 'sum',
            'Total Docs Issued (O+P+Q)': 'sum',
            'Citations Issued': 'sum',
            'Nov Issued': 'sum',
            'Warning Issued': 'sum'
        }
        dataframe['Date'] = start_date
        return dataframe.groupby(dataframe['Location Code']).aggregate(agg)  # pylint:disable=unsubscriptable-object

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=(retry_if_exception_type(requests.exceptions.ConnectionError) |
                  retry_if_exception_type(xlrd.biffh.XLRDError)))
    def get_officer_actions(self, start_date: date, end_date: date) -> Dict[str, pd.DataFrame]:
        """
        Get the 'officer actions' report to get the outcome of violations
        :param start_date: First date to search, inclusive
        :param end_date: Last date to search, inclusive
        :return: Dictionary of two dataframes; one is a pandas dataframe of the officer action report and the other is
        a pandas dataframe of the reject reason summary. The officer action report is index '0' and the reject reason
        summary is index '1'
        """
        delta = (end_date - start_date).days
        ret_actions: List[pd.dataframe] = []
        ret_reasons: List[pd.dataframe] = []
        if delta > 0:
            for i in range(delta + 1):
                cur_date = start_date + timedelta(days=i)
                ret = self.get_officer_actions(cur_date, cur_date)
                ret_actions.append(ret['0'])
                ret_reasons.append(ret['1'])
            return {'0': pd.concat(ret_actions), '1': pd.concat(ret_reasons)}

        parameters: Optional[ReportsDetailType] = self.get_reports_detail("OFFICER ACTION")
        if not parameters:
            logger.error("Unable to get location summary by lane")
            return pd.DataFrame()

        parameters['Parameters'][1]["ParmValue"] = start_date.strftime("%m/%d/%Y")
        parameters['Parameters'][2]["ParmValue"] = end_date.strftime("%m/%d/%Y")

        response = self._get_report(parameters, Reports.OFFICER_ACTION)
        dtypes = {
            'Queue': str,
            'Officer Name': str,
            'Reviewed': int,
            'Accepted': int,
            'Rejected': int,
            'Percent Accepted': float,
            'Percent Rejected': float
        }

        try:
            reasons = pd.read_excel(response.content, header=1, skipfooter=1,
                                    sheet_name=['Reject Reason Summary'])['Reject Reason Summary']
            reasons['Date'] = pd.to_datetime(start_date)
        except ValueError:
            # There is not a 'Reject Reason Summary' sheet... probably an incomplete sheet
            return {'0': pd.DataFrame(), '1': pd.DataFrame()}

        return {'0': pd.read_excel(response.content, parse_dates=['Action Date'], dtype=dtypes, header=1, skipfooter=1,
                                   names=['Action Date', 'Queue', 'Officer Name', 'Reviewed', 'Accepted', 'Rejected',
                                          'Percent Accepted', 'Percent Rejected']),
                '1': reasons}

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=retry_if_exception_type(requests.exceptions.ConnectionError))
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
            raise AssertionError("Invalid AXSIS username or password")

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

        response = self.session.post('https://webportal1.atsol.com/axsis.web/signin-oidc', headers=headers, data=data)
        soup = BeautifulSoup(response.content, "html.parser")
        self.client_id = soup.find_all('input', id='clientId')[0]['value']
        self.client_code = soup.find_all('input', id='clientCode')[0]['value']

        list_of_cookies = requests.utils.dict_from_cookiejar(self.session.cookies)
        for cookie_name in ['idsrv', 'idsrv.session', 'f5-axsisweb-lb-cookie', '_mvc3authcougar']:
            if cookie_name not in list_of_cookies:
                raise AssertionError(f'Cookie {cookie_name} not in list of valid cookie: {list_of_cookies.keys()}')

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=retry_if_exception_type(requests.exceptions.ConnectionError))
    def _get_reports(self, name: str) -> Optional[int]:
        """
        Take the response to GetReports and get the required report number

        :param name: The name of the report
        :return: The report_id. If the `name` was not valid, then the return will be None
        """
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        }

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

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=retry_if_exception_type(requests.exceptions.ConnectionError))
    def get_reports_detail(self, report_name: str) -> Optional[ReportsDetailType]:
        """
        Gets the ReportsDetailType structure of the report details from AXSIS.
        :param report_name: ReportDescription to get the parameter details for. Case sensitive, and is the string from
        the report page
        :return: List of dictionaries of the parameter definitions. If the report name isn't found, then return None.
        """
        logger.info("Getting report {}", report_name)
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
        if ret.get('Message') and \
                ('No HTTP resource was found that matches the request URI' in ret['Message'] or
                 ('An error has occurred' in ret['Message'])):
            # We requested an invalid report name
            return None
        return ret

    def get_location_info(self, location_id: str) -> Optional[str]:
        """
        Gets the location information (address) of a camera based on its ID
        :param location_id: the location identifier of the camera (IE BAL101)
        """
        report: Optional[ReportsDetailType] = self.get_reports_detail('LOCATION PERFORMANCE DETAIL')
        if report is None or report['Parameters'] is None:
            logger.error('Unable to get location info')
            return None

        for param_data in report['Parameters']:
            if param_data.get('ParmDataType') == 'PICKLIST' and param_data['ParmList'] is not None:
                for param_list in param_data['ParmList']:
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
