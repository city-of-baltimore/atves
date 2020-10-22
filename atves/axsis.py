"""
Python wrapper around the Axsis Mobility Platform
"""
import ast
import logging
from datetime import date, timedelta

import pandas as pd
import requests
import xlrd
from bs4 import BeautifulSoup
from retry import retry

ACCEPT_HEADER = ("text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/"
                 "signed-exchange;v=b3;q=0.9")

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class Axsis:
    """Interface to the Axsis Mobility Platform"""

    def __init__(self, username: str, password: str):
        """
        :param username: Case sensitive username used to log into Axsis
        :param password: Case sensitive password used to log into Axsis
        """
        logging.debug("Creating session for user %s", username)

        self.session = requests.Session()
        self.username = username
        self.password = password
        self.client_id = None
        self.client_code = None
        self._login()

    @retry(exceptions=(requests.exceptions.ConnectionError, xlrd.biffh.XLRDError),
           tries=10,
           delay=10)
    def get_traffic_counts(self, start_date: date, end_date: date):
        """
        Get the 'Site activity by traffic events' report
        :param start_date: First date to search, inclusive
        :param end_date: Last date to search, inclusive
        :return: Pandas data frame with the resulting data
        """
        logging.info("Getting traffic counts from %s to %s", start_date, end_date)
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://webportal1.atsol.com',
        }

        parameters = self.get_reports_detail("SITE ACTIVITY BY TRAFFIC EVENTS")
        parameters['Parameters'][1]["ParmValue"] = start_date.strftime("%m/%d/%Y")
        parameters['Parameters'][2]["ParmValue"] = end_date.strftime("%m/%d/%Y")

        response = self.session.post('https://webportal1.atsol.com/Axsis.Web/api/Report/PostCacheReportFile',
                                     headers=headers, data=self._depythonify_literal(parameters))

        guid = response.content[1:-1]

        headers = {
            'Accept': ACCEPT_HEADER,
        }

        params = (
            ('user', self.username),
            ('guid', guid),
            ('filename',
             ("http://biportal/enterprisereportingservices/"
              "Reports/AXSIS Report/Site_Activity_by_Traffic_Events_AXSIS.rdl")),
            ('description', 'SITE ACTIVITY BY TRAFFIC EVENTS'),
        )

        response = self.session.get('https://webportal1.atsol.com/Axsis.Web/Report/ReportFile', headers=headers,
                                    params=params)

        columns = ['Location code', 'Description', 'First Traf Evt', 'Last Traf Evt'] + \
                  [(start_date + timedelta(days=x)).strftime("%m/%d/%Y")
                   for x in range((end_date - start_date).days + 1)]
        return pd.read_excel(response.content, skiprows=[0, 1], names=columns)

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def _login(self):
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
            raise Exception("Invalid username or password")

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
            assert cookie_name in list_of_cookies.keys()

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def _get_client_id(self):
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
    def _get_reports(self, name):
        """
        Take the response to GetReports and get the required report number

        :param name: (str) The name of the report
        :return: (int) The report_id
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

        return 0

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def get_reports_detail(self, report_name: str):
        """

        :param report_name: ReportDescription to get the parameter details for
        :return: List of dictionaries of the parameter definitions
        """
        logging.info("Getting report %s", report_name)
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
        return self._pythonify_literal(response.content.decode())

    @staticmethod
    def _pythonify_literal(obj_str: str):
        """
        Takes a string with a python dictionary and/or list and handles json->python
        :param obj_str: A string with a data structure in it
        :return: The native data structure
        """
        obj_str = obj_str.replace('false', 'False')
        obj_str = obj_str.replace('null', 'None')

        return ast.literal_eval(obj_str)

    @staticmethod
    def _depythonify_literal(obj):
        """
        Takes a python object and converts it from python->json
        :param obj: Some python object that needs to be jsonified
        :return: String representation of the object
        """
        obj_str = str(obj)
        obj_str = obj_str.replace('None', 'null')
        obj_str = obj_str.replace('False', 'false')

        return obj_str
