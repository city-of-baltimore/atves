"""Wrapper for the Conduent website to get red light and over height ticket information"""
import calendar
import re
import urllib
from datetime import date, datetime, timedelta
from typing import Generator, List, Optional, Tuple

import pandas as pd  # type: ignore
import requests
from bs4 import BeautifulSoup  # type: ignore
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from atves.conduent_types import CameraType, ConduentResultsType, SessionStateType
from atves.constants import ALLCAMS, REDLIGHT, OVERHEIGHT


class Conduent:
    """Interface for Conduent that handles authentication and scraping"""

    def __init__(self, username: str, password: str):
        """
        Interface to work with Conduent ATVES (red light and over height) camera data
        :param username: Login to conduent account
        :param password: Password to conduent account
        """
        logger.debug("Creating interface with conduent ({})", username)
        self.session = requests.Session()
        self._state_vals: SessionStateType = {'__VIEWSTATE': None,
                                              '__VIEWSTATEGENERATOR': None,
                                              '__EVENTVALIDATION': None
                                              }
        self.deployment_server: Optional[str] = None
        self.session_id: Optional[str] = None

        self._login(username, password)

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=retry_if_exception_type(requests.exceptions.ConnectionError))
    def _login(self, username, password) -> None:
        """ First step of sending username and password """
        payload = {
            'txtUser': username,
            'txtPassword': password,
            'btnLogin': 'Sign In',
            'forgotpwd': 0
        }

        resp = self.session.get('https://cw3.cite-web.com/loginhub/Main.aspx')
        self._get_state_values(resp)

        payload.update(self._state_vals)

        resp = self.session.post('https://cw3.cite-web.com/loginhub/Main.aspx', data=payload)
        self._get_state_values(resp)
        soup = BeautifulSoup(resp.text, "html.parser")
        if len(soup.find_all('input', {'name': 'txtOTP'})) != 1:
            raise AssertionError("Login failure with Conduent")

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=retry_if_exception_type(requests.exceptions.ConnectionError))
    def login_otp(self, otp: str) -> None:
        """
        Logs in with the required one time password. This seems to not be required, but is left in incase they ever fix
        that.
        :return: None
        """
        payload = {
            'txtUser': '',
            'txtPassword': '',
            'txtOTP': otp,
            'btnOTP': 'Submit',
            'forgotpwd': 0
        }

        # Post the payload to the site to log in
        payload.update(self._state_vals)

        resp = self.session.post('https://cw3.cite-web.com/loginhub/Main.aspx',
                                 data=payload,
                                 headers={'referer': 'https://cw3.cite-web.com/loginhub/Main.aspx'})
        self._get_state_values(resp)

        self.session.get('https://cw3.cite-web.com/loginhub/Select.aspx?ID={}'.format(self.session_id),
                         headers={'referer': 'https://cw3.cite-web.com/loginhub/Main.aspx'})

    def _get_state_values(self, resp: requests.Response) -> None:
        """
        Gets the ASP.net state values from the hidden fields and populates them in self._state_vals
        :param resp: Response object
        """
        soup = BeautifulSoup(resp.text, "html.parser")

        # Get the ID that is used throughout the session
        id_tags = soup.find_all('input', {'value': 'Citeweb3'})
        if len(id_tags) > 0:
            if len(id_tags) != 1:
                logger.warning("Expected only one id tag, but found multiple: {}", id_tags)
            pattern = re.compile(r"ID=(\d*)")
            session_id = pattern.search(str(id_tags[0]))
            if session_id is None:
                raise AssertionError("Expected 'ID=' in response. Got {}".format(pattern))

            self.session_id = session_id.group(1)

        # get all state variables
        hidden_tags = soup.find_all("input", type="hidden")
        tags = {}
        for tag in hidden_tags:
            if tag.get('value') and tag.get('name'):
                tags[tag['name']] = tag['value']

        # Post the payload to the site to log in
        self._state_vals['__VIEWSTATE'] = tags['__VIEWSTATE']
        self._state_vals['__VIEWSTATEGENERATOR'] = tags['__VIEWSTATEGENERATOR']
        self._state_vals['__EVENTVALIDATION'] = tags['__EVENTVALIDATION']

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=retry_if_exception_type(requests.exceptions.ConnectionError))
    def get_location_by_id(self, loc_id: int, cam_type: int) -> CameraType:
        """
        Gets camera information by location id. The id is <ID> in
        https://cw3.cite-web.com/citeweb3/locationByID.asp?ID=<ID>
        :param loc_id: Camera ID to lookup
        :param cam_type: Type of camera data to pull (use the constants atves.REDLIGHT or atves.OVERHEIGHT
        :return: Dictionary of type `atves.conduent_types.CameraType` with camera data
        """
        ret: CameraType = {
            'site_code': None,
            'location': None,
            'jurisdiction': None,
            'date_created': None,
            'created_by': None,
            'effective_date': None,
            'speed_limit': None,
            'status': None,
            'cam_type': None}

        if cam_type == REDLIGHT:
            self._setup_report_request(REDLIGHT)
        elif cam_type == OVERHEIGHT:
            self._setup_report_request(OVERHEIGHT)
        else:
            raise AssertionError('Cam type {} is not valid'.format(cam_type))

        resp = self.session.get('https://cw3.cite-web.com/citeweb3/locationByID.asp?ID={}'.format(loc_id))
        if resp.status_code == 500:
            logger.error('Got HTTP response code {}'.format(resp.status_code))
            return ret

        soup = BeautifulSoup(resp.text, "html.parser")
        if soup.select_one('p:-soup-contains("No location exists with the selected ID!")') is not None:
            logger.info("No location for ID {}", loc_id)
            return ret

        effective_date = soup.select_one('p:-soup-contains("Effective Date")')
        if not effective_date:
            logger.error("Unable to find Effective Date in HTTP response")
            return ret

        text = effective_date.get_text().replace(u'\xa0', ' ')

        cam_type_str = ''
        if soup.select_one('p:-soup-contains("BaltimoreRL")'):
            cam_type_str = 'RL'
        elif soup.select_one('p:-soup-contains("BaltimoreOH")'):
            cam_type_str = 'OH'

        pattern = re.compile(r'Site Code:\s*(\d*)\s*(.*?)\s\s*Jurisdiction: (\S)\s*Date Created: (.*?)\s\s*Created By: '
                             r'(.*?)\s\s*Effective Date: (.*?)\s\s*Speed Limit: (\d*)\s\s*Status: (\w*)')
        results = pattern.search(text)
        if results is None:
            logger.error('Unable to find expected camera data in HTTP response: {}'.format(text))
            return ret

        return {'site_code': results.group(1),
                'location': results.group(2),
                'jurisdiction': results.group(3),
                'date_created': results.group(4),
                'created_by': results.group(5),
                'effective_date': results.group(6),
                'speed_limit': results.group(7),
                'status': results.group(8),
                'cam_type': cam_type_str}

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=retry_if_exception_type(requests.exceptions.ConnectionError))
    def get_overheight_cameras(self) -> List[Tuple[int, str]]:
        """
        Get the list of overheight cameras
        :return [[LocationID, LocationDesc], ...]
        """
        self._setup_report_request(OVERHEIGHT)

        # set the request up; we have to do this first
        report_url = "https://cw3.cite-web.com/citeweb3/UnivReports.asp?Server={}&Database={}".format(
            self.deployment_server, 'BaltimoreOH')
        self.session.get(report_url,
                         headers={'referer': 'https://cw3.cite-web.com/citeweb3/citmenu.asp?DB={}&Site=Maryland'.format(
                             'BaltimoreOH')})

        # generate the report request
        payload = {
            'lstReportList': '5575,307,Approval By Review Date - Details,1,false,true'
        }
        resp = self.session.post("https://cw3.cite-web.com/citeweb3/univReports.asp",
                                 data=payload,
                                 headers={'referer': report_url})
        soup = BeautifulSoup(resp.text, 'html.parser')
        return [x.text.split(' - ')
                for x in soup.select('select[id="ComboBox0"] > option')
                if x.text != 'All Locations']

    def get_deployment_data(self, start_date: date, end_date: date,
                            cam_type: int = ALLCAMS) -> List[ConduentResultsType]:
        """
        Gets the values of the red light camera deployments, with the number of accepted and rejected tickets
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param cam_type: Type of camera data to pull (use the constants atves.REDLIGHT or atves.OVERHEIGHT or
            atves.ALLCAMS (default: atves.ALLCAMS)
        :return: (list of dictionaries) Results with id, start_time, end_time, location, equip_type, accepted, rejected
        """
        logger.info("Getting deployment information for {} - {} and cam type {}", start_date, end_date, cam_type)

        if cam_type == ALLCAMS:
            ret = self._get_deployment_data(start_date, end_date, REDLIGHT)
            ret += self._get_deployment_data(start_date, end_date, OVERHEIGHT)
            return ret
        return self._get_deployment_data(start_date, end_date, cam_type)

    def get_amber_time_rejects_report(self,
                                      start_date: date,
                                      end_date: date,
                                      location='999,All Locations') -> pd.DataFrame:
        """
        Downloads the amber time rejects report (red light only)
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        start_date_str, end_date_str = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5974,302,Amber Time Rejects Report,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date_str, 'TextBox1': end_date_str,
                                             'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'],
                               parse_dates=['VioDate'])

    def get_approval_by_review_date_details(self, start_date: date, end_date: date, cam_type: int,
                                            location='999,All Locations') -> Optional[pd.DataFrame]:
        """
        Downloads the report detailing approval by review date
        :param cam_type: Camera type to query. Either atves.OVERHEIGHT or atves.REDLIGHT
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        if cam_type not in [REDLIGHT, OVERHEIGHT]:
            raise AssertionError("Cam type {} is unexpected".format(cam_type))
        if cam_type == REDLIGHT:
            report = '5575,302,Approval By Review Date - Details,1,false,true'
        else:
            report = '5575,307,Approval By Review Date - Details,1,false,true'

        start_date_str, end_date_str = self._convert_start_end_dates(start_date, end_date)

        ret = self.get_report(report,
                              cam_type,
                              input_params={'TextBox0': start_date_str, 'TextBox1': end_date_str,
                                            'ComboBox0': location},
                              scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                             'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'])
        if ret is None:
            return None

        ret['Vio Date'] = pd.to_datetime(ret['Vio Date'], format='%b %d %Y %I:%M%p').dt.date
        ret['Review Status'] = ret['Review Status'].str.strip()

        agg = {
            'Disapproved': 'sum',
            'Approved': 'sum'
        }

        ret = ret.groupby(['Vio Date', 'Review Status']).agg(agg).reset_index()

        return ret

    def get_approval_summary_by_queue(self, start_date: date, end_date: date, cam_type: int,
                                      location='999,All Locations') -> pd.DataFrame:
        """
        Downloads the approval summary by queue report
        :param cam_type: Camera type to query. Either atves.OVERHEIGHT or atves.REDLIGHT
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        if cam_type not in [REDLIGHT, OVERHEIGHT, ALLCAMS]:
            raise AssertionError("Cam type {} is unexpected".format(cam_type))

        if cam_type == REDLIGHT:
            report = '5532,302,Approval Summary By Queue,1,false,true'
        else:
            report = '5532,307,Approval Summary By Queue,1,false,true'

        start_date_str, end_date_str = self._convert_start_end_dates(start_date, end_date)

        return self.get_report(report,
                               cam_type,
                               input_params={'TextBox0': start_date_str, 'TextBox1': end_date_str,
                                             'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'],
                               parse_dates=['Review Date'])

    def get_client_summary_by_location(self, start_date: date, end_date: date, cam_type: int = ALLCAMS,
                                       location='999,All Locations') -> pd.DataFrame:
        """
        Downloads the client summary by location
        :param cam_type: Camera type to query. Either atves.OVERHEIGHT, atves.REDLIGHT, or atves.ALLCAMS
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        if cam_type not in [REDLIGHT, OVERHEIGHT, ALLCAMS]:
            raise AssertionError("Cam type {} is unexpected".format(cam_type))

        if cam_type == ALLCAMS:
            rep_rl = self.get_client_summary_by_location(start_date, end_date, cam_type=REDLIGHT)
            rep_oh = self.get_client_summary_by_location(start_date, end_date, cam_type=OVERHEIGHT)
            return pd.concat([rep_rl, rep_oh])

        if cam_type == REDLIGHT:
            report = '5608,302,Client Summary By Location,1,false,true'
        else:
            report = '5608,307,Client Summary By Location,1,false,true'

        working_date = start_date
        ret = None

        # The report batches the results for the whole range, so we will search by each date, and then add the date
        # in a new 'Date' column
        while working_date <= end_date:
            data = self.get_report(report,
                                   cam_type,
                                   input_params={'TextBox0': working_date.strftime("%m/%d/%y"),
                                                 'TextBox1': working_date.strftime("%m/%d/%y"),
                                                 'ComboBox0': location},
                                   scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                                  'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'])
            if data is not None:
                data['Date'] = working_date
                ret = pd.concat([ret, data]) if ret is not None else data
            working_date += timedelta(days=1)
        return ret

    def get_expired_by_location(self,
                                start_date: date,
                                end_date: date,
                                location='999,All Locations') -> pd.DataFrame:
        """
        Downloads the 'expired by location' report (redlight cameras only)
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        start_date_str, end_date_str = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5843,302,Expired by Location,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date_str, 'TextBox1': end_date_str,
                                             'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'])

    def get_in_city_vs_out_of_city(self,
                                   start_date: date,
                                   end_date: date) -> pd.DataFrame:
        """
        Downloads the 'in city vs out of city' report (red light cameras only)
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :return: pandas.core.frame.DataFrame
        """
        start_date_str, end_date_str = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5543,302,In City Vs Out of City,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date_str, 'TextBox1': end_date_str},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hTextBoxCount',
                                              'hComboBoxCount'])

    def get_straight_thru_vs_right_turn(self,
                                        start_date: date,
                                        end_date: date,
                                        location='999,All Locations') -> pd.DataFrame:
        """
        Downloads the 'straight thru vs right turn' report (red light cameras only)
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        start_date_str, end_date_str = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5868,302,Straight Thru vs Right Turn,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date_str, 'TextBox1': end_date_str,
                                             'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'],
                               parse_dates=['Violation Date'])

    def get_traffic_counts_by_location(self,
                                       start_date: date,
                                       end_date: date,
                                       location='999,All Locations') -> pd.DataFrame:
        """
        Downloads the 'traffic count by location' report (red light cameras only)
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        start_date_str, end_date_str = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('6021,302,Traffic count by Location,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date_str, 'TextBox1': end_date_str,
                                             'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'],
                               parse_dates=['Ddate'])

    def get_violations_issued_by_location(self,
                                          start_date: date,
                                          end_date: date) -> pd.DataFrame:
        """
        Downloads the 'violations issued by location' report (red light cameras only)
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :return: pandas.core.frame.DataFrame
        """
        start_date_str, end_date_str = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5657,302,Violations issued by Location,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date_str, 'TextBox1': end_date_str},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hTextBoxCount',
                                              'hComboBoxCount'])

    def get_daily_self_test(self,
                            start_date: date,
                            end_date: date) -> pd.DataFrame:
        """
        Downloads the daily self test report (overheight cameras only)
        :param start_date: Start date of the report to pull
        :param end_date: End date of the report to pull
        :return: pandas.core.frame.DataFrame
        """
        start_date_str, end_date_str = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5602,307,Daily Self Test,1,false,true',
                               OVERHEIGHT,
                               input_params={'TextBox0': start_date_str, 'TextBox1': end_date_str},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hTextBoxCount',
                                              'hComboBoxCount'],
                               parse_dates=['TestDate'])

    def get_pending_client_approval(self, cam_type: int) -> pd.DataFrame:
        """
        Downloads the pending client approval report (overheight cameras only)
        :return: pandas.core.frame.DataFrame
        """
        if cam_type not in [REDLIGHT, OVERHEIGHT]:
            raise AssertionError("Cam type {} is unexpected".format(cam_type))

        if cam_type == REDLIGHT:
            report = '5579,302,Pending Client Approval,1,false,true'
        else:
            report = '5579,307,Pending Client Approval,1,false,true'
        return self.get_report(report, OVERHEIGHT)

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=retry_if_exception_type((requests.exceptions.ConnectionError, urllib.error.URLError)))
    def get_report(self, report_type, cam_type, input_params=None,  # pylint:disable=too-many-arguments,too-many-locals
                   scrape_params=None, parse_dates=None) -> Optional[pd.DataFrame]:
        """
        Pulls the specified report
        :param report_type: Report type, which is the same as what is posted to univReport.asp
        :param cam_type: Camera type to query. Either atves.OVERHEIGHT or atves.REDLIGHT
        :param input_params: (dict) Parameters where the value is defined externally, such as search dates. Should be
        a dict with the parameter name and parameter value, as named on citeweb
        :param scrape_params: (list) Parameters that are defined internally to citeweb, and need to be scraped from
        univReport. The parameters will be scraped from the tags as '<input NAME=VALUE...', and will be submitted to
        citeweb in the form of name:value
        :param parse_dates: Directly passed to read_csv. See the documentation for pandas.read_csv
        :return: Report data
        """
        if cam_type == REDLIGHT:
            cam_val = 'BaltimoreRL'
        elif cam_type == OVERHEIGHT:
            cam_val = 'BaltimoreOH'
        else:
            raise AssertionError

        self._setup_report_request(cam_type)

        # set the request up; we have to do this first
        report_url = "https://cw3.cite-web.com/citeweb3/UnivReports.asp?Server={}&Database={}".format(
            self.deployment_server, cam_val)
        self.session.get(report_url,
                         headers={'referer': 'https://cw3.cite-web.com/citeweb3/citmenu.asp?DB={}&Site=Maryland'.format(
                             cam_val)})

        # generate the report request
        payload = {
            'lstReportList': report_type
        }
        resp = self.session.post("https://cw3.cite-web.com/citeweb3/univReports.asp",
                                 data=payload,
                                 headers={'referer': report_url})
        soup = BeautifulSoup(resp.text, "html.parser")

        payload = {
            'hReportID': soup.find('input', {'name': 'hReportID'}).get('value'),
            'hSQLDB_ID': soup.find('input', {'name': 'hSQLDB_ID'}).get('value'),
            'hPrePrint_Process_ID': soup.find('input', {'name': 'hPrePrint_Process_ID'}).get('value'),
            'hGraphStyle': soup.find('input', {'name': 'hGraphStyle'}).get('value'),
            'hIsParams': soup.find('input', {'name': 'hIsParams'}).get('value'),
            'hUpdFlag': soup.find('input', {'name': 'hUpdFlag'}).get('value'),
            'radioFormat': '8',  # CSV
            'ok': soup.find('input', {'name': 'ok'}).get('value'),
        }

        # add the input params to the payload data
        if input_params:
            payload.update(input_params)

        # scrape the parameters that need to come from univReports.asp
        if scrape_params:
            for name in scrape_params:
                val = soup.find('input', {'name': name}).get('value')
                payload[name] = val if val is not None else ''

        # request the report, and get the filename where we need to download it
        resp = self.session.post('https://cw3.cite-web.com/citeweb3/univReports.asp',
                                 data=payload,
                                 headers={'referer': 'https://cw3.cite-web.com/citeweb3/univReports.asp'})
        soup = BeautifulSoup(resp.text, "html.parser")
        pattern = re.compile(r'/media/.*\.csv')
        try:
            getreport = soup.find('a', {'name': 'aGetReport'})
            if not getreport:
                logger.error('Unable to find "<a name="aGetReport..." tag in {}'.format(soup))
                return None

            onclick = pattern.search(getreport.get('onclick'))
            if not onclick:
                logger.error('Unable to find onclick element of <a name="aGetReport".. in \n{}'.format(getreport))
                return None

        except IndexError:
            logger.error("There was an error with error generation. No file was generated. HTML output:\n\n")
            logger.debug(resp.text)
            return None

        # download the file and return it
        return pd.read_csv('https://cw3.cite-web.com{}'.format(onclick.group(0)), parse_dates=parse_dates)

    def _get_deployment_server(self, resp: requests.Response) -> None:
        """
        Returns the deployment server ip from the citmenu.asp response text
        :param resp: requests.models.Response
        :return: None
        """
        soup = BeautifulSoup(resp.text, "html.parser")
        results = [i.attrs.get('href') for i in soup.find_all('a') if i.text == "Reports"]
        for result in results:
            url = urllib.parse.urlparse(result)
            server_val = urllib.parse.parse_qs(url.query).get('Server')
            if server_val:
                self.deployment_server = server_val[0]
            if self.deployment_server is not None:
                break

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=retry_if_exception_type(requests.exceptions.ConnectionError))
    def _setup_report_request(self, cam_type: int) -> None:
        """
        This is mainly about requesting pages in the right order, to simulate someone using a browser
        :param: Either `REDLIGHT` or `OVERHEIGHT`. Will raise AssertionError if not one of these values
        """
        # Setup the cookies with these requests
        if cam_type == REDLIGHT:
            cookie_val = 'BaltimoreRL'
        elif cam_type == OVERHEIGHT:
            cookie_val = 'BaltimoreOH'
        else:
            raise AssertionError

        self.session.get('https://cw3.cite-web.com/citeweb3/Default.asp?ID={}'.format(self.session_id),
                         headers={'referer': 'https://cw3.cite-web.com/loginhub/Select.aspx?ID={}'.format(
                             self.session_id)})

        resp = self.session.get('https://cw3.cite-web.com/citeweb3/citmenu.asp?DB={}&Site=Maryland'.format(cookie_val),
                                headers={'referer': 'https://cw3.cite-web.com/citeweb3/newmenu.asp'})

        if self.deployment_server is None:
            self._get_deployment_server(resp)

        self.session.cookies.set('DBDisplay', None)
        self.session.cookies.set('DBDisplay', cookie_val)

        self.session.cookies.set('DB', None)
        self.session.cookies.set('DB', cookie_val)

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True,
           retry=retry_if_exception_type(requests.exceptions.ConnectionError))
    def _get_deployment_data(self, search_start_date: date, search_end_date: date,
                             cam_type) -> List[ConduentResultsType]:
        """ Pull the data from the deployment section"""
        if cam_type not in [REDLIGHT, OVERHEIGHT]:
            raise AssertionError("Cam type {} is unexpected".format(cam_type))

        self._setup_report_request(cam_type)

        deploy_type = {REDLIGHT: 'DeplByMonth_BaltimoreRL.asp', OVERHEIGHT: 'DeplByMonth.asp'}
        results: List[ConduentResultsType] = []

        for cur_year, cur_month in self._month_year_iter(search_start_date.month,
                                                         search_start_date.year,
                                                         search_end_date.month,
                                                         search_end_date.year):
            resp = self.session.get(
                'https://cw3.cite-web.com/citeweb3/{}?Month={}&Year={}'.format(deploy_type[cam_type],
                                                                               calendar.month_name[cur_month],
                                                                               cur_year))
            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", {"class": "detail"}, border=1)

            if not table:
                return results

            for row in table.find_all('tr'):
                elements = row.find_all('td')
                if len(elements) != 8:
                    logger.debug("Skipping {}", elements)
                    continue

                if hasattr(elements[1].p, 'text') and hasattr(elements[2].p, 'text') and \
                        elements[1].p.text and elements[2].p.text:

                    act_start_date = datetime.strptime(elements[1].p.text, "%b %d, %Y %H:%M:%S")
                    act_end_date = datetime.strptime(elements[2].p.text, "%b %d, %Y %H:%M:%S")
                    # Make sure we are within the date range
                    if act_start_date.date() >= search_start_date and act_end_date.date() <= search_end_date:
                        results.append({
                            'id': "" if not elements[0].p else elements[0].a.text,
                            'start_time': act_start_date,
                            'end_time': act_end_date,
                            'location': "" if not elements[3].p else elements[3].p.text,
                            'officer': "" if not elements[4].p else elements[4].p.text,
                            'equip_type': "" if not elements[5].p else elements[5].p.text,
                            'issued': "" if not elements[6].p else elements[6].p.text,
                            'rejected': "" if not elements[7].p else elements[7].p.text
                        })

        return results

    @staticmethod
    def _month_year_iter(start_month: int, start_year: int, end_month: int,
                         end_year: int) -> Generator[Tuple[int, int], None, None]:
        """
        Creates an iterator for a range of months
        :param start_month: (int) Start month for the range (inclusive)
        :param start_year: (int) Start year for the range (inclusive)
        :param end_month: (int) End month for the range (inclusive)
        :param end_year: (int) End year for the range (inclusive)
        :return: Generator of tuples of (int, int) with year, month
        """
        ym_start = 12 * start_year + start_month - 1
        ym_end = 12 * end_year + end_month
        for year_month in range(ym_start, ym_end):
            year, month = divmod(year_month, 12)
            yield year, month + 1

    @staticmethod
    def _convert_start_end_dates(start_date: date, end_date: date) -> Tuple[str, str]:
        return start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y")
