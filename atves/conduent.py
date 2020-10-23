"""Wrapper for the Conduent website to get red light and over height ticket information"""
import calendar
import logging
import re
import urllib
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup
from retry import retry

# Camera type
ALLCAMS = 0
REDLIGHT = 1
OVERHEIGHT = 2

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class Conduent:
    """Interface for Conduent that handles authentication and scraping"""

    def __init__(self, username, password):
        """
        Interface to work with Conduent ATVES (red light and over height) camera data
        :param username: (str) Login to conduent account
        :param password: (str) Password to conduent account
        """
        logging.debug("Creating interface with conduent (%s)", username)
        self.session = requests.Session()
        self._state_vals = {}
        self.deployment_server = None
        self.session_id = None

        self._login(username, password)

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def _login(self, username, password):
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

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def login_otp(self, otp):
        """
        Logs in with the required one time password. This is required for most accounts.
        :param otp: (str) The one time password from the user's email account
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

    def _get_state_values(self, resp):
        """
        Gets the ASP.net state values from the hidden fields and populates them in self._state_vals
        :param resp: Response object
        :return: None. Values are set in self._state_vals
        """
        soup = BeautifulSoup(resp.text, "html.parser")

        # Get the ID that is used throughout the session
        id_tags = soup.find_all('input', {'value': 'Citeweb3'})
        if len(id_tags) > 0:
            if len(id_tags) != 1:
                logging.warning("Expected only one id tag, but found multiple: %s", id_tags)
            pattern = re.compile(r"ID=(\d*)")
            self.session_id = pattern.search(str(id_tags[0])).group(1)

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

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def get_location_by_id(self, loc_id: int) -> object:
        """

        :param loc_id:
        :return:
        """
        resp = self.session.get('https://cw3.cite-web.com/citeweb3/locationByID.asp?ID={}'.format(loc_id))
        if resp.status_code == 500:
            self._setup_report_request(REDLIGHT)
            resp = self.session.get('https://cw3.cite-web.com/citeweb3/locationByID.asp?ID={}'.format(loc_id))

        soup = BeautifulSoup(resp.text, "html.parser")
        pattern = re.compile(r'Site Code:\s*(\d*)\s*(.*?)\s\s*Jurisdiction: (\S)\s*Date Created: (.*?)\s\s*Created By: '
                             r'(.*?)\s\s*Effective Date: (.*?)\s\s*Speed Limit: (\d*)\s\s*Status: (\w*)')
        if soup.select_one('p:contains("No location exists with the selected ID!")') is not None:
            logging.info("No location for ID %s", loc_id)
            return None

        text = soup.select_one('p:contains("Effective Date")').get_text().replace(u'\xa0', ' ')
        cam_type = ''
        if soup.select_one('p:contains("BaltimoreRL")'):
            cam_type = 'RL'
        elif soup.select_one('p:contains("BaltimoreOH")'):
            cam_type = 'OH'

        results = pattern.search(text)
        return {'Site Code': results.group(1),
                'Location': results.group(2),
                'Jurisdiction': results.group(3),
                'Date Created': results.group(4),
                'Created By': results.group(5),
                'Effective Date': results.group(6),
                'Speed Limit': results.group(7),
                'Status': results.group(8),
                'Cam Type': cam_type}

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def get_overheight_cameras(self):
        """Get the list of overheight cameras"""
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

    def get_deployment_data(self, start_date: datetime, end_date: datetime, cam_type=ALLCAMS):
        """
        Gets the values of the red light camera deployments, with the number of accepted and rejected tickets
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :param cam_type: Type of camera data to pull (use the constants conduent.REDLIGHT or conduent.OVERHEIGHT or
            conduent.ALLCAMS (default: conduent.ALLCAMS)
        :return: (list of dictionaries) Results with id, start_time, end_time, location, equip_type, accepted, rejected
        """
        logging.info("Getting deployment information for %s - %s and cam type %s", start_date, end_date, cam_type)

        if cam_type == ALLCAMS:
            ret = self._get_deployment_data(start_date, end_date, REDLIGHT)
            ret += self._get_deployment_data(start_date, end_date, OVERHEIGHT)
            return ret
        return self._get_deployment_data(start_date, end_date, cam_type)

    def get_amber_time_rejects_report(self,
                                      start_date: datetime,
                                      end_date: datetime,
                                      location='999,All Locations'):
        """
        Downloads the amber time rejects report (red light only)
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        start_date, end_date = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5974,302,Amber Time Rejects Report,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date, 'TextBox1': end_date, 'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'])

    def get_approval_by_review_date_details(self, start_date: datetime, end_date: datetime, cam_type,
                                            location='999,All Locations'):
        """
        Downloads the report detailing approval by review date
        :param cam_type: Camera type to query. Either conduent.OVERHEIGHT or conduent.REDLIGHT
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        assert cam_type in [REDLIGHT, OVERHEIGHT]
        if cam_type == REDLIGHT:
            report = '5575,302,Approval By Review Date - Details,1,false,true'
        else:
            report = '5575,307,Approval By Review Date - Details,1,false,true'

        start_date, end_date = self._convert_start_end_dates(start_date, end_date)

        return self.get_report(report,
                               cam_type,
                               input_params={'TextBox0': start_date, 'TextBox1': end_date, 'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'])

    def get_approval_summary_by_queue(self, start_date: datetime, end_date: datetime, cam_type,
                                      location='999,All Locations'):
        """
        Downloads the approval summary by queue report
        :param cam_type: Camera type to query. Either conduent.OVERHEIGHT or conduent.REDLIGHT
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        assert cam_type in [REDLIGHT, OVERHEIGHT]
        if cam_type == REDLIGHT:
            report = '5532,302,Approval Summary By Queue,1,false,true'
        else:
            report = '5532,307,Approval Summary By Queue,1,false,true'

        start_date, end_date = self._convert_start_end_dates(start_date, end_date)

        return self.get_report(report,
                               cam_type,
                               input_params={'TextBox0': start_date, 'TextBox1': end_date, 'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'])

    def get_client_summary_by_location(self, start_date: datetime, end_date: datetime, cam_type,
                                       location='999,All Locations'):
        """
        Downloads the client summary by location
        :param cam_type: Camera type to query. Either conduent.OVERHEIGHT or conduent.REDLIGHT
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        assert cam_type in [REDLIGHT, OVERHEIGHT]
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
            data['Date'] = working_date.strftime("%m/%d/%y")
            ret = pd.concat([ret, data]) if ret is not None else data
            working_date += timedelta(days=1)
        return ret

    def get_expired_by_location(self,
                                start_date,
                                end_date,
                                location='999,All Locations'):
        """
        Downloads the 'expired by location' report (redlight cameras only)
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        start_date, end_date = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5843,302,Expired by Location,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date, 'TextBox1': end_date, 'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'])

    def get_in_city_vs_out_of_city(self,
                                   start_date: datetime,
                                   end_date: datetime):
        """
        Downloads the 'in city vs out of city' report (red light cameras only)
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :return: pandas.core.frame.DataFrame
        """
        start_date, end_date = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5543,302,In City Vs Out of City,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date, 'TextBox1': end_date},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hTextBoxCount',
                                              'hComboBoxCount'])

    def get_straight_thru_vs_right_turn(self,
                                        start_date: datetime,
                                        end_date: datetime,
                                        location='999,All Locations'):
        """
        Downloads the 'straight thru vs right turn' report (red light cameras only)
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        start_date, end_date = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5868,302,Straight Thru vs Right Turn,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date, 'TextBox1': end_date, 'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'])

    def get_traffic_counts_by_location(self,
                                       start_date: datetime,
                                       end_date: datetime,
                                       location='999,All Locations'):
        """
        Downloads the 'traffic count by location' report (red light cameras only)
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :param location: Optional location search. Uses the codes from the website
        :return: pandas.core.frame.DataFrame
        """
        start_date, end_date = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('6021,302,Traffic count by Location,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date, 'TextBox1': end_date, 'ComboBox0': location},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hComboBoxTempo_Id0',
                                              'hComboBoxTempo_String0', 'hTextBoxCount', 'hComboBoxCount'])

    def get_violations_issued_by_location(self,
                                          start_date: datetime,
                                          end_date: datetime):
        """
        Downloads the 'violations issued by location' report (red light cameras only)
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :return: pandas.core.frame.DataFrame
        """
        start_date, end_date = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5657,302,Violations issued by Location,1,false,true',
                               REDLIGHT,
                               input_params={'TextBox0': start_date, 'TextBox1': end_date},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hTextBoxCount',
                                              'hComboBoxCount'])

    def get_daily_self_test(self,
                            start_date: datetime,
                            end_date: datetime):
        """
        Downloads the daily self test report (overheight cameras only)
        :param start_date: (datetime) Start date of the report to pull
        :param end_date: (datetime) End date of the report to pull
        :return: pandas.core.frame.DataFrame
        """
        start_date, end_date = self._convert_start_end_dates(start_date, end_date)
        return self.get_report('5602,307,Daily Self Test,1,false,true',
                               OVERHEIGHT,
                               input_params={'TextBox0': start_date, 'TextBox1': end_date},
                               scrape_params=['hTextBoxTempo_Id0', 'hTextBoxTempo_Id1', 'hTextBoxCount',
                                              'hComboBoxCount'])

    def get_pending_client_approval(self, cam_type):
        """
        Downloads the pending client approval report (overheight cameras only)
        :return: pandas.core.frame.DataFrame
        """
        assert cam_type in [REDLIGHT, OVERHEIGHT]
        if cam_type == REDLIGHT:
            report = '5579,302,Pending Client Approval,1,false,true'
        else:
            report = '5579,307,Pending Client Approval,1,false,true'
        return self.get_report(report, OVERHEIGHT)

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def get_report(self, report_type, cam_type, input_params=None, scrape_params=None):
        """
        Pulls the specified report
        :param report_type: Report type, which is the same as what is posted to univReport.asp
        :param cam_type: Camera type to query. Either conduent.OVERHEIGHT or conduent.REDLIGHT
        :param input_params: (dict) Parameters where the value is defined externally, such as search dates. Should be
        a dict with the parameter name and parameter value, as named on citeweb
        :param scrape_params: (list) Parameters that are defined internally to citeweb, and need to be scraped from
        univReport. The parameters will be scraped from the tags as '<input NAME=VALUE...', and will be submitted to
        citeweb in the form of name:value
        :return: pandas.core.frame.DataFrame
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
        payload.update(input_params)

        # scrape the parameters that need to come from univReports.asp
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
            report_file = pattern.search(soup.find('a', {'name': 'aGetReport'}).get('onclick')).group(0)
        except IndexError:
            logging.error("There was an error with error generation. No file was generated. HTML output:\n\n")
            logging.debug(resp.text)
            return None

        # download the file and return it
        return pd.read_csv('https://cw3.cite-web.com{}'.format(report_file))

    def _get_deployment_server(self, resp):
        """
        Returns the deployment server ip from the citmenu.asp response text
        :param resp: requests.models.Response
        :return: None
        """
        soup = BeautifulSoup(resp.text, "html.parser")
        results = [i.attrs.get('href') for i in soup.find_all('a') if i.text == "Reports"]
        for result in results:
            url = urllib.parse.urlparse(result)
            if len(urllib.parse.parse_qs(url.query).get('Server')) > 0:
                self.deployment_server = urllib.parse.parse_qs(url.query).get('Server')[0]
            if self.deployment_server is not None:
                break

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def _setup_report_request(self, cam_type):
        """ This is mainly about requesting pages in the right order, to simulate someone using a browser"""
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

    @retry(exceptions=requests.exceptions.ConnectionError,
           tries=10,
           delay=10)
    def _get_deployment_data(self, search_start_date: datetime, search_end_date: datetime, cam_type):
        """ Pull the data from the deployment section"""
        assert (cam_type in [REDLIGHT, OVERHEIGHT])

        self._setup_report_request(cam_type)

        deploy_type = {REDLIGHT: 'DeplByMonth_BaltimoreRL.asp', OVERHEIGHT: 'DeplByMonth.asp'}
        results = []

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

            for row in table.find_all("tr"):
                elements = row.find_all("td")
                if len(elements) != 8:
                    logging.debug("Skipping %s", elements)
                    continue

                if hasattr(row.find_all('td')[1].p, 'text') and hasattr(row.find_all('td')[2].p, 'text') and \
                        row.find_all('td')[1].p.text and row.find_all('td')[2].p.text:

                    act_start_date = datetime.strptime(row.find_all('td')[1].p.text, "%b %d, %Y %H:%M:%S")
                    act_end_date = datetime.strptime(row.find_all('td')[2].p.text, "%b %d, %Y %H:%M:%S")

                    # Make sure we are within the date range
                    if act_start_date >= search_start_date and act_end_date <= search_end_date:
                        results.append({
                            'id': "" if not row.find_all('td')[0].p else row.find_all('td')[0].a.text,
                            'start_time': act_start_date,
                            'end_time': act_end_date,
                            'location': "" if not row.find_all('td')[3].p else row.find_all('td')[3].p.text,
                            'officer': "" if not row.find_all('td')[4].p else row.find_all('td')[4].p.text,
                            'equip_type': "" if not row.find_all('td')[5].p else row.find_all('td')[5].p.text,
                            'issued': "" if not row.find_all('td')[6].p else row.find_all('td')[6].p.text,
                            'rejected': "" if not row.find_all('td')[7].p else row.find_all('td')[7].p.text
                        })

        return results

    @staticmethod
    def _month_year_iter(start_month, start_year, end_month, end_year):
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
    def _convert_start_end_dates(start_date: datetime, end_date: datetime):
        return start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y")
