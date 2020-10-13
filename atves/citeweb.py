"""Wrapper for the CiteWeb website to get red light and over height ticket information"""
from datetime import datetime, timedelta
import calendar
import logging

from bs4 import BeautifulSoup
import requests

# Camera type
ALLCAMS = 0
REDLIGHT = 1
OVERHEIGHT = 2

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class CiteWeb:
    """Interface for CiteWeb that handles authentication and scraping"""
    def __init__(self, username, password):
        """
        Interface to work with Conduent ATVES (red light and over height) camera data
        :param username: (str) Login to citeweb account
        :param password: (str) Password to citeweb account
        """
        logging.debug("Creating interface with citeweb (%s)", username)
        self.session = requests.Session()
        self._state_vals = {}

        self._login(username, password)

    def _login(self, username, password):
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

    def login_otp(self, otp):
        """
        Logs in with the required one time password. This is required for most accounts.
        :param otp: (str) The one time password from the user's email account
        :return:
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
        resp = self.session.post('https://cw3.cite-web.com/loginhub/Main.aspx', data=payload)
        self._get_state_values(resp)

    def _get_state_values(self, resp):
        """
        Gets the ASP.net state values from the hidden fields and populates them in self._state_vals
        :param resp: Response object
        :return: None. Values are set in self._state_vals
        """
        soup = BeautifulSoup(resp.text, "html.parser")
        hidden_tags = soup.find_all("input", type="hidden")
        tags = {}
        for tag in hidden_tags:
            if tag.get('value') and tag.get('name'):
                tags[tag['name']] = tag['value']

        # Post the payload to the site to log in
        self._state_vals['__VIEWSTATE'] = tags['__VIEWSTATE']
        self._state_vals['__VIEWSTATEGENERATOR'] = tags['__VIEWSTATEGENERATOR']
        self._state_vals['__EVENTVALIDATION'] = tags['__EVENTVALIDATION']

    def get_deployment_data(self, year, month, day, quantity=1, cam_type=ALLCAMS):  # pylint:disable=too-many-arguments
        """
        Gets the values of the red light camera deployments, with the number of accepted and rejected tickets
        :param year: (int) Four digit year to request as start of range
        :param month: (int) Month to request as start of range
        :param day: (int) Date to request as start of range
        :param quantity: (int) Number of days of data to pull, with year/month/day being the start of the range
        :param cam_type: Type of camera data to pull (use the constants citeweb.REDLIGHT or citeweb.OVERHEIGHT or
            citeweb.ALLCAMS (default: citeweb.ALLCAMS)
        :return: (list of dictionaries) Results with id, start_time, end_time, location, equip_type, accepted, rejected
        """
        logging.info("Getting deployment information for %s/%s and cam type %s", month, year, cam_type)

        if cam_type == ALLCAMS:
            ret = self._get_deployment_data(year, month, day, quantity, REDLIGHT)
            ret += self._get_deployment_data(year, month, day, quantity, OVERHEIGHT)
            return ret
        return self._get_deployment_data(year, month, day, quantity, cam_type)

    def _get_deployment_data(self, year, month, day, quantity, cam_type):  # pylint:disable=too-many-arguments,too-many-locals
        assert 1 <= month <= 12
        assert (cam_type in [REDLIGHT, OVERHEIGHT])

        # Setup the cookies with these requests
        code = {REDLIGHT: 'RL', OVERHEIGHT: 'OH'}
        self.session.get('https://cw3.cite-web.com/citeweb3/Default.asp?ID=1488671')
        self.session.get('https://cw3.cite-web.com/citeweb3/citmenu.asp?DB=Baltimore{}&Site=Maryland'.format(
            code[cam_type]
        ))

        if cam_type == REDLIGHT:
            cookie_val = 'BaltimoreRL'
        elif cam_type == OVERHEIGHT:
            cookie_val = 'BaltimoreOH'
        else:
            raise AssertionError

        self.session.cookies.set('DBDisplay', None)
        self.session.cookies.set('DBDisplay', cookie_val)

        self.session.cookies.set('DB', None)
        self.session.cookies.set('DB', cookie_val)

        deploy_type = {REDLIGHT: 'DeplByMonth_BaltimoreRL.asp', OVERHEIGHT: 'DeplByMonth.asp'}
        results = []
        search_start_date = datetime(year, month, day)
        search_end_date = datetime(year, month, day, 23, 59, 59) + timedelta(days=quantity-1)

        for cur_year, cur_month in self._month_year_iter(month, year, search_end_date.month, search_end_date.year):
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
                    print("Skipping {}".format(elements))
                    continue

                if hasattr(row.find_all('td')[1].p, 'text') and hasattr(row.find_all('td')[2].p, 'text') and \
                        row.find_all('td')[1].p.text and row.find_all('td')[2].p.text:

                    act_start_date = datetime.strptime(row.find_all('td')[1].p.text, "%b %d, %Y %H:%M:%S")
                    act_end_date = datetime.strptime(row.find_all('td')[2].p.text, "%b %d, %Y %H:%M:%S")

                    # Make sure we are within the date range
                    if act_start_date >= search_start_date and act_end_date <= search_end_date:
                        logging.info("Adding data from %s", act_start_date)
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
