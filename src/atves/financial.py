"""Interface with the financial reporting system for Baltimore City"""
import re
from datetime import date
from io import StringIO
from typing import Any, Dict, List, Tuple, Union

import mechanize  # type: ignore
import pandas as pd  # type: ignore
from bs4 import BeautifulSoup  # type: ignore
from loguru import logger
from ntlm import HTTPNtlmAuthHandler  # type:ignore
from tenacity import retry, wait_random_exponential, stop_after_attempt


class CobReports:
    """Interacts with the COB Reports on Baltimore City's external sql server website"""
    def __init__(self, username: str, password: str, baseurl: str = 'https://cobrpt02.rsm.cloud'):
        self.browser = mechanize.Browser()
        self.browser.set_handle_robots(False)
        self.browser.addheaders = [('User-agent',
                                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                    '(KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36')]

        # create the NTLM authentication handler
        passman = mechanize.HTTPPasswordMgrWithDefaultRealm()
        passman.add_password(None, 'https://cobrpt02.rsm.cloud', username, password)
        self.browser.add_handler(HTTPNtlmAuthHandler.HTTPNtlmAuthHandler(passman))

        if self.browser.open('{}/Reports'.format(baseurl)).code != 200:
            raise AssertionError('Invalid username/password')

        self.baseurl = baseurl

    def get_general_ledger_detail(self, start_date: date, end_date: date,  # pylint:disable=too-many-locals
                                  legacy_account_no: str, agency: str = '0') -> pd.DataFrame:
        """
        Pulls the COB Reports > Monthly Financials and Support > General Ledger Detail report. This holds most of the
        transaction level data for the city
        :param start_date: date to begin the transaction range
        :param end_date: date to end the transaction range
        :param legacy_account_no: account number to search; do not include dashes
        :param agency: agency code to filter on
        :return df: dataframe with the data from the specified search. Has the following headers:
            JournalEntryNo
            LedgerPostingDate
            AccountNo
            LegacyAccountNo
            Amount
            SourceJournal
            TrxReference
            TrxDescription
            UserWhoPosted
            TrxNo
            VendorIDOrCustomerID
            VendorOrCustomerName
            DocumentNo
            TrxSource
            AccountDescription
            AccountType
            AgencyOrCategory
        """
        logger.info("Getting the general ledger detail report")
        resp = self.browser.open(
            'https://cobrpt02.rsm.cloud/ReportServer/Pages/ReportViewer.aspx?%2FCOB%20Reports%2FMonthly%20Financials%20'
            'and%20Support%2FGeneral_Ledger_Detail&rc:showbackbutton=true').read()

        soup = BeautifulSoup(resp, features="html.parser")
        html = soup.find('form', id='ReportViewerForm').prettify().encode('utf8')

        self.browser.select_form(id='ReportViewerForm')
        self.browser.form.set_all_readonly(False)

        ctrl_dict: Dict[str, Union[str, List]] = {
            'AjaxScriptManager': 'AjaxScriptManager|ReportViewerControl$ctl04$ctl00',
            '__VIEWSTATE': soup.find('input', {'name': '__VIEWSTATE'}).get('value'),
            '__VIEWSTATEGENERATOR': soup.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value'),
            'ReportViewerControl$ctl11': 'standards',
            'ReportViewerControl$AsyncWait$HiddenCancelField': 'False',
            'ReportViewerControl$ctl04$ctl03$txtValue': start_date.strftime('%#m/%#d/%Y'),
            'ReportViewerControl$ctl04$ctl05$txtValue': end_date.strftime('%#m/%#d/%Y'),
            'ReportViewerControl$ctl04$ctl07$txtValue': legacy_account_no,
            'ReportViewerControl$ctl04$ctl09$ddValue': ['0'],
            'ReportViewerControl$ctl04$ctl31$ddValue': [agency],
            'ReportViewerControl$ToggleParam$collapse': 'false',
            'ReportViewerControl$ctl07$collapse': 'false',
            'ReportViewerControl$ctl09$ReportControl$ctl04': '100',
            '__ASYNCPOST': 'true',
        }

        self._set_controls(ctrl_dict)
        resp = self.browser.submit().read()

        # Get the download URL
        response_url_base_group = re.search(r'"ExportUrlBase":"(.*?)"', resp.decode())
        if response_url_base_group is None:
            raise AssertionError("Unable to find export URL")
        response_url_base = response_url_base_group.group(1)

        resp_dict = self.parse_ltiv_data(resp.decode())
        nav_corrector = BeautifulSoup(resp_dict['NavigationCorrector_ctl00'][0], features='html.parser')

        ctrl_dict['AjaxScriptManager'] = 'AjaxScriptManager|ReportViewerControl$ctl09$Reserved_AsyncLoadTarget'
        ctrl_dict['NavigationCorrector$NewViewState'] = nav_corrector.find('input', {
            'id': 'NavigationCorrector_NewViewState'}).get('value')
        ctrl_dict['ReportViewerControl$ctl10'] = 'ltr'
        ctrl_dict['__EVENTTARGET'] = resp_dict['__EVENTTARGET'][0]
        ctrl_dict['__VIEWSTATE'] = resp_dict['__VIEWSTATE'][0]
        ctrl_dict['__VIEWSTATEGENERATOR'] = resp_dict['__VIEWSTATEGENERATOR'][0]

        self._make_response_and_submit(ctrl_dict, html)

        csv_data = self.browser.open("{}{}CSV".format(self.baseurl, response_url_base.replace(r'\u0026', '&'))).read()

        logger.debug("Got {} bytes of data".format(len(csv_data)))

        dtypes = {
            'JournalEntryNo': str,
            'LedgerPostingDate': str,
            'AccountNo': str,
            'LegacyAccountNo': str,
            'Amount': str,  # converted to float below
            'SourceJournal': str,
            'TrxReference': str,
            'TrxDescription': str,
            'UserWhoPosted': str,
            'TrxNo': str,
            'VendorIDOrCustomerID': str,
            'VendorOrCustomerName': str,
            'DocumentNo': str,
            'TrxSource': str,
            'AccountDescription': str,
            'AccountType': str,
            'AgencyOrCategory': str,
        }
        parse_dates = ['LedgerPostingDate']
        ret = pd.read_csv(StringIO(csv_data.decode('utf-8')), delimiter=',', dtype=dtypes, parse_dates=parse_dates)

        # strip the whitespace
        df_obj = ret.select_dtypes(['object'])
        ret[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

        # Make the amount a float
        ret['Amount'] = ret['Amount'].replace(r'[\$,)]', '', regex=True).replace('[(]', '-', regex=True).fillna(0)\
            .astype(float)

        return ret

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(7), reraise=True)
    def _make_response_and_submit(self, ctrl_dict: Dict[str, Union[str, List]], html: str) -> str:
        """
        Helper to regenerate a response, assign it to the form, and resubmit it. Used for postbacks
        :param ctrl_dict: Dictionary of page control ids and the values they should be set to
        :return:
        """
        response = mechanize.make_response(html, [('Content-Type', 'text/html')],
                                           self.browser.geturl(), 200, 'OK')
        self.browser.set_response(response)
        self.browser.select_form(id='ReportViewerForm')
        self.browser.form.set_all_readonly(False)

        self._set_controls(ctrl_dict)

        return self.browser.submit().read()

    @staticmethod
    def parse_ltiv_data(data: str) -> Dict[str, Tuple[str, str]]:
        """
        Parses the data that comes back from the aspx pages. Its in the format LENGTH|TYPE|ID|VALUE
        :param data:
        :return: Returns {ID: (VALUE, TYPE), ID: (VALUE, TYPE)}
        """

        def get_next_element(idata: str, ilength: int = None) -> Tuple[str, str]:
            """Parser that pulls off an element to the next delimiter, and optionally will read ilength bytes"""
            if ilength is not None:
                if not (ilength < len(idata) and idata[ilength] == '|'):
                    raise AssertionError("Malformed input. Expected delimiter where there wasn't one. idata: {}"
                                         .format(idata[:100]))
                iret = idata[:ilength]
                idata = idata[ilength + 1:]  # drop the delimiter
                return iret, idata
            return get_next_element(idata, idata.index('|'))

        ret = {}

        while data:
            s_length, data = get_next_element(data)
            length = int(s_length)
            data_type, data = get_next_element(data)
            data_id, data = get_next_element(data)
            value, data = get_next_element(data, length)

            ret[data_id] = (value, data_type)
        return ret

    def _set_controls(self, ctrl_dict: Dict[str, Any]) -> None:
        for ctrl_id, val in ctrl_dict.items():
            try:
                ctrl = self.browser.form.find_control(name=ctrl_id)
                ctrl.disabled = False
                ctrl.value = val
            except mechanize.ControlNotFoundError:
                self.browser.form.new_control('hidden', ctrl_id, {'value': val})
        self.browser.form.fixup()
        self._log_controls()

    def _log_controls(self) -> None:
        logger.debug('\n'.join(
            ['%s: %s *%s*' % (c.name, c.value, c.disabled) if c.disabled else '%s: %s' % (c.name, c.value) for c in
             self.browser.form.controls]))
