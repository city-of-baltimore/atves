"""Tests test_finacial.py"""
from datetime import date

import pytest

import atves


def test_financial_invalid_user_pass():
    """Tests the Financial interface with an invalid username and password"""
    with pytest.raises(AssertionError):
        atves.financial.CobReports('test', 'test')


@pytest.mark.financial
def test_financial_get_general_ledger_detail(cobreport_fixture):
    """Tests get_general_ledger_detail"""
    start_date = date(2021, 2, 1)
    end_date = date(2021, 2, 4)
    res = cobreport_fixture.get_general_ledger_detail(start_date, end_date, 'A00119120300000', '55')
    assert len([row['LedgerPostingDate']
                for _, row in res.iterrows()
                if not start_date <= row['LedgerPostingDate'].date() <= end_date]) == 0
    assert len(res) > 5
    assert len(res.columns) == 17
