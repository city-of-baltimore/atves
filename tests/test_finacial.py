"""Tests test_finacial.py"""
from datetime import date


def test_get_general_ledger_detail(cobreport_fixture):
    """Tests get_general_ledger_detail"""
    res = cobreport_fixture.get_general_ledger_detail(date(2021, 2, 1), date(2021, 2, 4), 'A00119120300000', '55')
    assert len(res) > 5
    assert len(res.columns) == 17
