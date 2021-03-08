"""Test suite for axsis.py"""
# pylint:disable=protected-access
from datetime import date


def test_axsis_get_traffic_counts(axsis_fixture):
    """Test suite get_traffic_counts"""
    start_date = date(2020, 11, 1)
    end_date = date(2020, 12, 5)
    ret = axsis_fixture.get_traffic_counts(start_date, end_date)
    assert len(ret.columns) == 39  # 35 date columns plus location code, description, first and last traf evt
    assert len(ret) > 150


def test_axsis_get_client_id(axsis_fixture):
    """Test suite _get_client_id"""
    axsis_fixture._get_client_id()
    assert axsis_fixture.client_id is not None
    assert axsis_fixture.client_code is not None


def test_axsis_get_reports(axsis_fixture):
    """Test suite _get_reports"""
    assert axsis_fixture._get_reports('ENFORCEMENT EXPIRE') == 183
    assert axsis_fixture._get_reports('OFFICER ACTION') == 629596
    assert axsis_fixture._get_reports('NOTAREALREPORT') is None


def test_axsis_get_reports_detail(axsis_fixture):
    """Test suite get_reports_detail"""
    ret = axsis_fixture.get_reports_detail('ENFORCEMENT EXPIRE')
    assert isinstance(ret, dict)
    assert ret['Definition']['ReportName'] == 'ENFORCEMENT EXPIRE'

    ret = axsis_fixture.get_reports_detail('OFFICER ACTION')
    assert isinstance(ret, dict)
    assert ret['Definition']['ReportName'] == 'OFFICER ACTION'

    assert axsis_fixture.get_reports_detail('NOTAREALREPORT') is None


def test_get_location_info(axsis_fixture):
    """Test suite get_location_info"""
    assert axsis_fixture.get_location_info('BALP056') == '2800 BLK REISTERSTOWN RD EB'
    assert axsis_fixture.get_location_info('BAL103') == '6000 BLK HILLEN RD SB'
    assert axsis_fixture.get_location_info('BALP111') == '2800 BLK LOCH RAVEN NB'
    assert axsis_fixture.get_location_info('INVALID') is None
