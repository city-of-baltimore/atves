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


def test_get_location_summary_by_lane(axsis_fixture):
    """Test get_location_summary_by_lane"""
    start_date = date(2021, 5, 3)
    end_date = date(2021, 5, 4)
    ret = axsis_fixture.get_location_summary_by_lane(start_date, end_date)
    assert len(ret.columns) == 17
    assert len(ret) == 235


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


def test_get_officer_actions(axsis_fixture):
    """Test suite get_officer_actions"""
    res = axsis_fixture.get_officer_actions(date(2021, 11, 5), date(2021, 11, 8))
    assert any(res['1']['Date'].isin([date(2021, 11, 5)]))
    assert not any(res['1']['Date'].isin([date(2021, 11, 6)]))
    assert not any(res['1']['Date'].isin([date(2021, 11, 7)]))
    assert any(res['1']['Date'].isin([date(2021, 11, 8)]))
    assert len(res['1']['Date'].unique()) == 2  # there is missing data on the 6th and 7th

    assert any(res['0']['Action Date'].isin([date(2021, 11, 5)]))
    assert any(res['0']['Action Date'].isin([date(2021, 11, 6)]))
    assert not any(res['0']['Action Date'].isin([date(2021, 11, 7)]))
    assert any(res['0']['Action Date'].isin([date(2021, 11, 8)]))
    assert len(res['0']['Action Date'].unique()) == 3  # there is missing data on the 7th

    # Test failure case
    res = axsis_fixture.get_officer_actions(date(2021, 11, 7), date(2021, 11, 7))
    assert res['0'].empty
    assert res['1'].empty
