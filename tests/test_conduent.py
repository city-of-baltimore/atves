"""Tests atves.conduent"""
from datetime import date, datetime
import numbers
import pytest

import atves


def test_invalid_user_pass():
    """Tests the Conduent interface with an invalid username and password"""
    with pytest.raises(AssertionError):
        atves.conduent.Conduent('test', 'test')


def test_get_location_by_id(conduent_fixture):
    """Tests get_location_by_id"""
    ret = conduent_fixture.get_location_by_id(1)
    assert ret is not None
    assert len(ret) == 9
    assert str(ret['Site Code']).isnumeric()
    assert str(ret['Location']) is not None
    assert str(ret['Jurisdiction']).isnumeric()
    assert str(ret['Date Created']) is not None
    assert str(ret['Created By']) is not None
    assert str(ret['Cam Type']) is not None
    assert ret['Effective Date'] is not None
    assert str(ret['Speed Limit']).isnumeric()
    assert ret['Status'] == 'Active'


def test_get_overheight_cameras(conduent_fixture):
    """Tests get_overheight_cameras"""
    ret = conduent_fixture.get_overheight_cameras()
    assert len(ret) > 1
    assert {len(x) for x in ret} == {2}
    assert str(ret[0][0]).isnumeric()
    assert str(ret[0][1]) is not None


def test_get_deployment_data(conduent_fixture):
    """Tests get_deployment_data"""
    def verify_dataframe(ret, start, end):
        assert len(ret) > 10
        assert len(ret[0]) == 8

        assert str(ret[0]['id']).isnumeric()
        assert isinstance(ret[0]['start_time'], date)
        assert isinstance(ret[0]['end_time'], date)
        assert str(ret[0]['location']) is not None
        assert str(ret[0]['officer']) is not None
        assert str(ret[0]['equip_type'])
        assert str(ret[0]['issued'])
        assert str(ret[0]['rejected'])

    start_date = date(2020, 2, 1)
    end_date = date(2020, 2, 28)

    ret_all = conduent_fixture.get_deployment_data(start_date, end_date, atves.conduent.ALLCAMS)
    verify_dataframe(ret_all, start_date, end_date)

    ret_redlight = conduent_fixture.get_deployment_data(start_date, end_date, atves.conduent.REDLIGHT)
    verify_dataframe(ret_redlight, start_date, end_date)

    ret_overheight = conduent_fixture.get_deployment_data(start_date, end_date, atves.conduent.OVERHEIGHT)
    verify_dataframe(ret_overheight, start_date, end_date)

    assert len(ret_all) == len(ret_redlight) + len(ret_overheight)


"""
        
        assert isinstance(ret.iLocationCode, pandas.core.series.Series)
        assert isinstance(ret, pandas.core.frame.DataFrame)

        for i in ret.VioDate:
            assert start <= datetime.strptime(i, '%m/%d/%Y %I:%M:%S %p') <= end
"""


def test_get_amber_time_rejects_report(conduent_fixture):
    """Tests get_amber_time_rejects_report"""
    start_date = date(2020, 2, 1)
    end_date = date(2020, 2, 1)
    ret = conduent_fixture.get_amber_time_rejects_report(start_date, end_date)
    assert len(ret) > 10


def test_get_approval_by_review_date_details(conduent_fixture):
    """Tests get_approval_by_review_date_details"""
    def verify_dataframes(dataframe):
        assert len(dataframe) > 10
        assert isinstance(dataframe.iloc[0].Disapproved, numbers.Number)
        assert isinstance(dataframe.iloc[0].Approved, numbers.Number)
        assert isinstance(dataframe.iloc[0].Officer, str)
        assert isinstance(datetime.strptime(dataframe.iloc[0].get('Vio Date'), '%b %d %Y %I:%M%p'), datetime)
        assert dataframe.iloc[0].get('Review Status') in ['Approved', 'Disapproved']
        assert isinstance(datetime.strptime(dataframe.iloc[0].get('Review Date'), "%m/%d/%Y"), datetime)
        assert isinstance(datetime.strptime(dataframe.iloc[0].get('Review Time'), " %I:%M%p"), datetime)
        assert isinstance(datetime.strptime(dataframe.iloc[0].st, "%H:%M:%S"), datetime)

    # todo test locations
    start_date = date(2020, 2, 28)
    end_date = date(2020, 2, 28)
    ret = conduent_fixture.get_approval_by_review_date_details(start_date, end_date, atves.conduent.REDLIGHT)
    verify_dataframes(ret)

    ret = conduent_fixture.get_approval_by_review_date_details(start_date, end_date, atves.conduent.OVERHEIGHT)
    verify_dataframes(ret)


def test_get_approval_summary_by_queue(conduent_fixture):
    """Tests get_approval_summary_by_queue"""
    # todo test locations
    start_date = date(2020, 2, 1)
    end_date = date(2020, 2, 28)
    ret = conduent_fixture.get_approval_summary_by_queue(start_date, end_date, atves.conduent.REDLIGHT)
    assert len(ret) > 10

    ret = conduent_fixture.get_approval_summary_by_queue(start_date, end_date, atves.conduent.OVERHEIGHT)
    assert len(ret) > 10


def test_get_client_summary_by_location(conduent_fixture):
    """Tests get_client_summary_by_location"""
    # todo test locations
    start_date = date(2020, 2, 28)
    end_date = date(2020, 2, 28)
    ret = conduent_fixture.get_client_summary_by_location(start_date, end_date, atves.conduent.REDLIGHT)
    assert len(ret) > 10

    ret = conduent_fixture.get_client_summary_by_location(start_date, end_date, atves.conduent.OVERHEIGHT)
    assert len(ret) > 10


def test_get_expired_by_location(conduent_fixture):
    """Tests get_expired_by_location"""
    # todo test locations
    start_date = date(2020, 2, 1)
    end_date = date(2020, 2, 28)
    ret = conduent_fixture.get_expired_by_location(start_date, end_date)
    assert len(ret) > 10


def test_get_in_city_vs_out_of_city(conduent_fixture):
    """Tests get_in_city_vs_out_of_city"""
    # todo test locations
    start_date = date(2020, 2, 1)
    end_date = date(2020, 2, 28)
    ret = conduent_fixture.get_in_city_vs_out_of_city(start_date, end_date)
    assert len(ret) > 10


def test_get_straight_thru_vs_right_turn(conduent_fixture):
    """Tests get_straight_thru_vs_right_turn"""
    # todo test locations
    start_date = date(2020, 2, 1)
    end_date = date(2020, 2, 28)
    ret = conduent_fixture.get_straight_thru_vs_right_turn(start_date, end_date)
    assert len(ret) > 10


def test_get_traffic_counts_by_location(conduent_fixture):
    """Tests get_traffic_counts_by_location"""
    # todo test locations
    start_date = date(2020, 2, 1)
    end_date = date(2020, 2, 28)
    ret = conduent_fixture.get_traffic_counts_by_location(start_date, end_date)
    assert len(ret) > 10


def test_get_violations_issued_by_location(conduent_fixture):
    """Tests get_violations_issued_by_location"""
    start_date = date(2020, 2, 1)
    end_date = date(2020, 2, 28)
    ret = conduent_fixture.get_violations_issued_by_location(start_date, end_date)
    assert len(ret) > 10


def get_daily_self_test(conduent_fixture):
    """Tests get_daily_self_test"""
    start_date = date(2020, 2, 1)
    end_date = date(2020, 2, 28)
    ret = conduent_fixture.get_daily_self_test(start_date, end_date)
    assert len(ret) > 10


def get_pending_client_approval(conduent_fixture):
    """Tests get_pending_client_approval"""
    ret = conduent_fixture.get_pending_client_approval(atves.conduent.OVERHEIGHT)
    assert len(ret) > 10

    ret = conduent_fixture.get_pending_client_approval(atves.conduent.REDLIGHT)
    assert len(ret) > 10

    with pytest.raises(AssertionError):
        conduent_fixture.get_pending_client_approval("invalid")
