"""Tests atves.conduent"""
import numbers
from datetime import date, datetime

import pytest

import atves


def test_invalid_user_pass():
    """Tests the Conduent interface with an invalid username and password"""
    with pytest.raises(AssertionError):
        atves.conduent.Conduent('test', 'test')


def test_get_location_by_id(conduent_fixture):
    """Tests get_location_by_id"""
    # Camera 1 is a red light cam, and camera 2003 is an overheight cam
    for cam_type in [atves.conduent.REDLIGHT, atves.conduent.OVERHEIGHT]:
        ret = conduent_fixture.get_location_by_id(1, cam_type)
        assert ret is not None
        assert len(ret) == 9
        assert str(ret['site_code']).isnumeric()
        assert str(ret['location']) is not None
        assert str(ret['jurisdiction']).isnumeric()
        assert str(ret['date_created']) is not None
        assert str(ret['created_by']) is not None
        if cam_type == atves.conduent.REDLIGHT:
            assert str(ret['cam_type']) == 'RL'
        else:
            assert str(ret['cam_type']) == 'OH'
        assert ret['effective_date'] is not None
        assert str(ret['speed_limit']).isnumeric()
        assert ret['status'] == 'Active'


def test_get_location_by_id_invalid(conduent_fixture):
    """Tests get_location_by_id with a bad id"""
    for cam_type in [atves.conduent.REDLIGHT, atves.conduent.OVERHEIGHT]:
        ret = conduent_fixture.get_location_by_id(9999999999, cam_type)
        assert ret is not None
        assert len(ret) == 9
        assert ret['site_code'] is None
        assert ret['location'] is None
        assert ret['jurisdiction'] is None
        assert ret['date_created'] is None
        assert ret['created_by'] is None
        assert ret['cam_type'] is None
        assert ret['effective_date'] is None
        assert ret['speed_limit'] is None
        assert ret['status'] is None

    # invalid cam type
    with pytest.raises(AssertionError):
        conduent_fixture.get_location_by_id(9999999999, 30)


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
        assert len(ret) > 5
        assert len(ret[0]) == 8

        assert str(ret[0]['id']).isnumeric()
        assert isinstance(ret[0]['start_time'], date)
        assert start <= ret[0]['start_time']
        assert isinstance(ret[0]['end_time'], date)
        assert end >= ret[0]['end_time']
        assert str(ret[0]['location']) is not None
        assert str(ret[0]['officer']) is not None
        assert str(ret[0]['equip_type'])
        assert str(ret[0]['issued'])
        assert str(ret[0]['rejected'])

    start_date = datetime(2020, 11, 1, 0, 0)
    end_date = datetime(2020, 11, 30, 0, 0)

    ret_all = conduent_fixture.get_deployment_data(start_date.date(), end_date.date(), atves.conduent.ALLCAMS)
    verify_dataframe(ret_all, start_date, end_date)

    ret_redlight = conduent_fixture.get_deployment_data(start_date.date(), end_date.date(), atves.conduent.REDLIGHT)
    verify_dataframe(ret_redlight, start_date, end_date)

    ret_overheight = conduent_fixture.get_deployment_data(start_date.date(), end_date.date(), atves.conduent.OVERHEIGHT)
    verify_dataframe(ret_overheight, start_date, end_date)

    assert len(ret_all) == len(ret_redlight) + len(ret_overheight)

    # invalid cam type
    with pytest.raises(AssertionError):
        conduent_fixture.get_deployment_data(start_date.date(), end_date.date(), 30)


def test_get_amber_time_rejects_report(conduent_fixture):
    """Tests get_amber_time_rejects_report"""
    start_date = date(2020, 11, 1)
    end_date = date(2020, 11, 30)
    ret = conduent_fixture.get_amber_time_rejects_report(start_date, end_date)
    assert len(ret) > 5


def test_get_approval_by_review_date_details(conduent_fixture):
    """Tests get_approval_by_review_date_details"""

    def verify_dataframes(dataframe):
        assert len(dataframe) > 5
        assert isinstance(dataframe.iloc[0].Disapproved, numbers.Number)
        assert isinstance(dataframe.iloc[0].Approved, numbers.Number)
        assert isinstance(dataframe.iloc[0].get('Vio Date'), date)

        assert dataframe.iloc[0].get('Review Status') in {'Plate Glare         ', 'Camera Not Focused  ',
                                                          'Right on Red        ', 'Not Issued          ',
                                                          'Stop Bar Not Visible', 'Unclear tag         ',
                                                          'Officer Present     ', 'No Violation        ',
                                                          'Flash Not Working   ', 'Funeral Procession -',
                                                          'Picture Bad         ', 'Vehicle Make Failure',
                                                          'Bad Weather         ', 'Poor Video Quality  ',
                                                          'Emergency Vehicle   ', 'Yellow Phase        ',
                                                          'No Image            ', 'Approved', 'Temporary Tag       ',
                                                          'Plate Unreadable/Mar', 'Plate Obstructed    ',
                                                          'Stop Bar Missing    ', 'Car Obstructed      ',
                                                          'Yielding To Emergenc', 'Equipment Malfunctio',
                                                          'Data Bar Error      ', 'Missing Make        ',
                                                          'Wrong Plate Keyed   ', 'Missing Video       ',
                                                          'Image Mismatch      ', 'Duplicate Violation ',
                                                          'Missing Traffic Ligh', 'Signal Glare        '}

    start_date = date(2020, 11, 1)
    end_date = date(2020, 11, 30)

    # redlight, all locations
    ret = conduent_fixture.get_approval_by_review_date_details(start_date, end_date, atves.conduent.REDLIGHT)
    verify_dataframes(ret)

    # redlight, specific location
    ret = conduent_fixture.get_approval_by_review_date_details(start_date, end_date, atves.conduent.REDLIGHT,
                                                               '1,1002 - Reisterstown Rd SB @ Patterson Ave')
    verify_dataframes(ret)

    # redlight, bad location
    ret = conduent_fixture.get_approval_by_review_date_details(start_date, end_date, atves.conduent.REDLIGHT,
                                                               'NOTALOCATION')
    assert ret is None

    # overheight, all locations
    ret = conduent_fixture.get_approval_by_review_date_details(start_date, end_date, atves.conduent.OVERHEIGHT)
    verify_dataframes(ret)

    # overheight, specific location
    ret = conduent_fixture.get_approval_by_review_date_details(start_date, end_date, atves.conduent.OVERHEIGHT,
                                                               '2,2014 - 4000 blk Pulaski Hwy WB')
    verify_dataframes(ret)

    # overheight, bad location
    ret = conduent_fixture.get_approval_by_review_date_details(start_date, end_date, atves.conduent.OVERHEIGHT,
                                                               'NOTALOCATION')
    assert ret is None

    # invalid cam type
    with pytest.raises(AssertionError):
        conduent_fixture.get_approval_by_review_date_details(start_date, end_date, 30)


def test_get_approval_summary_by_queue(conduent_fixture):
    """Tests get_approval_summary_by_queue"""
    start_date = date(2020, 11, 1)
    end_date = date(2020, 11, 30)

    # redlight, all locations
    ret = conduent_fixture.get_approval_summary_by_queue(start_date, end_date, atves.conduent.REDLIGHT)
    assert len(ret) > 5

    # redlight, specific location
    ret = conduent_fixture.get_approval_summary_by_queue(start_date, end_date, atves.conduent.REDLIGHT,
                                                         '2,1014 - E North Ave WB @ N Howard St')
    assert len(ret) > 5

    # redlight, bad location
    ret = conduent_fixture.get_approval_summary_by_queue(start_date, end_date, atves.conduent.REDLIGHT,
                                                         'NOTALOCATION')
    assert ret is None

    # overheight, all locations
    ret = conduent_fixture.get_approval_summary_by_queue(start_date, end_date, atves.conduent.OVERHEIGHT)
    assert len(ret) > 5

    # overheight, specific location
    ret = conduent_fixture.get_approval_summary_by_queue(start_date, end_date, atves.conduent.OVERHEIGHT,
                                                         '2,2014 - 4000 blk Pulaski Hwy WB')
    assert len(ret) > 5

    # overheight, bad location
    ret = conduent_fixture.get_approval_summary_by_queue(start_date, end_date, atves.conduent.OVERHEIGHT,
                                                         'NOTALOCATION')
    assert ret is None

    # invalid cam type
    with pytest.raises(AssertionError):
        conduent_fixture.get_approval_summary_by_queue(start_date, end_date, 30)


def test_get_client_summary_by_location(conduent_fixture):
    """Tests get_client_summary_by_location"""
    # We have to pull this report by day, so its slow
    start_date = date(2020, 11, 1)
    end_date = date(2020, 11, 1)

    # redlight, all locations
    ret = conduent_fixture.get_client_summary_by_location(start_date, end_date, atves.conduent.REDLIGHT)
    assert len(ret) > 5

    # redlight, specific location
    ret = conduent_fixture.get_client_summary_by_location(start_date, end_date, atves.conduent.REDLIGHT,
                                                          '1014,1014 - E North Ave WB @ N Howard St')
    assert len(ret) > 5

    # redlight, bad location
    ret = conduent_fixture.get_client_summary_by_location(start_date, end_date, atves.conduent.REDLIGHT,
                                                          'NOTALOCATION')
    assert ret is None

    # overheight, all locations
    ret = conduent_fixture.get_client_summary_by_location(start_date, end_date, atves.conduent.OVERHEIGHT)
    assert len(ret) > 5

    # overheight, specific location
    ret = conduent_fixture.get_client_summary_by_location(start_date, end_date, atves.conduent.OVERHEIGHT,
                                                          '2014,2014 - 4000 blk Pulaski Hwy WB')
    assert len(ret) > 5

    # overheight, bad location
    ret = conduent_fixture.get_client_summary_by_location(start_date, end_date, atves.conduent.OVERHEIGHT,
                                                          'NOTALOCATION')
    assert ret is None

    # all cams
    ret = conduent_fixture.get_client_summary_by_location(start_date, end_date, atves.conduent.ALLCAMS)
    assert len(ret) > 10

    # invalid cam type
    with pytest.raises(AssertionError):
        conduent_fixture.get_client_summary_by_location(start_date, end_date, 30)


def test_get_expired_by_location(conduent_fixture):
    """Tests get_expired_by_location"""
    start_date = date(2020, 11, 1)
    end_date = date(2020, 11, 30)

    # all locations
    ret = conduent_fixture.get_expired_by_location(start_date, end_date)
    assert len(ret) > 5

    # specific location
    ret = conduent_fixture.get_expired_by_location(start_date, end_date, '1014,1014 - E North Ave WB @ N Howard St')
    assert len(ret) == 1

    # bad location
    ret = conduent_fixture.get_expired_by_location(start_date, end_date, 'NOTALOCATION')
    assert ret is None


def test_get_in_city_vs_out_of_city(conduent_fixture):
    """Tests get_in_city_vs_out_of_city"""
    start_date = date(2020, 11, 1)
    end_date = date(2020, 11, 30)

    ret = conduent_fixture.get_in_city_vs_out_of_city(start_date, end_date)
    assert ret.at[0, 'TCount'] >= 15000
    assert ret.at[0, 'INState'] >= 10000
    assert ret.at[0, 'OutState'] >= 5000


def test_get_straight_thru_vs_right_turn(conduent_fixture):
    """Tests get_straight_thru_vs_right_turn"""
    start_date = date(2020, 11, 1)
    end_date = date(2020, 11, 30)

    # all locations
    ret = conduent_fixture.get_straight_thru_vs_right_turn(start_date, end_date)
    assert len(ret) > 5

    # specific location
    ret = conduent_fixture.get_straight_thru_vs_right_turn(start_date, end_date,
                                                           '2,1014 - E North Ave WB @ N Howard St')
    assert len(ret) > 5

    # bad location
    ret = conduent_fixture.get_straight_thru_vs_right_turn(start_date, end_date, 'NOTALOCATION')
    assert ret is None


def test_get_traffic_counts_by_location(conduent_fixture):
    """Tests get_traffic_counts_by_location"""
    start_date = date(2020, 11, 1)
    end_date = date(2020, 11, 30)

    # all locations
    ret = conduent_fixture.get_traffic_counts_by_location(start_date, end_date)
    assert len(ret) > 5

    # specific location
    ret = conduent_fixture.get_traffic_counts_by_location(start_date, end_date,
                                                          '1742,1742 Perring Pkwy SB @ Echodale Ave')
    assert len(ret) > 5

    # bad location
    ret = conduent_fixture.get_traffic_counts_by_location(start_date, end_date, 'NOTALOCATION')
    assert ret is None


def test_get_violations_issued_by_location(conduent_fixture):
    """Tests get_violations_issued_by_location"""
    start_date = date(2020, 11, 1)
    end_date = date(2020, 11, 30)
    ret = conduent_fixture.get_violations_issued_by_location(start_date, end_date)
    assert len(ret) > 5


def test_get_daily_self_test(conduent_fixture):
    """Tests get_daily_self_test"""
    start_date = date(2020, 11, 1)
    end_date = date(2020, 11, 30)
    ret = conduent_fixture.get_daily_self_test(start_date, end_date)
    assert len(ret) > 5


def test_get_pending_client_approval(conduent_fixture):
    """Tests get_pending_client_approval"""
    ret = conduent_fixture.get_pending_client_approval(atves.conduent.REDLIGHT)
    assert len(ret) >= 1

    ret = conduent_fixture.get_pending_client_approval(atves.conduent.OVERHEIGHT)
    assert len(ret) >= 1

    # invalid cam type
    with pytest.raises(AssertionError):
        conduent_fixture.get_pending_client_approval(30)

    with pytest.raises(AssertionError):
        conduent_fixture.get_pending_client_approval("invalid")
