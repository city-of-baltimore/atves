"""Test suite for atves_database.py"""
# pylint:disable=protected-access
from datetime import date

from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from atves.atves_schema import AtvesAmberTimeRejects, AtvesApprovalByReviewDateDetails, AtvesByLocation, \
    AtvesCamLocations, AtvesTicketCameras, AtvesTrafficCounts
from atves.conduent import REDLIGHT, OVERHEIGHT


def test_atvesdb_build_db_conduent_red_light(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str):
    """Testing _build_db_conduent_red_light"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.build_location_db()
        ret = session.query(AtvesCamLocations.cam_type).filter(AtvesCamLocations.cam_type == 'RL')
        assert ret.count() == 0

        atvesdb_fixture.build_location_db()
        ret = session.query(AtvesCamLocations.cam_type,
                            AtvesCamLocations.lat,
                            AtvesCamLocations.long).filter(AtvesCamLocations.cam_type == 'RL')
        assert ret.count() > 100
        assert all((-76.73 < i[1] < -76.52 for i in ret.all()))
        assert all((39.2 < i[2] < 39.38 for i in ret.all()))


def test_atvesdb_build_db_conduent_overheight(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str):
    """Testing _build_db_conduent_overheight"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.build_location_db()
        ret = session.query(AtvesCamLocations.cam_type).filter(AtvesCamLocations.cam_type == 'OH')
        assert ret.count() == 0

        atvesdb_fixture.build_location_db()
        ret = session.query(AtvesCamLocations.cam_type,
                            AtvesCamLocations.lat,
                            AtvesCamLocations.long,
                            AtvesCamLocations.locationdescription).filter(AtvesCamLocations.cam_type == 'OH')
        assert ret.count() > 10

        # throw away None results, but make sure its not all of them
        lats = [-76.73 < i[1] < -76.52 for i in ret.all() if i[1]]
        lngs = [39.2 < i[2] < 39.38 for i in ret.all() if i[2]]
        assert all(lats)
        assert len(lats) > 8
        assert all(lngs)
        assert len(lngs) > 8


def test_atvesdb_build_db_speed_cameras(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str):
    """Testing _build_db_speed_cameras"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.build_location_db()
        ret = session.query(AtvesCamLocations.cam_type).filter(AtvesCamLocations.cam_type == 'SC')
        assert ret.count() == 0

        atvesdb_fixture.build_location_db()
        ret = session.query(AtvesCamLocations.cam_type,
                            AtvesCamLocations.lat,
                            AtvesCamLocations.long).filter(AtvesCamLocations.cam_type == 'SC')
        assert ret.count() > 10

        # throw away None results, but make sure its not all of them
        lats = [-76.73 < i[1] < -76.52 for i in ret.all() if i[1]]
        lngs = [39.2 < i[2] < 39.38 for i in ret.all() if i[2]]
        assert all(lats)
        assert len(lats) > 10
        assert all(lngs)
        assert len(lngs) > 10


def test_atvesdb_process_conduent_reject_numbers(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str):
    """Testing process_conduent_reject_numbers"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_conduent_reject_numbers(start_date=date(2020, 11, 1),
                                                                 end_date=date(2020, 11, 3))
        ret = session.query(AtvesTicketCameras)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_reject_numbers(start_date=date(2010, 11, 1), end_date=date(2010, 11, 3))
        ret = session.query(AtvesTicketCameras)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_reject_numbers(start_date=date(2020, 11, 1), end_date=date(2020, 11, 3))
        ret = session.query(AtvesTicketCameras)
        assert ret.count() > 300


def test_atvesdb_process_conduent_data_amber_time(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str):
    """Testing process_conduent_data_amber_time"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_conduent_data_amber_time(start_date=date(2020, 11, 1),
                                                                  end_date=date(2020, 11, 3))
        ret = session.query(AtvesAmberTimeRejects)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_amber_time(start_date=date(2010, 11, 1), end_date=date(2010, 11, 3))
        ret = session.query(AtvesAmberTimeRejects)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_amber_time(start_date=date(2020, 11, 1), end_date=date(2020, 11, 3))
        ret = session.query(AtvesAmberTimeRejects)
        assert ret.count() > 30


def test_atvesdb_process_conduent_data_approval_by_review_date_rl(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str):
    """Testing process_conduent_data_approval_by_review_date"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_conduent_data_approval_by_review_date(start_date=date(2020, 11, 1),
                                                                               end_date=date(2020, 11, 3),
                                                                               cam_type=REDLIGHT)
        ret = session.query(AtvesApprovalByReviewDateDetails)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_approval_by_review_date(start_date=date(2010, 11, 1),
                                                                      end_date=date(2010, 11, 3),
                                                                      cam_type=REDLIGHT)
        ret = session.query(AtvesApprovalByReviewDateDetails)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_approval_by_review_date(start_date=date(2020, 11, 1),
                                                                      end_date=date(2020, 11, 3),
                                                                      cam_type=REDLIGHT)
        ret = session.query(AtvesApprovalByReviewDateDetails)
        assert ret.count() > 1000


def test_atvesdb_process_conduent_data_approval_by_review_date_oh(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str):
    """Testing process_conduent_data_approval_by_review_date"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_conduent_data_approval_by_review_date(start_date=date(2020, 11, 1),
                                                                               end_date=date(2020, 11, 30),
                                                                               cam_type=OVERHEIGHT)
        ret = session.query(AtvesApprovalByReviewDateDetails)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_approval_by_review_date(start_date=date(2010, 11, 1),
                                                                      end_date=date(2010, 11, 30),
                                                                      cam_type=OVERHEIGHT)
        ret = session.query(AtvesApprovalByReviewDateDetails)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_approval_by_review_date(start_date=date(2020, 11, 1),
                                                                      end_date=date(2020, 11, 30),
                                                                      cam_type=OVERHEIGHT)
        ret = session.query(AtvesApprovalByReviewDateDetails)
        assert ret.count() > 40


def test_atvesdb_process_conduent_data_by_location(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str):
    """Testing process_conduent_data_by_location"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_conduent_data_by_location(start_date=date(2020, 11, 1),
                                                                   end_date=date(2020, 11, 3))
        ret = session.query(AtvesByLocation)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_by_location(start_date=date(2010, 11, 1), end_date=date(2010, 11, 3))
        ret = session.query(AtvesByLocation)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_by_location(start_date=date(2020, 11, 1), end_date=date(2020, 11, 3))
        ret = session.query(AtvesByLocation)
        assert ret.count() > 2000


def test_atvesdb_process_traffic_count_data(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str):
    """Testing process_traffic_count_data"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_traffic_count_data(start_date=date(2020, 11, 1), end_date=date(2020, 11, 3))
        ret = session.query(AtvesTrafficCounts)
        assert ret.count() == 0

        atvesdb_fixture.process_traffic_count_data(start_date=date(2010, 11, 1), end_date=date(2010, 11, 3))
        ret = session.query(AtvesTrafficCounts)
        assert ret.count() == 0

        atvesdb_fixture.process_traffic_count_data(start_date=date(2020, 11, 1), end_date=date(2020, 11, 3))
        ret = session.query(AtvesTrafficCounts)
        assert ret.count() > 100
