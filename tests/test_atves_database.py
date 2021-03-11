"""Test suite for atves_database.py"""
# pylint:disable=protected-access
from datetime import date
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from atves.conduent import REDLIGHT, OVERHEIGHT
from atves.atves_schema import AtvesAmberTimeRejects, AtvesApprovalByReviewDateDetails, AtvesByLocation, \
    AtvesCamLocations, AtvesTicketCameras, AtvesTrafficCounts


def test_atvesdb_build_db_conduent_red_light(atvesdb_fixture, conn_str):
    """Testing _build_db_conduent_red_light"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        ret = session.query(AtvesCamLocations.cam_type).filter(AtvesCamLocations.cam_type == 'RL')
        assert ret.count() > 100


def test_atvesdb_build_db_conduent_overheight(atvesdb_fixture, conn_str):
    """Testing _build_db_conduent_overheight"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        ret = session.query(AtvesCamLocations.cam_type).filter(AtvesCamLocations.cam_type == 'OH')
        assert ret.count() > 10


def test_atvesdb_build_db_speed_cameras(atvesdb_fixture, conn_str):
    """Testing _build_db_speed_cameras"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        ret = session.query(AtvesCamLocations.cam_type).filter(AtvesCamLocations.cam_type == 'SC')
        assert ret.count() > 10


def test_atvesdb_process_conduent_reject_numbers(atvesdb_fixture, conn_str):
    """Testing process_conduent_reject_numbers"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture.process_conduent_reject_numbers(start_date=date(2010, 11, 1), end_date=date(2010, 11, 7))
        ret = session.query(AtvesTicketCameras)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_reject_numbers(start_date=date(2020, 11, 1), end_date=date(2020, 11, 7))
        ret = session.query(AtvesTicketCameras)
        assert ret.count() > 900


def test_atvesdb_process_conduent_data_amber_time(atvesdb_fixture, conn_str):
    """Testing process_conduent_data_amber_time"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture.process_conduent_data_amber_time(start_date=date(2010, 11, 1), end_date=date(2010, 11, 7))
        ret = session.query(AtvesAmberTimeRejects)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_amber_time(start_date=date(2020, 11, 1), end_date=date(2020, 11, 7))
        ret = session.query(AtvesAmberTimeRejects)
        assert ret.count() > 100


def test_atvesdb_process_conduent_data_approval_by_review_date_rl(atvesdb_fixture, conn_str):
    """Testing process_conduent_data_approval_by_review_date"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture.process_conduent_data_approval_by_review_date(start_date=date(2010, 11, 1),
                                                                      end_date=date(2010, 11, 7),
                                                                      cam_type=REDLIGHT)
        ret = session.query(AtvesApprovalByReviewDateDetails)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_approval_by_review_date(start_date=date(2020, 11, 1),
                                                                      end_date=date(2020, 11, 7),
                                                                      cam_type=REDLIGHT)
        ret = session.query(AtvesApprovalByReviewDateDetails)
        assert ret.count() > 3000


def test_atvesdb_process_conduent_data_approval_by_review_date_oh(atvesdb_fixture, conn_str):
    """Testing process_conduent_data_approval_by_review_date"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
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


def test_atvesdb_process_conduent_data_by_location(atvesdb_fixture, conn_str):
    """Testing process_conduent_data_by_location"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture.process_conduent_data_by_location(start_date=date(2010, 11, 1), end_date=date(2010, 11, 7))
        ret = session.query(AtvesByLocation)
        assert ret.count() == 0

        atvesdb_fixture.process_conduent_data_by_location(start_date=date(2020, 11, 1), end_date=date(2020, 11, 7))
        ret = session.query(AtvesByLocation)
        assert ret.count() > 7000


def test_atvesdb_process_traffic_count_data(atvesdb_fixture, conn_str):
    """Testing process_traffic_count_data"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture.process_traffic_count_data(start_date=date(2010, 11, 1), end_date=date(2010, 11, 7))
        ret = session.query(AtvesTrafficCounts)
        assert ret.count() == 0

        atvesdb_fixture.process_traffic_count_data(start_date=date(2020, 11, 1), end_date=date(2020, 11, 7))
        ret = session.query(AtvesTrafficCounts)
        assert ret.count() > 300
