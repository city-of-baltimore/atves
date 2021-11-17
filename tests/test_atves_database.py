"""Test suite for atves_database.py"""
# pylint:disable=protected-access,unused-argument
import sys
import warnings
from datetime import date
from pathlib import Path

import pytest
from loguru import logger
from pandas import to_datetime  # type: ignore
from sqlalchemy import create_engine, exc as sa_exc  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from atves.constants import OVERHEIGHT, REDLIGHT, SPEED
from atves.atves_database import parse_args
from atves.atves_schema import AtvesAmberTimeRejects, AtvesCamLocations, AtvesFinancial, AtvesRejectReason, \
    AtvesTrafficCounts, AtvesViolationCategories, AtvesViolations


def test_atvesdb_build_db_conduent_red_light(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str, reset_database):
    """Testing _build_db_conduent_red_light"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        # atvesdb_fixture.build_location_db() is called in the setup
        ret = session.query(AtvesCamLocations.cam_type,
                            AtvesCamLocations.lat,
                            AtvesCamLocations.long).filter(AtvesCamLocations.cam_type == 'RL')
        assert ret.count() > 100
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=sa_exc.SAWarning)
            assert all((39.2 < i[1] < 39.38 for i in ret.all()))
            assert all((-76.73 < i[2] < -76.52 for i in ret.all()))


def test_atvesdb_build_db_conduent_overheight(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str, reset_database):
    """Testing _build_db_conduent_overheight"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        # atvesdb_fixture.build_location_db() is called in the setup
        ret = session.query(AtvesCamLocations.cam_type,
                            AtvesCamLocations.lat,
                            AtvesCamLocations.long,
                            AtvesCamLocations.locationdescription,
                            AtvesCamLocations.speed_limit,
                            AtvesCamLocations.effective_date).filter(AtvesCamLocations.cam_type == 'OH')
        assert ret.count() > 10

        # throw away None results, but make sure its not all of them
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=sa_exc.SAWarning)
            vals = ret.all()
            lats = [39.2 < i[1] < 39.38 for i in vals if i[1]]
            lngs = [-76.73 < i[2] < -76.52 for i in vals if i[2]]
            speed_limits = [i for i in vals if i[4]]
            effective_dates = [i for i in vals if i[5]]
            assert all(lats)
            assert len(lats) > 8
            assert all(lngs)
            assert len(lngs) > 8
            assert len(speed_limits) > 8
            assert len(effective_dates) > 8


def test_atvesdb_build_db_speed_cameras(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str, reset_database):
    """Testing _build_db_speed_cameras"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        # atvesdb_fixture.build_location_db() is called in the setup
        ret = session.query(AtvesCamLocations.cam_type,
                            AtvesCamLocations.lat,
                            AtvesCamLocations.long,
                            AtvesCamLocations.effective_date).filter(AtvesCamLocations.cam_type == 'SC')
        assert ret.count() > 10

        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=sa_exc.SAWarning)
            # throw away None results, but make sure its not all of them
            lats = [39.2 < i[1] < 39.38 for i in ret.all() if i[1]]
            lngs = [-76.73 < i[2] < -76.52 for i in ret.all() if i[2]]
        assert all(lats)
        assert len(lats) > 10
        assert all(lngs)
        assert len(lngs) > 10

        ret = session.query(AtvesCamLocations.effective_date).filter(AtvesCamLocations.location_code == 'BAL100')
        assert not ret.all()[0][0]

        # Test the logic to use the violations to get the start date
        session.add_all([
            AtvesViolationCategories(
                violation_cat=1,
                description=' '),
            AtvesViolations(
                date=to_datetime('2020-01-01 00:00:00.000'),
                location_code='BAL100',
                count=0,
                violation_cat=1,
                details='Citations Issued'),
            AtvesViolations(
                date=to_datetime('2020-01-02 00:00:00.000'),
                location_code='BAL100',
                count=0,
                violation_cat=1,
                details='Citations Issued'),
            AtvesViolations(
                date=to_datetime('2020-01-03 00:00:00.000'),
                location_code='BAL100',
                count=0,
                violation_cat=1,
                details='Citations Issued')
        ])
        session.commit()

        atvesdb_fixture.build_location_db(True)
        ret = session.query(AtvesCamLocations.effective_date).filter(AtvesCamLocations.location_code == 'BAL100')
        assert ret.all()[0][0] == date(2020, 1, 1)

        ret = session.query(AtvesCamLocations.effective_date).filter(AtvesCamLocations.location_code == 'BAL101')
        assert not ret.all()[0][0]

        # Test the logic to use traffic counts to determine the start date
        session.add_all([
            AtvesTrafficCounts(
                location_code='BAL102',
                date=to_datetime('2020-02-01 00:00:00.000'),
                count=500
            )
        ])
        session.commit()

        atvesdb_fixture.build_location_db(True)
        ret = session.query(AtvesCamLocations.effective_date).filter(AtvesCamLocations.location_code == 'BAL102')
        assert ret.all()[0][0] == date(2020, 2, 1)

        ret = session.query(AtvesCamLocations.effective_date).filter(AtvesCamLocations.location_code == 'BAL103')
        assert not ret.all()[0][0]


def test_atvesdb_process_conduent_data_amber_time(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str, reset_database):
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
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=sa_exc.SAWarning)
            assert len([x.violation_date.date()
                        for x in ret
                        if x.violation_date.date() > date(2020, 11, 3) or
                        x.violation_date.date() < date(2020, 11, 1)]) == 0
        assert ret.count() > 30


def test_atvesdb_process_traffic_count_data(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str, reset_database):
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
        assert len([x.date for x in ret if x.date > date(2020, 11, 3) or x.date < date(2020, 11, 1)]) == 0
        assert ret.count() > 100


def test_atvesdb_process_violations(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str, reset_database):
    """Testing process_violations"""
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_violations(start_date=date(2021, 6, 1), end_date=date(2021, 6, 3))
        ret = session.query(AtvesViolations)
        assert ret.count() == 0

        atvesdb_fixture.process_violations(start_date=date(2021, 6, 1), end_date=date(2021, 6, 3))
        ret = session.query(AtvesViolationCategories)
        assert ret.count() == 6

        ret = session.query(AtvesViolations)
        assert len([x.date for x in ret if x.date > date(2021, 6, 3) or x.date < date(2021, 6, 1)]) == 0
        assert ret.count() > 3000


@pytest.mark.skip
def test_atvesdb_process_financials_overheight(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str, reset_database):
    """
    Test process_financials with OVERHEIGHT

    The overheight account number is wrong, so we are skipping this for now
    """
    start_date = date(2021, 2, 1)
    end_date = date(2021, 2, 28)
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_financials(start_date=start_date, end_date=end_date, cam_type=OVERHEIGHT)
        ret = session.query(AtvesFinancial)
        assert ret.count() == 0

        atvesdb_fixture.process_financials(start_date=start_date, end_date=end_date, cam_type=OVERHEIGHT)
        ret = session.query(AtvesFinancial)
        assert len([x.ledger_posting_date
                    for x in ret
                    if x.ledger_posting_date > end_date or x.ledger_posting_date < start_date]) == 0
        assert ret.count() > 10


@pytest.mark.vpn
def test_atvesdb_process_financials_redlight(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str, reset_database):
    """Test process_financials with REDLIGHT"""
    start_date = date(2021, 2, 1)
    end_date = date(2021, 2, 28)
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_financials(start_date=start_date, end_date=end_date, cam_type=REDLIGHT)
        ret = session.query(AtvesFinancial)
        assert ret.count() == 0

        atvesdb_fixture.process_financials(start_date=start_date, end_date=end_date, cam_type=REDLIGHT)
        ret = session.query(AtvesFinancial)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=sa_exc.SAWarning)
            assert len([x.ledger_posting_date
                        for x in ret
                        if x.ledger_posting_date > end_date or x.ledger_posting_date < start_date]) == 0
        assert ret.count() > 10


@pytest.mark.vpn
def test_atvesdb_process_financials_speed(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str, reset_database):
    """Test process_financials with SPEED"""
    start_date = date(2021, 2, 1)
    end_date = date(2021, 2, 28)
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_financials(start_date=start_date, end_date=end_date, cam_type=SPEED)
        ret = session.query(AtvesFinancial)
        assert ret.count() == 0

        atvesdb_fixture.process_financials(start_date=start_date, end_date=end_date, cam_type=SPEED)
        ret = session.query(AtvesFinancial)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=sa_exc.SAWarning)
            assert len([x.ledger_posting_date
                        for x in ret
                        if x.ledger_posting_date > end_date or x.ledger_posting_date < start_date]) == 0
        assert ret.count() > 10


def test_process_officer_actions(atvesdb_fixture, atvesdb_fixture_no_creds, conn_str, reset_database):
    """Test process_officer_actions"""
    start_date = date(2021, 11, 5)
    end_date = date(2021, 11, 8)
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        atvesdb_fixture_no_creds.process_officer_actions(start_date=start_date, end_date=end_date)
        ret = session.query(AtvesRejectReason)
        assert ret.count() == 0

        atvesdb_fixture.process_officer_actions(start_date=start_date, end_date=end_date)
        ret = session.query(AtvesRejectReason)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=sa_exc.SAWarning)
            assert len([x.date
                        for x in ret
                        if x.date > end_date or x.date < start_date]) == 0
            # There is missing data on the 6th and 7th
            assert len(set([x.date
                           for x in ret
                           if x.date <= end_date or x.date >= start_date])) == 2
        assert ret.count() > 10


def test_get_lat_long(atvesdb_fixture):
    """Test get_lat_long"""
    lat, lng = atvesdb_fixture.get_lat_long('4000 blk Pulaski Hwy WB')
    assert lat and lng


def setup_logging(debug=False, info=False, path: Path = None):
    """
    Configures the logging level, and sets up file based logging. By default, the following logging levels are enabled:
    fatal, error, and warn. This optionally enables info and debug.

    :param debug: If true, the Debug logging level is used, and verbose is ignored
    :param info: If true and debug is false, then the info log level is used
    :param path: Base path where the logs folder should go. If not specified, then it uses the current dir
    """
    # Setup logging
    log_level = 'WARNING'
    if debug:
        log_level = 'DEBUG'
    elif info:
        log_level = 'INFO'

    if path:
        log_path = path / 'logs' / 'file-{time}.log'
    else:
        log_path = Path('logs') / 'file-{time}.log'

    handlers = [
        {'sink': sys.stdout, 'format': '{time} - {message}', 'colorize': True, 'backtrace': True, 'diagnose': True,
         'level': log_level},
        {'sink': log_path, 'serialize': True, 'backtrace': True,
         'diagnose': True, 'rotation': '1 week', 'retention': '3 months', 'compression': 'zip', 'level': log_level},
    ]

    logger.configure(handlers=handlers)


def test_parse_args():
    """Test parse_args"""
    conn_str = 'conn_str'
    start_date_str = '2021-07-20'
    start_date = date(2021, 7, 20)
    end_date_str = '2021-07-21'
    end_date = date(2021, 7, 21)
    args = parse_args(['-v', '-c', conn_str, '-s', start_date_str, '-e', end_date_str, '-b', '-f'])
    assert args.verbose
    assert not args.debug
    assert args.conn_str == conn_str
    assert args.startdate == start_date
    assert args.enddate == end_date
    assert args.builddb
    assert args.force
