"""Test suite for atves_database.py"""
# pylint:disable=protected-access,unused-argument
import sys
import warnings
from datetime import date
from pathlib import Path

import pytest
from loguru import logger
from sqlalchemy import create_engine, exc as sa_exc  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from atves.constants import OVERHEIGHT, REDLIGHT, SPEED
from atves.atves_database import parse_args
from atves.atves_schema import AtvesAmberTimeRejects, AtvesCamLocations, AtvesFinancial, AtvesTrafficCounts, \
    AtvesViolationCategories, AtvesViolations


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
            assert all((-76.73 < i[1] < -76.52 for i in ret.all()))
            assert all((39.2 < i[2] < 39.38 for i in ret.all()))


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
            lats = [-76.73 < i[1] < -76.52 for i in vals if i[1]]
            lngs = [39.2 < i[2] < 39.38 for i in vals if i[2]]
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
                            AtvesCamLocations.long).filter(AtvesCamLocations.cam_type == 'SC')
        assert ret.count() > 10

        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=sa_exc.SAWarning)
            # throw away None results, but make sure its not all of them
            lats = [-76.73 < i[1] < -76.52 for i in ret.all() if i[1]]
            lngs = [39.2 < i[2] < 39.38 for i in ret.all() if i[2]]
        assert all(lats)
        assert len(lats) > 10
        assert all(lngs)
        assert len(lngs) > 10


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
