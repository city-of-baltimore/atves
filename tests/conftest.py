"""Pytest directory-specific hook implementations"""
import pytest
from pandas import to_datetime  # type: ignore
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

import atves
from atves.atves_schema import AtvesAmberTimeRejects, AtvesFinancial, AtvesTrafficCounts, AtvesViolations, \
    AtvesViolationCategories, Base


def pytest_addoption(parser):
    """Pytest custom arguments"""
    parser.addoption('--axsis-user', action='store')
    parser.addoption('--axsis-pass', action='store')
    parser.addoption('--conduent-user', action='store')
    parser.addoption('--conduent-pass', action='store')
    parser.addoption('--report-user', action='store')
    parser.addoption('--report-pass', action='store')


@pytest.fixture(scope='session', name='axsis_username')
def fixture_axsis_username(request):
    """The username to login to AXSIS"""
    return request.config.getoption('--axsis-user')


@pytest.fixture(scope='session', name='axsis_password')
def fixture_axsis_password(request):
    """The password to login to AXSIS"""
    return request.config.getoption('--axsis-pass')


@pytest.fixture(scope='session', name='conduent_username')
def fixture_conduent_username(request):
    """The username to login to Conduent"""
    return request.config.getoption('--conduent-user')


@pytest.fixture(scope='session', name='conduent_password')
def fixture_conduent_password(request):
    """The password to login to Conduent"""
    return request.config.getoption('--conduent-pass')


@pytest.fixture(scope='session', name='report_username')
def fixture_report_username(request):
    """The username to login to the financial report server"""
    return request.config.getoption('--report-user')


@pytest.fixture(scope='session', name='report_password')
def fixture_report_password(request):
    """The username to login to the financial report server"""
    return request.config.getoption('--report-pass')


@pytest.fixture(name='conduent_fixture')
def fixture_conduent(conduent_username, conduent_password):
    """Conduent object"""
    return atves.conduent.Conduent(conduent_username, conduent_password)


@pytest.fixture(name='axsis_fixture')
def fixture_axsis(axsis_username, axsis_password):
    """Axsis object"""
    return atves.axsis.Axsis(axsis_username, axsis_password)


@pytest.fixture(name='cobreport_fixture')
def fixture_cobreport(report_username, report_password):
    """CobReport object"""
    return atves.financial.CobReports(report_username, report_password)


@pytest.fixture(scope='session', name='atvesdb_fixture')
def fixture_atvesdb(conn_str, axsis_username, axsis_password,  # pylint:disable=too-many-arguments
                    conduent_username, conduent_password, report_username, report_password):
    """ATVES Database object"""
    ret = atves.atves_database.AtvesDatabase(conn_str, axsis_username, axsis_password, conduent_username,
                                             conduent_password, report_username, report_password)
    ret.build_location_db()
    return ret


@pytest.fixture(name='atvesdb_fixture_no_creds')
def fixture_atvesdb_no_creds(conn_str):
    """ATVES Database object"""
    return atves.atves_database.AtvesDatabase(conn_str, None, None, None, None, None, None)


@pytest.fixture(scope='session', name='conn_str')
def fixture_conn_str(tmp_path_factory):
    """Fixture for the WorksheetMaker class"""
    conn_str = 'sqlite:///{}'.format(str(tmp_path_factory.mktemp("data") / 'atves.db'))
    engine = create_engine(conn_str, echo=True, future=True)
    with engine.begin() as connection:
        Base.metadata.create_all(connection)

    with Session(bind=engine) as session:
        session.add_all([
            AtvesTrafficCounts(location_code='BAL101',
                               date=to_datetime('2020-11-01 00:00:00.000'),
                               count=348),
            AtvesTrafficCounts(location_code='BAL101',
                               date=to_datetime('2020-11-02 00:00:00.000'),
                               count=52),
            AtvesTrafficCounts(location_code='BAL102',
                               date=to_datetime('2020-11-01 00:00:00.000'),
                               count=33),
            ])
    return conn_str


@pytest.fixture(name='reset_database')
def fixture_reset_database(conn_str):
    """
    Resets the database, other than the camera locations. This gives us a clean DB without regenerating the camera
    locations, which is a 2 minute process on each test
    """
    engine = create_engine(conn_str, echo=True, future=True)
    with Session(bind=engine) as session:
        session.query(AtvesTrafficCounts).delete(synchronize_session=False)
        session.query(AtvesAmberTimeRejects).delete(synchronize_session=False)
        session.query(AtvesViolations).delete(synchronize_session=False)
        session.query(AtvesViolationCategories).delete(synchronize_session=False)
        session.query(AtvesFinancial).delete(synchronize_session=False)
        session.commit()
