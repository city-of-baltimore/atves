"""Pytest directory-specific hook implementations"""
import pytest

import atves


def pytest_addoption(parser):
    """Pytest custom arguments"""
    parser.addoption('--axsis-user', action='store')
    parser.addoption('--axsis-pass', action='store')
    parser.addoption('--conduent-user', action='store')
    parser.addoption('--conduent-pass', action='store')
    parser.addoption('--geocodio-key', action='store')


@pytest.fixture(name='conduent_fixture')
def fixture_conduent(conduent_username, conduent_password):
    """Setup for each test"""
    return atves.conduent.Conduent(conduent_username, conduent_password)


@pytest.fixture(name='axsis_username')
def fixture_axsis_username(request):
    """The username to login to AXSIS"""
    return request.config.getoption('--axsis-user')


@pytest.fixture(name='axsis_password')
def fixture_axsis_password(request):
    """The password to login to AXSIS"""
    return request.config.getoption('--axsis-pass')


@pytest.fixture(name='conduent_username')
def fixture_conduent_username(request):
    """The username to login to Conduent"""
    return request.config.getoption('--conduent-user')


@pytest.fixture(name='conduent_password')
def fixture_conduent_password(request):
    """The password to login to Conduent"""
    return request.config.getoption('--conduent-pass')


@pytest.fixture(name='geocodio_api')
def fixture_geocodio_key(request):
    """The API key for Geocodio"""
    return request.config.getoption('--geocodio-key')
