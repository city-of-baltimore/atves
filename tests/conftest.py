"""Pytest directory-specific hook implementations"""
import pytest

import atves


@pytest.fixture(name='conduent_fixture')
def fixture_conduent():
    """Setup for each test"""
    return atves.conduent.Conduent(atves.creds.CONDUENT_USERNAME, atves.creds.CONDUENT_PASSWORD)
