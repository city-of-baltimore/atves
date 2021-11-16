"""Types used only for type checking in conduent.py"""
# pylint:disable=too-few-public-methods
# pylint:disable=inherit-non-class
# pylint:disable=unused-private-member
from typing import Optional, TypedDict
from datetime import datetime


class CameraType(TypedDict):
    """Camera location data"""
    site_code: Optional[str]
    location: Optional[str]
    jurisdiction: Optional[str]
    date_created: Optional[str]
    created_by: Optional[str]
    effective_date: Optional[str]
    speed_limit: Optional[str]
    status: Optional[str]
    cam_type: Optional[str]


class ConduentResultsType(TypedDict):
    """Return data from Conduent camera search"""
    id: str
    start_time: datetime
    end_time: datetime
    location: str
    officer: str
    equip_type: str
    issued: str
    rejected: str


class SessionStateType(TypedDict):
    """Tracking http state variables in the Conduent website"""
    __VIEWSTATE: Optional[str]
    __VIEWSTATEGENERATOR: Optional[str]
    __EVENTVALIDATION: Optional[str]
