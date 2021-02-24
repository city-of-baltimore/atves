"""Types used only for type checking in conduent.py"""
# pylint:disable=too-few-public-methods
# pylint:disable=inherit-non-class
from typing import Optional, TypedDict
from datetime import datetime


class CameraType(TypedDict):
    """Structure used for camera location data"""
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
    id: str
    start_time: datetime
    end_time: datetime
    location: str
    officer: str
    equip_type: str
    issued: str
    rejected: str
