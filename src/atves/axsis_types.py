"""Data types used for typing in axsis.py"""
# pylint:disable=too-few-public-methods
# pylint:disable=inherit-non-class
from typing import List, Optional, TypedDict


class ParameterType(TypedDict):
    """Parameter value in ReportsDetailType"""
    ParmDataType: str
    ParmDescription: str
    ParmId: int
    ParmName: str
    ParmOrder: int
    ParmTitle: str
    ParmValue: str
    ReportId: int
    SystemId: int
    ParmList: Optional[List]


class DefinitionType(TypedDict):
    """Definition value in ReportsDetailType"""
    DisabledYn: bool
    EndDate: str
    ReportDescription: str
    ReportFile: str
    ReportId: int
    ReportName: str
    ReportOrder: int
    ReportVersion: int
    StartDate: str
    SystemId: int
    ClientId: int
    ClientCode: str
    User: str


class ReportsDetailType(TypedDict):
    """Return from the ReportsDetail request"""
    Parameters: List[ParameterType]
    Definition: List[DefinitionType]
    Message: str
