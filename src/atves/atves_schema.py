"""Models used by sqlalchemy"""
# pylint:disable=too-few-public-methods
from sqlalchemy import Column, ForeignKey  # type: ignore
from sqlalchemy.orm import relationship  # type: ignore
from sqlalchemy.ext.declarative import DeclarativeMeta  # type: ignore
from sqlalchemy.ext.declarative import declarative_base  # type: ignore
from sqlalchemy.types import Boolean, Date, DateTime, Integer, Numeric, String  # type: ignore

Base: DeclarativeMeta = declarative_base()


class AtvesTrafficCounts(Base):
    """Table holding the traffic counts from the speed cameras"""
    __tablename__ = "atves_traffic_counts"

    location_code = Column(String(length=100), ForeignKey('atves_cam_locations.location_code'), primary_key=True)
    date = Column(Date, primary_key=True)
    count = Column(Integer)


class AtvesCamLocations(Base):
    """The camera location database looks like this"""
    __tablename__ = "atves_cam_locations"

    location_code = Column(String(length=100), primary_key=True)
    locationdescription = Column(String(length=100), nullable=False)
    lat = Column(Numeric(precision=6, scale=4))
    long = Column(Numeric(precision=6, scale=4))
    cam_type = Column(String(length=2), nullable=False)
    effective_date = Column(Date)
    speed_limit = Column(Integer)
    status = Column(Boolean)

    TrafficCounts = relationship('AtvesTrafficCounts')
    AmberTimeRejects = relationship('AtvesAmberTimeRejects')


class AtvesAmberTimeRejects(Base):
    """get_amber_time_rejects_report (red light only)"""
    __tablename__ = "atves_amber_time_rejects"

    location_code = Column(String(length=100), ForeignKey('atves_cam_locations.location_code'))
    deployment_no = Column(Integer, nullable=False)
    violation_date = Column(DateTime, nullable=False)
    amber_time = Column(Numeric(precision=5, scale=3), nullable=False)
    amber_reject_code = Column(String(length=100))
    event_number = Column(Integer, primary_key=True)


class AtvesViolations(Base):
    """Violation counts"""
    __tablename__ = "atves_violations"

    date = Column(Date)
    location_code = Column(String(length=100), primary_key=True)
    count = Column(Integer)
    violation_cat = Column(Integer, ForeignKey('atves_violation_categories.violation_cat'), primary_key=True)
    details = Column(String(length=100), primary_key=True)


class AtvesViolationCategories(Base):
    """Lookup table for the violation_cat column in AtvesViolations"""
    __tablename__ = "atves_violation_categories"

    violation_cat = Column(Integer, primary_key=True)
    description = Column(String(length=100))

    AtvesViolations = relationship('AtvesViolations')


class AtvesFinancial(Base):
    """General ledger detail reports"""
    __tablename__ = 'atves_financial'

    journal_entry_no = Column(String(length=30), primary_key=True)
    ledger_posting_date = Column(Date, nullable=False)
    account_no = Column(String(length=50))
    legacy_account_no = Column(String(length=50))
    amount = Column(Numeric(precision=12, scale=2))
    source_journal = Column(String(length=50))
    trx_reference = Column(String(length=50))
    TrxDescription = Column(String(length=255))
    user_who_posted = Column(String(length=50))
    trx_no = Column(String(length=50))
    vendorid_or_customerid = Column(String(length=50))
    vendor_or_customer_name = Column(String(length=100))
    document_no = Column(String(length=50))
    Trx_source = Column(String(length=50))
    account_description = Column(String(length=255))
    account_type = Column(String(length=50))
    agency_or_category = Column(String(length=50))

# Reports not yet being imported
# get_approval_summary_by_queue
# get_expired_by_location
# get_in_city_vs_out_of_city
# get_daily_self_test
# get_pending_client_approval
