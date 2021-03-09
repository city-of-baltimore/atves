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


class AtvesTicketCameras(Base):
    """Table holding the ticket counts for the red light and overheight cameras"""
    __tablename__ = "atves_ticket_cameras"

    id = Column(Integer, primary_key=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    location = Column(String, ForeignKey('atves_cam_locations.locationdescription'), nullable=False)
    officer = Column(String)
    equip_type = Column(String(length=10), primary_key=True)
    issued = Column(Integer, nullable=False)
    rejected = Column(Integer, nullable=False)


class AtvesCamLocations(Base):
    """The camera location database looks like this"""
    __tablename__ = "atves_cam_locations"

    location_code = Column(String(length=100), primary_key=True)
    locationdescription = Column(String(length=100), nullable=False)
    lat = Column(Numeric(precision=6, scale=4), nullable=False)
    long = Column(Numeric(precision=6, scale=4), nullable=False)
    cam_type = Column(String(length=2), nullable=False)
    effective_date = Column(Date)
    speed_limit = Column(Integer)
    status = Column(Boolean)

    TrafficCounts = relationship('AtvesTrafficCounts')
    TicketCameras = relationship('AtvesTicketCameras')
    AmberTimeRejects = relationship('AtvesAmberTimeRejects')
    AtvesByLocation = relationship('AtvesByLocation')


class AtvesAmberTimeRejects(Base):
    """get_amber_time_rejects_report (red light only)"""
    __tablename__ = "atves_amber_time_rejects"

    location_code = Column(String(length=100), ForeignKey('atves_cam_locations.location_code'))
    deployment_no = Column(Integer, nullable=False)
    violation_date = Column(DateTime, nullable=False)
    amber_time = Column(Numeric(precision=5, scale=3), nullable=False)
    amber_reject_code = Column(String(length=100))
    event_number = Column(Integer, primary_key=True)


class AtvesApprovalByReviewDateDetails(Base):
    """get_approval_by_review_date_details (red light only)"""
    __tablename__ = "atves_approval_by_review_date_details"

    disapproved = Column(Integer, nullable=False)
    approved = Column(Integer, nullable=False)
    officer = Column(String(length=100))
    citation_no = Column(String(length=20), primary_key=True)
    violation_date = Column(DateTime)
    review_status = Column(String(length=20))
    review_datetime = Column(DateTime)


class AtvesByLocation(Base):
    """get_client_summary_by_location"""
    __tablename__ = "atves_by_location"

    date = Column(Date, primary_key=True)
    location_code = Column(String(length=100), ForeignKey('atves_cam_locations.location_code'), primary_key=True)
    section = Column(String(length=20))
    details = Column(String(length=100))
    percentage_desc = Column(String(length=50))
    issued = Column(Integer, nullable=False)
    in_process = Column(Integer, nullable=False)
    non_violations = Column(Integer, nullable=False)
    controllable_rejects = Column(Integer, nullable=False)
    uncontrollable_rejects = Column(Integer, nullable=False)
    pending_initial_approval = Column(Integer, nullable=False)
    pending_reject_approval = Column(Integer, nullable=False)
    vcDescription = Column(String(length=100), primary_key=True)
    detail_count = Column(Integer)
    order_by = Column(Integer)


# Reports not yet being imported
# get_approval_summary_by_queue
# get_expired_by_location
# get_in_city_vs_out_of_city
# get_daily_self_test
# get_pending_client_approval
