import os
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, func, MetaData, Column, Integer

load_dotenv()


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://sql_conn_user:Password@localhost:5432/sql_conn")

Base = automap_base()
  

class ReportCase(Base):
    __tablename__ = 'report_case'
    id = Column(Integer, primary_key=True)

class RoadLocation(Base):
    __tablename__ = 'road_location'
    id = Column(Integer, primary_key=True)

class Vehicle(Base):
    __tablename__ = 'vehicle'
    id = Column(Integer, primary_key=True)

class DriverPerson(Base):
    __tablename__ = 'driver_person'
    id = Column(Integer, primary_key=True)

class IncidentDetails(Base):
    __tablename__ = 'incident_details'
    id = Column(Integer, primary_key=True)

class FactTable(Base):
    __tablename__ = 'fact_table'
    id = Column(Integer, primary_key=True)
    report_case_id = Column(Integer)
    road_location_id = Column(Integer)
    vehicle_id = Column(Integer)
    driver_person_id = Column(Integer)
    incident_details_id = Column(Integer)
    number_of_crashes = Column(Integer)
    number_of_injured = Column(Integer)
    most_common_crash_type = Column(Integer)
    most_common_weather = Column(Integer)

engine = create_engine(DATABASE_URL)

Base.prepare(autoload_with=engine)
Session = sessionmaker(bind = engine)
session = Session()



metadata = MetaData()
metadata.reflect(bind=engine)

report_case = metadata.tables['report_case']
road_location = metadata.tables['road_location']
vehicle = metadata.tables['vehicle']
driver_person = metadata.tables['driver_person']
incident_details = metadata.tables['incident_details']
fact_table = metadata.tables['fact_table']



def insert_fact_table():
    try:
        number_of_crashes = session.query(func.count(report_case.c["Report Number"])).scalar()

        number_of_injured = session.query(
            func.count().filter(
                (incident_details.c["Injury Severity"].isnot(None)) &
                (incident_details.c["Injury Severity"] != 'NO APPARENT INJURY')
            )
        ).scalar()

        most_common_crash_type = session.query(
            incident_details.c["Collision Type"]
        ).group_by(
            incident_details.c["Collision Type"]
        ).order_by(
            func.count().desc()
        ).limit(1).scalar()

        most_common_weather = session.query(
            incident_details.c["Weather"]
        ).group_by(
            incident_details.c["Weather"]
        ).order_by(
            func.count().desc()
        ).limit(1).scalar()

        insert_stmt = fact_table.insert().values(
            report_case_id=None,
            road_location_id=None,
            vehicle_id=None,
            driver_person_id=None,
            incident_details_id=None,
            number_of_crashes=number_of_crashes,
            number_of_injured=number_of_injured,
            most_common_crash_type=most_common_crash_type,
            most_common_weather=most_common_weather
        )
        session.execute(insert_stmt)
        session.commit()
        print("Data inserted into fact_table")
    except Exception as e:
        print(f"An error occurred: {e}")

insert_fact_table()