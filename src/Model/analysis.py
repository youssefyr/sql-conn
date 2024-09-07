import os
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, func, MetaData, Column, Integer

# Load environment variables from .env file
load_dotenv()

# Read DATABASE_URL from environment variable
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://sql_conn_user:Password@localhost:5432/sql_conn")

# Initialize the base class for automap
Base = automap_base()
  
# Define the database models
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
    """
    Represents the fact_table table in the database.

    Attributes:
        id (int): The primary key of the fact_table.
        report_case_id (int): The report case ID associated with the fact_table.
        road_location_id (int): The road location ID associated with the fact_table.
        vehicle_id (int): The vehicle ID associated with the fact_table.
        driver_person_id (int): The driver person ID associated with the fact_table.
        incident_details_id (int): The incident details ID associated with the fact_table.
        number_of_crashes (int): The number of crashes associated with the fact_table.
        number_of_injured (int): The number of injured associated with the fact_table.
        most_common_crash_type (int): The most common crash type associated with the fact_table.
        most_common_weather (int): The most common weather associated with the fact_table.
    """
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

# Create the database engine
engine = create_engine(DATABASE_URL)

Base.prepare(autoload_with=engine)
Session = sessionmaker(bind = engine)
session = Session()


# Reflect the database tables (get the types from the database)
metadata = MetaData()
metadata.reflect(bind=engine)

# Map the tables to variables
report_case = metadata.tables['report_case']
road_location = metadata.tables['road_location']
vehicle = metadata.tables['vehicle']
driver_person = metadata.tables['driver_person']
incident_details = metadata.tables['incident_details']
fact_table = metadata.tables['fact_table']



def insert_fact_table():
    """
    Inserts data into the fact_table.

    This function retrieves various statistics from the database and inserts them into the fact_table.
    The statistics include the number of crashes, the number of injured individuals, the most common crash type,
    and the most common weather condition.

    Raises:
        Exception: If an error occurs during the insertion process.

    Returns:
        None
    """
    try:
        # Calculate the number of crashes
        number_of_crashes = session.query(func.count(report_case.c["Report Number"])).scalar()
        # Calculate the number of injured individuals
        number_of_injured = session.query(
            func.count().filter(
                (incident_details.c["Injury Severity"].isnot(None)) &
                (incident_details.c["Injury Severity"] != 'NO APPARENT INJURY')
            )
        ).scalar()
        # Find the most common crash type
        most_common_crash_type = session.query(
            incident_details.c["Collision Type"]
        ).group_by(
            incident_details.c["Collision Type"]
        ).order_by(
            func.count().desc()
        ).limit(1).scalar()

        # Find the most common weather condition
        most_common_weather = session.query(
            incident_details.c["Weather"]
        ).group_by(
            incident_details.c["Weather"]
        ).order_by(
            func.count().desc()
        ).limit(1).scalar()

        # Insert the calculated values into the fact_table
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


# Call the function to insert data into the fact_table
insert_fact_table()