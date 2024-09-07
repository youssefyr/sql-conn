import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://sql_conn_user:Password@localhost:5432/sql_conn")


basepath = os.path.dirname(__file__)
default_path = os.path.abspath(os.path.join(basepath, '../Extract/loaded'))


Base = declarative_base()


engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

# Read the CSV files
df_report_case = pd.read_csv(default_path + '/report_case.csv')
df_road_location = pd.read_csv(default_path + '/road_location.csv')
df_vehicle = pd.read_csv(default_path + '/vehicle.csv')
df_driver_person = pd.read_csv(default_path + '/driver_person.txt', sep=',')
df_incident_details = pd.read_csv(default_path + '/incident_details.txt', sep=',')

with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS fact_table CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS report_case CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS road_location CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS vehicle CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS driver_person CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS incident_details CASCADE;"))



with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS fact_table CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS report_case CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS road_location CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS vehicle CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS driver_person CASCADE;"))
    conn.execute(text("DROP TABLE IF EXISTS incident_details CASCADE;"))

df_report_case.to_sql('report_case', engine, if_exists='replace', index=False)
df_road_location.to_sql('road_location', engine, if_exists='replace', index=False)
df_vehicle.to_sql('vehicle', engine, if_exists='replace', index=False)
df_driver_person.to_sql('driver_person', engine, if_exists='replace', index=False)
df_incident_details.to_sql('incident_details', engine, if_exists='replace', index=False)


fact_table = pd.DataFrame({
    'report_case_id': [None],
    'road_location_id': [None],
    'vehicle_id': [None],
    'driver_person_id': [None],
    'incident_details_id': [None],
    'number_of_crashes': [None],
    'number_of_injured': [None],
    'most_common_crash_type': [None],
    'most_common_weather': [None]
})

fact_table.to_sql('fact_table', engine, if_exists='replace', index=False)

session.commit()


fact_table = pd.DataFrame({
    'report_case_id': [None],
    'road_location_id': [None],
    'vehicle_id': [None],
    'driver_person_id': [None],
    'incident_details_id': [None],
    'number_of_crashes': [None],
    'number_of_injured': [None],
    'most_common_crash_type': [None],
    'most_common_weather': [None]
})

fact_table.to_sql('fact_table', engine, if_exists='replace', index=False)


session.commit()