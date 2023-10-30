# Import the dependencies.
from sqlalchemy import create_engine, text, and_
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from sqlalchemy.ext.automap import automap_base

#################################################
# Database Setup
#################################################


# reflect an existing database into a new model

engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# reflect the tables
# Use automap_base() to reflect the tables into classes
Base = automap_base()

# Reflect the tables from the database into the Base
Base.prepare(engine, reflect=True)


#Save references to the 'station' and 'measurement' classes
Station = Base.classes.station
Measurement = Base.classes.measurement

# Create our session (link) from Python to the DB
session = Session(engine)


#################################################33
# Defining Functions
#################################################

# Function to get the last 12 months of precipitation data for all stations
def get_precipitation_data():
    # Calculate the most recent date in the data
    most_recent_date_query = text("""
        SELECT MAX(date) AS max_date
        FROM measurement
    """)
    with Session(engine) as session:
        most_recent_date_result = session.execute(most_recent_date_query)
        most_recent_date_str = most_recent_date_result.scalar()
        most_recent_date = datetime.strptime(most_recent_date_str, '%Y-%m-%d')

    # Calculate the date 12 months ago from the most recent date
    start_date = most_recent_date - timedelta(days=365)

    # Fetch precipitation data for all stations within the date range
    with Session(engine) as session:
        precipitation_query = text("""
            SELECT station, date, prcp
            FROM measurement
            WHERE date >= :start_date
            AND date <= :end_date
        """)
        precipitation_query = precipitation_query.bindparams(start_date=start_date, end_date=most_recent_date)
        precipitation_result = session.execute(precipitation_query)
        precipitation_data = {}
        for row in precipitation_result:
            station_date = row.date #row.station + '-' + row.date
            precipitation_data[station_date] = row.prcp

    return precipitation_data

# Function to fetch the list of stations from the dataset
def get_stations():
    with Session(engine) as session:
        stations_query = text("""
            SELECT DISTINCT station
            FROM measurement
        """)
        stations_result = session.execute(stations_query)
        stations = [row.station for row in stations_result]
    return stations

# Function to fetch the most active station ID
def get_most_active_station():
    with Session(engine) as session:
        most_active_station_query = text("""
            SELECT station, COUNT(station) AS station_count
            FROM measurement
            GROUP BY station
            ORDER BY station_count DESC
        """)
        most_active_station_result = session.execute(most_active_station_query)
        most_active_station_id = most_active_station_result.first().station
    return most_active_station_id



def get_temperature_observations(station_id):
    # Calculate the date 12 months ago from the most recent date
    with Session(engine) as session:
        most_recent_date_query = text("""
            SELECT MAX(date) AS max_date
            FROM measurement
            WHERE station = :station_id
        """)
        most_recent_date_query = most_recent_date_query.bindparams(station_id=station_id)
        most_recent_date_result = session.execute(most_recent_date_query)
        most_recent_date_str = most_recent_date_result.scalar()
        most_recent_date = datetime.strptime(most_recent_date_str, '%Y-%m-%d')
        start_date = most_recent_date - timedelta(days=365)

    # Fetch temperature observations for the most active station within the date range
    with Session(engine) as session:
        temperature_query = text("""
            SELECT date, tobs
            FROM measurement
            WHERE station = :station_id
            AND date >= :start_date
            AND date <= :end_date
        """)
        temperature_query = temperature_query.bindparams(station_id=station_id, start_date=start_date, end_date=most_recent_date)
        temperature_result = session.execute(temperature_query)
        temperature_observations = [{"date": row.date, "tobs": row.tobs} for row in temperature_result]
    return temperature_observations


# Function to calculate TMIN, TAVG, and TMAX for a specified date range
def calculate_temperature_stats(start_date, end_date=None):
    with Session(engine) as session:
        # Create the base query to calculate temperature statistics
        base_query = text("""
            SELECT MIN(tobs) AS TMIN, AVG(tobs) AS TAVG, MAX(tobs) AS TMAX
            FROM measurement
            WHERE date >= :start_date
        """)
        
        if end_date:
            # If end_date is specified, update the query to include an end date filter
            base_query = text("""
                SELECT MIN(tobs) AS TMIN, AVG(tobs) AS TAVG, MAX(tobs) AS TMAX
                FROM measurement
                WHERE date >= :start_date AND date <= :end_date
            """)
            base_query = base_query.bindparams(start_date=start_date,end_date=end_date)
            temperature_stats = session.execute(base_query).first()
        else:
            base_query = base_query.bindparams(start_date=start_date)
            temperature_stats = session.execute(base_query).first()

    return temperature_stats

#################################################
# Flask Setup
#################################################

from flask import Flask, jsonify

app = Flask(__name__)


# Homepage
@app.route("/")
def home(): 
    return(
        f"Welcome to My Climate API homepage!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start-date<br/>"
        f"/api/v1.0/start-date/end-date<br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Add code to retrieve and return precipitation data
    precipitation_data = get_precipitation_data()
    return jsonify(precipitation_data)

# Route to get the list of stations
@app.route("/api/v1.0/stations")
def stations():
    # Add code to retrieve and return the list of stations
    station_list = get_stations()
    return jsonify({"stations": station_list})

# Route to get temperature observations for the most active station for the previous year
@app.route("/api/v1.0/tobs")
def temperature_observations():
    # Add code to retrieve and return temperature observations
    most_active_station = get_most_active_station()
    temperature_observations = get_temperature_observations(most_active_station)
    return jsonify({"temperature_observations": temperature_observations})

# Route to calculate TMIN, TAVG, and TMAX for a specified start date
@app.route("/api/v1.0/<start>")
def temperature_stats_start(start):
    start_date = datetime.strptime(start, '%Y-%m-%d')
    stats = calculate_temperature_stats(start_date)
    return jsonify({"TMIN": stats.TMIN, "TAVG": stats.TAVG, "TMAX": stats.TMAX})

# Route to calculate TMIN, TAVG, and TMAX for a specified start and end date range
@app.route("/api/v1.0/<start>/<end>")
def temperature_stats_start_end(start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.strptime(end, '%Y-%m-%d')
    stats = calculate_temperature_stats(start_date, end_date)
    return jsonify({"TMIN": stats.TMIN, "TAVG": stats.TAVG, "TMAX": stats.TMAX})


if __name__ == "__main__":
    app.run(debug=True)

#################################################
# Flask Routes
#################################################
