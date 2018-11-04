import numpy as np
import pandas as pd
import datetime as dt
from flask import Flask, jsonify


#################################################
# Flask Setup
#################################################
app = Flask(__name__)

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)
#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    return (
        f"Welcome to the Climate API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date<br/>"
        f"/api/v1.0/start_date/end_date"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    prcp_dict_list = []
    sel = [Measurement.date,Measurement.prcp]
    df_prcp = session.query(*sel).all()
    for x in df_prcp:
        prcp_dict_list.append({"date":x[0],
                               "prcp":x[1]})

    return jsonify(prcp_dict_list)

@app.route("/api/v1.0/stations")
def stations():
    station_dict_list = []

    df_station = session.query(Measurement.station).distinct().all()

    for x in df_station:
        station_dict_list.append({"station":x[0]})

    return jsonify(station_dict_list)


@app.route("/api/v1.0/tobs")
def temps():
    temp_dict_list = []

    max_date = engine.execute("SELECT max(date) FROM measurement").fetchall()[0][0]
    past_date = engine.execute("SELECT date(max(date),'-12 months') FROM measurement").fetchall()[0][0]
    sel = [Measurement.station,
           Measurement.tobs,
           Measurement.date]
    df_temp = session.query(*sel).\
        filter(Measurement.date >= past_date).\
        filter(Measurement.date <= max_date).all()

    for x in df_temp:
        temp_dict_list.append({"station":x[0],
                               "temps":x[1],
                               "date":x[2]})

    return jsonify(temp_dict_list)


@app.route("/api/v1.0/<start>")
def date_start(start):
    agg_dict_list = []

    sel = [Measurement.station,
       func.min(Measurement.tobs),
       func.max(Measurement.tobs),
       func.avg(Measurement.tobs),
       func.count(Measurement.tobs),
       Measurement.date]
    df_agg = session.query(*sel).\
            filter(sel[5] > start).\
            group_by(Measurement.station, Measurement.date).\
            order_by(sel[4].desc()).all()

    for x in df_agg:
        agg_dict_list.append({"station":x[0],
                              "min":x[1],
                              "max":x[2],
                              "avg":x[3],
                              "count":x[4],
                              "date":x[5]})

    if not agg_dict_list:
        return jsonify({"error": f"Date out of Range"}), 404
    else:
        return jsonify(agg_dict_list)

@app.route("/api/v1.0/<start>/<end>")
def date_start_end(start, end):
    agg_dict_list = []

    sel = [Measurement.station,
       func.min(Measurement.tobs),
       func.max(Measurement.tobs),
       func.avg(Measurement.tobs),
       func.count(Measurement.tobs),
       Measurement.date]
    df_agg = session.query(*sel).\
            filter(sel[5] >= start).\
            filter(sel[5] <= end).\
            group_by(Measurement.station, Measurement.date).\
            order_by(sel[4].desc()).all()

    for x in df_agg:
        agg_dict_list.append({"station":x[0],
                              "min":x[1],
                              "max":x[2],
                              "avg":x[3],
                              "count":x[4],
                              "date":x[5]})

    if not agg_dict_list:
        return jsonify({"error": f"Date out of Range"}), 404
    else:
        return jsonify(agg_dict_list)


if __name__ == "__main__":
    app.run(debug=True)
