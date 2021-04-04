# dependencies imports
import requests
import json
import numpy as np
import random
import string
import json
from flask import Flask
from flask import request,jsonify
from flask_cors import CORS, cross_origin
import math
import pyodbc
from timeloop import Timeloop
from datetime import timedelta
import csv
import requests
import cv2
import io
import os
from google.cloud import vision
from google.cloud import bigquery
import openai

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service.json"

# Construct a BigQuery client object.
client = bigquery.Client()

tl = Timeloop()

app = Flask(__name__)
cors = CORS(app, resources={
                r"/createReport": {"origins": "*"},
                r"getReportInfo": {"Origins": "*"},
                r"getPollutionData": {"Origins": "*"},
                r"getHurricaneData": {"Origins": "*"},
                r"getLightningData": {"Origins": "*"},
                r"getSeismicData": {"Origins:": "*"},
                r"getTemperatureData": {"Origins": "*"},
                r"uploadImageLabel": {"Origins": "*"},
                })
app.config['CORS_HEADERS'] = 'Content-Type'



# helper function for generating random hashes
def random_string(length):
    pool = string.ascii_uppercase + string.digits
    return ''.join(random.choice(pool) for i in range(length))


def random_digits(length):
    pool = string.digits
    return ''.join(random.choice(pool) for i in range(length))


def random_string_lower_case(length):
    pool = string.ascii_lowercase + string.digits
    return ''.join(random.choice(pool) for i in range(length))

'''
helper function for calculating the distance between two geocoordinates
'''
def calc_distance(user_lat, user_lon, target_lat, target_lon):
    r = 6371e3
    user_lat_radian = user_lat * math.pi / 180
    target_lat_radian = target_lat * math.pi / 180
    d_lat = (target_lat - user_lat) * math.pi / 180
    d_lon = (target_lon - user_lon) * math.pi / 180

    a = (math.sin(d_lat / 2))**2 + math.cos(user_lat_radian) * math.cos(target_lat_radian) * math.sin(d_lon / 2) * math.sin(d_lon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return r * c


def getAnswer(key1, key2, key3):
    
    
    openai.api_key = "YOUR OPEN_AI KEY"
    start_sequence = "\nAI:"
    restart_sequence = "\nHuman: "

    response = openai.Completion.create(
        engine="davinci",
        prompt="The following is a conversation with an AI assistant. \
            The assistant is helpful, creative, clever, and very friendly.\
                \n\nHuman: Hello, who are you?\nAI: I am an AI created by OpenAI. \
                How can I help you today?\nHuman: there are computer and laptop in front of me, generate a report \
                    \nAI: There is a laptop in front of me as well. Here is a report.\nHuman: generate a report including computer and laptop \
                    \nAI: Shall I include my computer and MacBook Pro laptop in the working space report?\nHuman: generate a short story with computer and laptop screen \
                    \nAI: Should I include a short story featuring computer and laptop in the report? \
                    \nHuman: computer, laptop screen, person \
                    \nAI: In the report, there is a laptop, stretched over the whole middle of the desk.  \
                    A man with glasses and a white coat stands at the computer. He is looking at the screen. \
                    \nHuman: %s, %s, %s\nAI:" % (key1, key2, key3),
        temperature=0.9,
        max_tokens=150,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0.6,
        stop=["\n", " Human:", " AI:"]
    )


    return response['choices'][0]['text']


def createReport(brief, latitude, longitude):
    # randomly generate customer ID during signup
    report_id = random_string_lower_case(64)

    drivers = [item for item in pyodbc.drivers()]
    driver = drivers[-1]
    print("driver:{}".format(driver))

    server = 'us-gp-db-server.database.windows.net'
    database = 'usw-db-gp'
    username = 'sql-db-admin'
    password = 'Pwned_2023'
    driver = '{ODBC Driver 17 for SQL Server}'

    with pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=' + server + ';\
                PORT=1433;DATABASE=' + database + ';\
                    UID=' + username + ';\
                        PWD=' + password) as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO [dbo].[Reports] \
                 (report_id, brief, latitude, longitude) \
                     VALUES ('%s', '%s', %.4f, %.4f);" % (report_id, brief, latitude, longitude))
            conn.commit()


@app.route('/')
def index():
    return 'invalid call'

'''
function for handling order placing request
'''
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
@app.route('/getReportInfo', methods=[ 'GET'])
def getReportInfo():
    drivers = [item for item in pyodbc.drivers()]
    driver = drivers[-1]
    print("driver:{}".format(driver))

    server = 'us-gp-db-server.database.windows.net'
    database = 'usw-db-gp'
    username = 'sql-db-admin'
    password = 'Pwned_2023'
    driver = '{ODBC Driver 17 for SQL Server}'

    report_list = []


    with pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=' + server + ';\
                PORT=1433;DATABASE=' + database + ';\
                    UID=' + username + ';\
                        PWD=' + password) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM [dbo].[Reports]")
            row = cursor.fetchone()
            while row is not None:

                report_list.append({
                    'id':row[0],
                    'brief': row[1],
                    'create_date': row[2],
                    'latitude': float(row[3]),
                    'longitude': float(row[4])
                })
                row = cursor.fetchone()

    return jsonify({'report_info': report_list, 'success': True})



# getting polution data for all US cities or cities of interest
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
@app.route('/getPollutionData', methods=[ 'GET'])
def getPollutionData():

    pollutionInfo = []
    num_cities = request.args.get('number')


    query = """
        SELECT
        location, city, pollutant, value, latitude, longitude, state
        FROM
        `bigquery-public-data.openaq.global_air_quality` AS air_quality,
        `bigquery-public-data.geo_us_boundaries.states` AS us_states
        WHERE
        air_quality.country = 'US'
        AND ST_WITHIN(ST_GEOGPOINT(air_quality.longitude,
            air_quality.latitude),
            us_states.state_geom)
        ORDER BY air_quality.value DESC LIMIT %s;
    """ % num_cities
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        # res = "city={}, pollution level={}".format(row[1], row[4])
        res = {
            "city": row[1],
            "pollution value": row[4]
        }
        pollutionInfo.append(res)
        print(res)

    return jsonify({'pollution_info': pollutionInfo, 'success': True})

# getting hurricane data for all US cities or cities of interest
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
@app.route('/getHurricaneData', methods=[ 'GET'])
def getHurricaneData():

    hurricane_info = []
    num_cities = request.args.get('number')


    query = """
        SELECT
        state, state_name, int_point_lat, int_point_lon, wmo_wind, dist2land
        FROM
        `bigquery-public-data.geo_us_boundaries.states` AS us_states,
        `bigquery-public-data.noaa_hurricanes.hurricanes` AS hurricanes
        WHERE
        ST_WITHIN(ST_GEOGPOINT(hurricanes.longitude,
            hurricanes.latitude),
            us_states.state_geom)
        ORDER BY hurricanes.wmo_wind DESC LIMIT %s;
    """ % num_cities
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        # res = "city={}, pollution level={}".format(row[1], row[4])
        res = {
            "city": row[1],
            "pollution value": row[4]
        }
        hurricane_info.append(res)

    return jsonify({'hurricane_info': hurricane_info, 'success': True})


# getting hurricane data for all US cities or cities of interest
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
@app.route('/getLightningData', methods=[ 'GET'])
def getLightningData():

    lightning_info = []
    num_cities = request.args.get('number')


    query = """
        SELECT
        state, int_point_lat, int_point_lon, number_of_strikes
        FROM
        `bigquery-public-data.geo_us_boundaries.states` AS us_states,
        `bigquery-public-data.noaa_lightning.lightning_2020` AS lightning
        WHERE
        ST_WITHIN(lightning.center_point_geom,
            us_states.state_geom)
        ORDER BY lightning.number_of_strikes DESC LIMIT %s;
    """ % num_cities
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        # res = "city={}, pollution level={}".format(row[1], row[4])
        res = {
            "state": row[0],
            "lat": row[1],
            "lon": row[2],
            "number_strikes": row[3]
        }
        lightning_info.append(res)

    return jsonify({'lightning_info': lightning_info, 'success': True})


# getting hurricane data for all US cities or cities of interest
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
@app.route('/getSeismicData', methods=[ 'GET'])
def getSeismicData():
    CSV_URL = 'https://service.iris.edu/fdsnws/event/1/query?starttime=2021-04-01T04:43:28&orderby=time&format=text&limit=100&maxlat=49.384358&minlon=-124.848974&maxlon=-66.885444&minlat=24.396308&nodata=404'
    with requests.Session() as s:
        download = s.get(CSV_URL)

        decoded_content = download.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)
        seismic_data_list = []
        i = 0
        for row in my_list:

            if i >= 1:
                seismic_data = row[0].split('|')
                
                seismic_info = {
                    'id': seismic_data[0],
                    'latitude': seismic_data[2],
                    'longitude': seismic_data[3],
                    'depth': seismic_data[4],
                    'magnitude': seismic_data[-3]
                }
                seismic_data_list.append(seismic_info)

            i += 1
    return jsonify({'seismic_info': seismic_data_list, 'success': True})

# getting hurricane data for all US cities or cities of interest
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
@app.route('/getTemperatureData', methods=[ 'GET'])
def getTemperatureData():

    temperature_info = []
    num_cities = request.args.get('number')


    query = """
        SELECT
        latitude, longitude, sample_measurement
        FROM
        `bigquery-public-data.epa_historical_air_quality.temperature_hourly_summary` AS temperature,
        `bigquery-public-data.geo_us_boundaries.states` AS us_states
        WHERE
        ST_WITHIN(ST_GEOGPOINT(temperature.longitude,
            temperature.latitude),
            us_states.state_geom)
        ORDER BY temperature.sample_measurement DESC LIMIT %s;
    """ % num_cities
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        # res = "city={}, pollution level={}".format(row[1], row[4])
        res = {
            "latitude": row[0],
            "longitude": row[1],
            "temperature": row[2]
        }
        temperature_info.append(res)

    return jsonify({'temperature_info': temperature_info, 'success': True})


# automatically generating report with GPT-3 bot
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
@app.route('/uploadImageLabel', methods=[ 'POST'])
def uploadImageLabel():
    data = request.data
    loaded_json = json.loads(data)
    item0 = loaded_json["label0"]
    item1 = loaded_json["label1"]
    item2 = loaded_json["label2"]
    # latitude = loaded_json["latitude"]
    # longitude = loaded_json["longitude"]

    report = getAnswer(item0, item1, item2)
    print(report)

    # write into SQL table
    createReport(report, 40.3573, 74.6672)

    return jsonify({'success': True})




if __name__ == "__main__":
    tl.start(block=False)
    app.run(host='0.0.0.0', port=5000, threaded=True)
