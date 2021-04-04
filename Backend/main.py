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

from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

tl = Timeloop()

app = Flask(__name__)
cors = CORS(app, resources={
                r"/createResource": {"origins": "*"},
                r"/createFire": {"origins": "*"},
                r"createOperation": {"Origins": "*"},
                r"getFireInfo": {"Origins": "*"},
                r"getResourceInfo": {"Origins": "*"},
                r"getPollutionData": {"Origins": "*"},
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



@app.route('/')
def index():
    return 'invalid call'


'''
function for creating a new firefighting resource
'''
@app.route('/createResource', methods=['GET', 'POST','OPTIONS'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def createResource():
    # randomly generate customer ID during signup
    r_id = random_string_lower_case(64)

    title = request.json["title"]
    type = request.json["type"]
    latitude = request.json["latitude"]
    longitude = request.json["longitude"]



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
            cursor.execute("INSERT INTO [dbo].[Resources] \
                 (resource_id, title, r_type, deployed, latitude, longitude) \
                     VALUES ('%s', '%s', '%s', %d, %.4f, %.4f);" % (r_id, title, type, 0, latitude, longitude))
            conn.commit()
            
    return jsonify({'r_id':r_id,'success':True})

'''
function for handling creating new fire incident reports
'''
@app.route('/createFire', methods=['GET', 'POST','OPTIONS'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def createFire():
    # randomly generate customer ID during signup
    report_id = random_string_lower_case(64)
    brief = request.json["brief"]
    latitude = request.json["latitude"]
    longitude = request.json["longitude"]

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
            cursor.execute("INSERT INTO [dbo].[Fire] \
                 (report_id, brief, latitude, longitude) \
                     VALUES ('%s', '%s', %.4f, %.4f);" % (report_id, brief, latitude, longitude))
            conn.commit()
            
    return jsonify({'report_id':report_id,'success':True})


'''
function for handling creating new fire incident reports
'''
@app.route('/createOperation', methods=['GET', 'POST','OPTIONS'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def createOperation():
    # randomly generate customer ID during signup
    operation_id = random_string_lower_case(64)
    resource_id = request.json["resource_id"]
    fire_id = request.json["fire_id"]

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
            cursor.execute("CREATE TABLE [dbo].[FirefightingOperation] \
                 (operation_id, resource_id, fire_id) \
                     VALUES ('%s', '%s', '%s');" % (operation_id, resource_id, fire_id))
            conn.commit()

    return jsonify({'operation_id':operation_id, 'success':True})


'''
function for handling order placing request
'''
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
@app.route('/getFireInfo', methods=[ 'GET'])
def getFireInfo():
    drivers = [item for item in pyodbc.drivers()]
    driver = drivers[-1]
    print("driver:{}".format(driver))

    server = 'us-gp-db-server.database.windows.net'
    database = 'usw-db-gp'
    username = 'sql-db-admin'
    password = 'Pwned_2023'
    driver = '{ODBC Driver 17 for SQL Server}'

    fire_list = []


    with pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=' + server + ';\
                PORT=1433;DATABASE=' + database + ';\
                    UID=' + username + ';\
                        PWD=' + password) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM [dbo].[Fire]")
            # cursor.execute("SELECT latitude, longitude FROM [dbo].[Customers];")

            row = cursor.fetchone()
            while row is not None:
                # add the restaurants within search range to the list

                fire_list.append({
                    'id':row[0],
                    'brief': row[1],
                    'create_date': row[2],
                    'latitude': float(row[3]),
                    'longitude': float(row[4])
                })
                row = cursor.fetchone()

        

    return jsonify({'fire_info': fire_list, 'success': True})



@cross_origin(origin='*',headers=['Content-Type','Authorization'])
@app.route('/getResourceInfo', methods=[ 'GET'])
def getResourceInfo():
    drivers = [item for item in pyodbc.drivers()]
    driver = drivers[-1]
    print("driver:{}".format(driver))

    server = 'ubuntu1.database.windows.net'
    database = 'database-gp'
    username = 'gcp-usw-sql-server'
    password = 'Pwned_2023'
    driver = '{ODBC Driver 17 for SQL Server}'

    resource_list = []


    with pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=' + server + ';\
                PORT=1433;DATABASE=' + database + ';\
                    UID=' + username + ';\
                        PWD=' + password) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM [dbo].[Resources]")
            # cursor.execute("SELECT latitude, longitude FROM [dbo].[Customers];")

            row = cursor.fetchone()
            while row is not None:
                # add the restaurants within search range to the list

                resource_list.append({
                    'resource_id':row[0],
                    'title': row[1],
                    'r_type': row[2],
                    'deployed': int(row[3]),
                    'created_date': row[4],
                    'latitude': float(row[5]),
                    'longitude': float(row[6])
                })
                row = cursor.fetchone()

        

    return jsonify({'resource_info': resource_list, 'success': True})


# getting polution data for all US cities or cities of interest
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
@app.route('/getPollutionData', methods=[ 'GET'])
def getPollutionData():

    resource_list = []
    num_cities = request.args.get('number')


    query = """
        SELECT
        *
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
        resource_list.append(res)
        print(res)

    return jsonify({'resource_info': resource_list, 'success': True})





if __name__ == "__main__":
    tl.start(block=False)
    app.run(host='0.0.0.0', port=5000, threaded=True)
