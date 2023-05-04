from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import json

app = Flask(__name__)
CORS(app)

# define route for POST request
@app.route('/data', methods=['POST'])
def get_data():
    # establish connection to database
    conn = psycopg2.connect(
     host="dpg-ch5bsu1jvhts7knifekg-a.oregon-postgres.render.com",
     database="mutexdatabase",
     user="mutexdatabase_user",
     password="fjIWP0PAHvHQJWAmkOdFMLpiT6CM9X9p"
    )
    # create a cursor
    cursor = conn.cursor()
    # Create temporary tables
    cursor.execute("CREATE TEMPORARY TABLE men_events AS SELECT * FROM olympic WHERE event LIKE '%Men%'")
    cursor.execute("CREATE TEMPORARY TABLE women_events AS SELECT * FROM olympic WHERE event LIKE '%Women%'")
    # get data from request
    data = request.get_json()
    first = data['first']
    second = data['second']
    upper = data['upper']
    lower = data['lower']
    column_name = data['column_name']
    num_rows = data['num_rows']
    
    # execute SQL query to get column data
    tabla = "women_events"
    cursor.execute(f"SELECT {column_name} FROM {tabla} LIMIT {num_rows}")

    # fetch all rows
    rows = cursor.fetchall()

    # create a list of column data
    column_data = [row[0] for row in rows]

    # create JSON object
    json_data = json.dumps(column_data)

    # close cursor and connection
    cursor.close()
    conn.close()

    # return JSON data
    return jsonify(json_data)

if __name__ == '__main__':
    # App run on port 5000
    app.run(port=5000)
    app.run(debug=True)
