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
    equals={"medal":"l0", "team":"l1", "country":"l2", "sport":"l3", "year":"l4"}
    queries={
    equals["country"]+equals["sport"]+"_M": "SELECT sport, COUNT(DISTINCT country_noc) AS countries FROM men_events GROUP BY sport ORDER BY countries DESC LIMIT 10;",
    equals["country"]+equals["sport"]+"_F": "SELECT sport, COUNT(DISTINCT country_noc) AS countries FROM women_events GROUP BY sport ORDER BY countries DESC;",
    equals["sport"]+equals["country"]+"_M": "SELECT country_noc, COUNT(DISTINCT sport) AS sports_played FROM men_events GROUP BY country_noc ORDER BY sports_played DESC;",
    equals["sport"]+equals["country"]+"_F": "SELECT country_noc, COUNT(DISTINCT sport) AS sports_played FROM women_events GROUP BY country_noc ORDER BY sports_played DESC;",
    equals["country"]+equals["year"]+"_M": "SELECT country_noc, COUNT(DISTINCT edition) AS years_participated FROM men_events GROUP BY country_noc ORDER BY years_participated DESC;",
    equals["country"]+equals["year"]+"_F": "SELECT country_noc, COUNT(DISTINCT edition) AS years_participated FROM women_events GROUP BY country_noc ORDER BY years_participated DESC;",
    "k7": "SELECT edition, COUNT(DISTINCT country_noc) AS num_countries FROM men_events GROUP BY edition ORDER BY edition ASC;",
    "k8": "SELECT edition, COUNT(DISTINCT country_noc) AS num_countries FROM women_events GROUP BY edition ORDER BY edition ASC;",
    "k9": "SELECT medal, COUNT(DISTINCT country_noc) AS countries FROM men_events WHERE medal IN ('Gold', 'Silver', 'Bronze') GROUP BY medal ORDER BY CASE medal WHEN 'Gold' THEN 1 WHEN 'Silver' THEN 2 WHEN 'Bronze' THEN 3 END;",
    "k10": "SELECT medal, COUNT(DISTINCT country_noc) AS countries FROM women_events WHERE medal IN ('Gold', 'Silver', 'Bronze') GROUP BY medal ORDER BY CASE medal WHEN 'Gold' THEN 1 WHEN 'Silver' THEN 2 WHEN 'Bronze' THEN 3 END;",
    "k11": "SELECT country_noc, COUNT(CASE WHEN medal = 'Gold' THEN 1 ELSE NULL END) AS gold_medals, COUNT(CASE WHEN medal = 'Silver' THEN 1 ELSE NULL END) AS silver_medals, COUNT(CASE WHEN medal = 'Bronze' THEN 1 ELSE NULL END) AS bronze_medals FROM men_events WHERE medal IN ('Gold', 'Silver', 'Bronze') GROUP BY country_noc ORDER BY gold_medals DESC, silver_medals DESC, bronze_medals DESC;",
    "k12": "SELECT country_noc, COUNT(CASE WHEN medal = 'Gold' THEN 1 ELSE NULL END) AS gold_medals, COUNT(CASE WHEN medal = 'Silver' THEN 1 ELSE NULL END) AS silver_medals, COUNT(CASE WHEN medal = 'Bronze' THEN 1 ELSE NULL END) AS bronze_medals FROM women_events WHERE medal IN ('Gold', 'Silver', 'Bronze') GROUP BY country_noc ORDER BY gold_medals DESC, silver_medals DESC, bronze_medals DESC;",
    "k13": "SELECT country_noc, SUM(CASE WHEN \"isTeamSport\" = 'True' THEN 1 ELSE 0 END) AS num_team_events FROM men_events GROUP BY country_noc ORDER BY num_team_events DESC;",
    "k14": "SELECT country_noc, SUM(CASE WHEN “isTeamSport” = 'True' THEN 1 ELSE 0 END) AS num_team_events FROM women_events GROUP BY country_noc ORDER BY num_team_events DESC;",
    'k15': 'SELECT COUNT(DISTINCT CASE WHEN "isTeamSport" = \'True\' THEN country_noc END) AS num_countries_in_team_events FROM men_events;',
    'k16': 'SELECT COUNT(DISTINCT CASE WHEN "isTeamSport" = \'True\' THEN country_noc END) AS num_countries_in_team_events FROM women_events;',
    'k17': 'SELECT edition, sport, COUNT(*) AS num_times_played FROM men_events GROUP BY edition, sport;',
    'k18': 'SELECT edition, sport, COUNT(*) AS num_times_played FROM women_events GROUP BY edition, sport;',
    'k19': 'SELECT edition, COUNT(DISTINCT sport) AS num_sports_played FROM men_events GROUP BY edition;',
    'k20': 'SELECT edition, COUNT(DISTINCT sport) AS num_sports_played FROM women_events GROUP BY edition;',
    'k21': 'SELECT sport, COUNT(CASE WHEN medal = \'Gold\' THEN 1 END) AS num_gold_medals, COUNT(CASE WHEN medal = \'Silver\' THEN 1 END) AS num_silver_medals, COUNT(CASE WHEN medal = \'Bronze\' THEN 1 END) AS num_bronze_medals, COUNT(CASE WHEN medal IS NULL THEN 1 END) AS num_no_medals FROM men_events GROUP BY sport;',
    'k22': 'SELECT sport, COUNT(CASE WHEN medal = \'Gold\' THEN 1 END) AS num_gold_medals, COUNT(CASE WHEN medal = \'Silver\' THEN 1 END) AS num_silver_medals, COUNT(CASE WHEN medal = \'Bronze\' THEN 1 END) AS num_bronze_medals, COUNT(CASE WHEN medal IS NULL THEN 1 END) AS num_no_medals FROM women_events GROUP BY sport;',
    'k23': 'SELECT sport, medal, COUNT(*) AS total FROM men_events WHERE medal != \'na\' GROUP BY sport, medal ORDER BY sport, medal;',
    'k24': 'SELECT sport, medal, COUNT(*) AS total FROM women_events WHERE medal != \'na\' GROUP BY sport, medal ORDER BY sport, medal;',
    'k25': 'SELECT sport, COUNT(*) AS TotalTimesPlayedinTeam FROM men_events WHERE "isTeamSport" = \'True\' GROUP BY sport ORDER BY sport;',
    'k26': 'SELECT sport, COUNT(*) AS TotalTimesPlayedinTeam FROM women_events WHERE "isTeamSport" = \'True\' GROUP BY sport ORDER BY sport;',
    'k27': 'SELECT "isTeamSport", COUNT(DISTINCT sport) AS total FROM men_events GROUP BY "isTeamSport";',
    'k28': 'SELECT "isTeamSport", COUNT(DISTINCT sport) AS total FROM women_events GROUP BY "isTeamSport";',
    'k29': 'SELECT edition, COUNT(CASE WHEN medal = \'Gold\' THEN 1 END) AS gold_medals, COUNT(CASE WHEN medal = \'Silver\' THEN 1 END) AS silver_medals, COUNT(CASE WHEN medal = \'Bronze\' THEN 1 END) AS bronze_medals FROM men_events WHERE medal IN (\'Gold\', \'Silver\', \'Bronze\') GROUP BY edition ORDER BY edition;',
    'k30': "SELECT edition, COUNT(CASE WHEN medal = 'Gold' THEN 1 END) AS gold_medals, COUNT(CASE WHEN medal = 'Silver' THEN 1 END) AS silver_medals, COUNT(CASE WHEN medal = 'Bronze' THEN 1 END) AS bronze_medals FROM women_events WHERE medal IN ('Gold', 'Silver', 'Bronze') GROUP BY edition ORDER BY edition;",
    'k31': "SELECT DISTINCT edition AS year, 'Gold' AS medal FROM men_events WHERE medal = 'Gold' UNION SELECT DISTINCT edition AS year, 'Silver' AS medal FROM men_events WHERE medal = 'Silver' UNION SELECT DISTINCT edition AS year, 'Bronze' AS medal FROM men_events WHERE medal = 'Bronze' ORDER BY year, medal;",
    'k32': "SELECT DISTINCT edition AS year, 'Gold' AS medal FROM women_events WHERE medal = 'Gold' UNION SELECT DISTINCT edition AS year, 'Silver' AS medal FROM women_events WHERE medal = 'Silver' UNION SELECT DISTINCT edition AS year, 'Bronze' AS medal FROM women_events WHERE medal = 'Bronze' ORDER BY year, medal;",
    'k33': "SELECT edition, COUNT(DISTINCT sport) AS num_team_sports FROM men_events WHERE \"isTeamSport\" = 'True' GROUP BY edition ORDER BY edition ASC;",
    'k34': "SELECT edition, COUNT(DISTINCT sport) AS num_team_sports FROM women_events WHERE \"isTeamSport\" = 'True' GROUP BY edition ORDER BY edition ASC;",
    'k36': "SELECT COUNT(DISTINCT edition) as team_sports_years FROM men_events WHERE isTeamSport = 'True';",
    'k37': "SELECT \"isTeamSport\", medal, COUNT(*) AS total_medals FROM men_events WHERE medal <> 'na' GROUP BY \"isTeamSport\", medal ORDER BY medal;",
    'k38': "SELECT \"isTeamSport\", medal, COUNT(*) AS total_medals FROM women_events WHERE medal <> 'na' GROUP BY \"isTeamSport\", medal ORDER BY medal;",
    'k39': "SELECT \"isTeamSport\", medal, COUNT(CASE WHEN medal = 'Gold' AND \"isTeamSport\" = 'True' THEN 1 END) AS gold_team, COUNT(CASE WHEN medal = 'Silver' AND \"isTeamSport\" = 'True' THEN 1 END) AS silver_team, COUNT(CASE WHEN medal = 'Bronze' AND \"isTeamSport\" = 'True' THEN 1 END) AS bronze_team, COUNT(CASE WHEN medal = 'Gold' AND \"isTeamSport\" = 'False' THEN 1 END) AS gold_individual, COUNT(CASE WHEN medal = 'Silver' AND \"isTeamSport\" = 'False' THEN 1 END) AS silver_individual, COUNT(CASE WHEN medal = 'Bronze' AND \"isTeamSport\" = 'False' THEN 1 END) AS bronze_individual FROM men_events WHERE medal IS NOT NULL AND \"isTeamSport\" IS NOT NULL GROUP BY \"isTeamSport\", medal ORDER BY medal;",
    'k40': "SELECT \"isTeamSport\", medal, COUNT(CASE WHEN medal = 'Gold' AND \"isTeamSport\" = 'True' THEN 1 END) AS gold_team, COUNT(CASE WHEN medal = 'Silver' AND \"isTeamSport\" = 'True' THEN 1 END) AS silver_team, COUNT(CASE WHEN medal = 'Bronze' AND \"isTeamSport\" = 'True' THEN 1 END) AS bronze_team, COUNT(CASE WHEN medal = 'Gold' AND \"isTeamSport\" = 'False' THEN 1 END) AS gold_individual, COUNT(CASE WHEN medal = 'Silver' AND \"isTeamSport\" = 'False' THEN 1 END) AS silver_individual, COUNT(CASE WHEN medal = 'Bronze' AND \"isTeamSport\" = 'False' THEN 1 END) AS bronze_individual FROM women_events WHERE medal IS NOT NULL AND \"isTeamSport\" IS NOT NULL GROUP BY \"isTeamSport\", medal ORDER BY medal;"
    }
    
    # execute SQL query to get column data
    tabla = "women_events"
    cursor.execute(queries["l2l3_M"])

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
