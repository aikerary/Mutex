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
    equals["country"]+equals["sport"]+"_M": "SELECT sport, COUNT(DISTINCT country_noc) AS countries FROM men_events GROUP BY sport ORDER BY countries DESC",
    equals["country"]+equals["sport"]+"_F": "SELECT sport, COUNT(DISTINCT country_noc) AS countries FROM women_events GROUP BY sport ORDER BY countries DESC",
    equals["sport"]+equals["country"]+"_M": "SELECT country_noc, COUNT(DISTINCT sport) AS sports_played FROM men_events GROUP BY country_noc ORDER BY sports_played DESC",
    equals["sport"]+equals["country"]+"_F": "SELECT country_noc, COUNT(DISTINCT sport) AS sports_played FROM women_events GROUP BY country_noc ORDER BY sports_played DESC",
    equals["country"]+equals["year"]+"_M": "SELECT country_noc, COUNT(DISTINCT edition) AS years_participated FROM men_events GROUP BY country_noc ORDER BY years_participated DESC",
    equals["country"]+equals["year"]+"_F": "SELECT country_noc, COUNT(DISTINCT edition) AS years_participated FROM women_events GROUP BY country_noc ORDER BY years_participated DESC",
    equals["year"]+equals["country"]+"_M": "SELECT edition, COUNT(DISTINCT country_noc) AS num_countries FROM men_events GROUP BY edition ORDER BY edition ASC",
    equals["year"]+equals["country"]+"_F": "SELECT edition, COUNT(DISTINCT country_noc) AS num_countries FROM women_events GROUP BY edition ORDER BY edition ASC",
    equals["country"]+equals["medal"]+"_M": "SELECT medal, COUNT(DISTINCT country_noc) AS countries FROM men_events WHERE medal IN ('Gold', 'Silver', 'Bronze') GROUP BY medal ORDER BY CASE medal WHEN 'Gold' THEN 1 WHEN 'Silver' THEN 2 WHEN 'Bronze' THEN 3 END",
    equals["country"]+equals["medal"]+"_F": "SELECT medal, COUNT(DISTINCT country_noc) AS countries FROM women_events WHERE medal IN ('Gold', 'Silver', 'Bronze') GROUP BY medal ORDER BY CASE medal WHEN 'Gold' THEN 1 WHEN 'Silver' THEN 2 WHEN 'Bronze' THEN 3 END",
    equals["medal"]+equals["country"]+"_M": "SELECT country_noc, COUNT(CASE WHEN medal = 'Gold' THEN 1 ELSE NULL END) AS gold_medals, COUNT(CASE WHEN medal = 'Silver' THEN 1 ELSE NULL END) AS silver_medals, COUNT(CASE WHEN medal = 'Bronze' THEN 1 ELSE NULL END) AS bronze_medals FROM men_events WHERE medal IN ('Gold', 'Silver', 'Bronze') GROUP BY country_noc ORDER BY gold_medals DESC, silver_medals DESC, bronze_medals DESC LIMIT 10;",
    equals["medal"]+equals["country"]+"_F": "SELECT country_noc, COUNT(CASE WHEN medal = 'Gold' THEN 1 ELSE NULL END) AS gold_medals, COUNT(CASE WHEN medal = 'Silver' THEN 1 ELSE NULL END) AS silver_medals, COUNT(CASE WHEN medal = 'Bronze' THEN 1 ELSE NULL END) AS bronze_medals FROM women_events WHERE medal IN ('Gold', 'Silver', 'Bronze') GROUP BY country_noc ORDER BY gold_medals DESC, silver_medals DESC, bronze_medals DESC",
    equals["country"]+equals["team"]+"_M": "SELECT country_noc, SUM(CASE WHEN \"isTeamSport\" = 'True' THEN 1 ELSE 0 END) AS num_team_events FROM men_events GROUP BY country_noc ORDER BY num_team_events DESC",
    equals["country"]+equals["team"]+"_F": "SELECT country_noc, SUM(CASE WHEN “isTeamSport” = 'True' THEN 1 ELSE 0 END) AS num_team_events FROM women_events GROUP BY country_noc ORDER BY num_team_events DESC",
    equals["team"]+equals["country"]+"_M": 'SELECT COUNT(DISTINCT CASE WHEN "isTeamSport" = \'True\' THEN country_noc END) AS num_countries_in_team_events FROM men_events',
    equals["team"]+equals["country"]+"_F": 'SELECT COUNT(DISTINCT CASE WHEN "isTeamSport" = \'True\' THEN country_noc END) AS num_countries_in_team_events FROM women_events',
    equals["sport"]+equals["year"]+"_M": 'SELECT edition, sport, COUNT(*) AS num_times_played FROM men_events GROUP BY edition, sport',
    equals["sport"]+equals["year"]+"_F": 'SELECT edition, sport, COUNT(*) AS num_times_played FROM women_events GROUP BY edition, sport',
    equals["year"]+equals["sport"]+"_M": 'SELECT edition, COUNT(DISTINCT sport) AS num_sports_played FROM men_events GROUP BY edition',
    equals["year"]+equals["sport"]+"_F": 'SELECT edition, COUNT(DISTINCT sport) AS num_sports_played FROM women_events GROUP BY edition',
    equals["sport"]+equals["medal"]+"_M": 'SELECT sport, COUNT(CASE WHEN medal = \'Gold\' THEN 1 END) AS num_gold_medals, COUNT(CASE WHEN medal = \'Silver\' THEN 1 END) AS num_silver_medals, COUNT(CASE WHEN medal = \'Bronze\' THEN 1 END) AS num_bronze_medals, COUNT(CASE WHEN medal IS NULL THEN 1 END) AS num_no_medals FROM men_events GROUP BY sport',
    equals["sport"]+equals["medal"]+"_F": 'SELECT sport, COUNT(CASE WHEN medal = \'Gold\' THEN 1 END) AS num_gold_medals, COUNT(CASE WHEN medal = \'Silver\' THEN 1 END) AS num_silver_medals, COUNT(CASE WHEN medal = \'Bronze\' THEN 1 END) AS num_bronze_medals, COUNT(CASE WHEN medal IS NULL THEN 1 END) AS num_no_medals FROM women_events GROUP BY sport',
    equals["medal"]+equals["sport"]+"_M": 'SELECT sport, medal, COUNT(*) AS total FROM men_events WHERE medal != \'na\' GROUP BY sport, medal ORDER BY sport, medal',
    equals["medal"]+equals["sport"]+"_F": 'SELECT sport, medal, COUNT(*) AS total FROM women_events WHERE medal != \'na\' GROUP BY sport, medal ORDER BY sport, medal',
    equals["sport"]+equals["team"]+"_M": 'SELECT sport, COUNT(*) AS TotalTimesPlayedinTeam FROM men_events WHERE "isTeamSport" = \'True\' GROUP BY sport ORDER BY sport',
    equals["sport"]+equals["team"]+"_F": 'SELECT sport, COUNT(*) AS TotalTimesPlayedinTeam FROM women_events WHERE "isTeamSport" = \'True\' GROUP BY sport ORDER BY sport',
    equals["team"]+equals["sport"]+"_M": 'SELECT "isTeamSport", COUNT(DISTINCT sport) AS total FROM men_events GROUP BY "isTeamSport"',
    equals["team"]+equals["sport"]+"_F": 'SELECT "isTeamSport", COUNT(DISTINCT sport) AS total FROM women_events GROUP BY "isTeamSport"',
    equals["year"]+equals["medal"]+"_M": 'SELECT edition, COUNT(CASE WHEN medal = \'Gold\' THEN 1 END) AS gold_medals, COUNT(CASE WHEN medal = \'Silver\' THEN 1 END) AS silver_medals, COUNT(CASE WHEN medal = \'Bronze\' THEN 1 END) AS bronze_medals FROM men_events WHERE medal IN (\'Gold\', \'Silver\', \'Bronze\') GROUP BY edition ORDER BY edition',
    equals["year"]+equals["medal"]+"_F": "SELECT edition, COUNT(CASE WHEN medal = 'Gold' THEN 1 END) AS gold_medals, COUNT(CASE WHEN medal = 'Silver' THEN 1 END) AS silver_medals, COUNT(CASE WHEN medal = 'Bronze' THEN 1 END) AS bronze_medals FROM women_events WHERE medal IN ('Gold', 'Silver', 'Bronze') GROUP BY edition ORDER BY edition",
    equals["medal"]+equals["year"]+"_M": "SELECT DISTINCT edition AS year, 'Gold' AS medal FROM men_events WHERE medal = 'Gold' UNION SELECT DISTINCT edition AS year, 'Silver' AS medal FROM men_events WHERE medal = 'Silver' UNION SELECT DISTINCT edition AS year, 'Bronze' AS medal FROM men_events WHERE medal = 'Bronze' ORDER BY year, medal",
    equals["medal"]+equals["year"]+"_F": "SELECT DISTINCT edition AS year, 'Gold' AS medal FROM women_events WHERE medal = 'Gold' UNION SELECT DISTINCT edition AS year, 'Silver' AS medal FROM women_events WHERE medal = 'Silver' UNION SELECT DISTINCT edition AS year, 'Bronze' AS medal FROM women_events WHERE medal = 'Bronze' ORDER BY year, medal",
    equals["year"]+equals["team"]+"_M": "SELECT edition, COUNT(DISTINCT sport) AS num_team_sports FROM men_events WHERE \"isTeamSport\" = 'True' GROUP BY edition ORDER BY edition ASC",
    equals["year"]+equals["team"]+"_F": "SELECT edition, COUNT(DISTINCT sport) AS num_team_sports FROM women_events WHERE \"isTeamSport\" = 'True' GROUP BY edition ORDER BY edition ASC",
    equals["team"]+equals["year"]+"_M": "SELECT COUNT(DISTINCT edition) as team_sports_years FROM men_events WHERE isTeamSport = 'True'",
    equals["team"]+equals["year"]+"_F": "SELECT COUNT(DISTINCT edition) as team_sports_years FROM women_events WHERE isTeamSport = 'True'",
    equals["medal"]+equals["team"]+"_M": "SELECT \"isTeamSport\", medal, COUNT(*) AS total_medals FROM men_events WHERE medal <> 'na' GROUP BY \"isTeamSport\", medal ORDER BY medal",
    equals["medal"]+equals["team"]+"_F": "SELECT \"isTeamSport\", medal, COUNT(*) AS total_medals FROM women_events WHERE medal <> 'na' GROUP BY \"isTeamSport\", medal ORDER BY medal",
    equals["team"]+equals["medal"]+"_M": "SELECT \"isTeamSport\", medal, COUNT(CASE WHEN medal = 'Gold' AND \"isTeamSport\" = 'True' THEN 1 END) AS gold_team, COUNT(CASE WHEN medal = 'Silver' AND \"isTeamSport\" = 'True' THEN 1 END) AS silver_team, COUNT(CASE WHEN medal = 'Bronze' AND \"isTeamSport\" = 'True' THEN 1 END) AS bronze_team, COUNT(CASE WHEN medal = 'Gold' AND \"isTeamSport\" = 'False' THEN 1 END) AS gold_individual, COUNT(CASE WHEN medal = 'Silver' AND \"isTeamSport\" = 'False' THEN 1 END) AS silver_individual, COUNT(CASE WHEN medal = 'Bronze' AND \"isTeamSport\" = 'False' THEN 1 END) AS bronze_individual FROM men_events WHERE medal IS NOT NULL AND \"isTeamSport\" IS NOT NULL GROUP BY \"isTeamSport\", medal ORDER BY medal",
    equals["team"]+equals["medal"]+"_F": "SELECT \"isTeamSport\", medal, COUNT(CASE WHEN medal = 'Gold' AND \"isTeamSport\" = 'True' THEN 1 END) AS gold_team, COUNT(CASE WHEN medal = 'Silver' AND \"isTeamSport\" = 'True' THEN 1 END) AS silver_team, COUNT(CASE WHEN medal = 'Bronze' AND \"isTeamSport\" = 'True' THEN 1 END) AS bronze_team, COUNT(CASE WHEN medal = 'Gold' AND \"isTeamSport\" = 'False' THEN 1 END) AS gold_individual, COUNT(CASE WHEN medal = 'Silver' AND \"isTeamSport\" = 'False' THEN 1 END) AS silver_individual, COUNT(CASE WHEN medal = 'Bronze' AND \"isTeamSport\" = 'False' THEN 1 END) AS bronze_individual FROM women_events WHERE medal IS NOT NULL AND \"isTeamSport\" IS NOT NULL GROUP BY \"isTeamSport\", medal ORDER BY medal"
    }
    
    # execute SQL query to get column data
    cursor.execute(queries["l0l2_M"])
    
    # fetch all rows
    results = cursor.fetchall()
    # Obtener los nombres de las columnas
    column_names = [desc[0] for desc in cursor.description]

# Crear una lista de diccionarios, cada uno representa una fila de resultados
    json_results = []
    for row in results:
        row_dict = {}
        for idx, col in enumerate(row):
            col_name = column_names[idx]
            row_dict[col_name] = col
        json_results.append(row_dict)

# Crear un diccionario con el encabezado de la tabla y la lista de resultados
    json_data = {
    "header": column_names,
    "rows": json_results
    }

    # close cursor and connection
    cursor.close()
    conn.close()

    # return JSON data
    return jsonify(json_data)

if __name__ == '__main__':
    # App run on port 5000
    app.run(port=5000)
    app.run(debug=True)
