from typing import Dict
from flask import request, Flask
import os
from flask.json import jsonify
from pyhive import hive
from TCLIService.ttypes import TOperationState

app = Flask(__name__)
db_uri = os.getenv('HIVE_URI', "jdbc:hive2://localhost")

@app.route("/random")
def randomData():
    json_data = request.json
    tables = json_data['tables']
    prefix_table = json_data['table_prefix']
    in_date = json_data['date']

    # host = "host.docker.internal"
    host = "localhost"

    conn = hive.Connection(host=host, port=10000)

    for table in tables:
        file = open(f'./json_data/{table}.json', "w+")
        from_table = prefix_table + table
        query = """
            SELECT message
            FROM delta.`{from_table}`
            WHERE DATE(timestamp) = '{in_date}'
            ORDER BY RAND()
            LIMIT 10
        """.format(from_table=from_table, in_date=in_date)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        file.write('[')
        for row in results:
            file.write(str(row[0]))
            file.write(',')
            file.write('\n')
        file.write(']')
        file.close()
    return 'HELLO'


@app.route("/count")
def countRow():
    json_data = request.json
    tables = json_data['tables']
    prefix_table = json_data['table_prefix']
    in_date = json_data['date']

    # host = "host.docker.internal"
    host = "localhost"

    conn = hive.Connection(host=host, port=10000)
    response_json = {}

    for table in tables:
        from_table = prefix_table + table
        query = """
            SELECT COUNT(*) as count
            FROM delta.`{from_table}`
            WHERE DATE(timestamp) = '{in_date}'
        """.format(from_table=from_table, in_date=in_date)

        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        response_json[table] = result[0]
        print(f'{table} -- {result[0]}')

    return jsonify(response_json)


if __name__ == "__main__":
  app.run()