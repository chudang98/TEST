import datetime
from flask import request, Flask
from flask_pymongo import MongoClient, ObjectId
from deepdiff import DeepDiff
import os

app = Flask(__name__)
db_uri = os.getenv('MONGO_URI')
client = MongoClient(db_uri)

@app.route("/<schema>/<collection>")
def checkDoc(schema=None, collection=None):
    json_data = request.json
    date_fields = json_data['date']
    def preprocess_data(data_json):
        new_id = data_json["_id"]["oid"]
        data_json['_id'] = new_id
        for field in date_fields:
            date_time1 = data_json[field]
            data_json[field] = datetime.datetime.strptime(date_time1, '%Y-%m-%dT%H:%M:%S.%fZ')
        return data_json

    list_json_data = list(map(preprocess_data, json_data['data']))
    list_ids = [ObjectId(doc['_id']) for doc in list_json_data]

    db = client.get_database(schema)
    # cursor = db[schema].find({ "_id" : {"$in": list_ids }})
    cursor = db[collection].find().limit(10)

    for t in cursor:
        print(t)

    for doc_json in list_json_data:
        res = next((sub for sub in cursor if sub['_id'] == doc_json['_id']), None)
        diff = DeepDiff(res, doc_json)
        if bool(diff):
            print("KHÁC NHAU")
            break
    
    return f'{schema} -- {collection}'


if __name__ == "__main__":
  app.run()