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
    objectId = json_data['object_id']

    def preprocess_data(data_json):
        if objectId:
            new_id = data_json["_id"]["oid"]
            data_json['_id'] = new_id
        for field in date_fields:
            date_time1 = data_json[field]
            data_json[field] = datetime.datetime.strptime(date_time1, '%Y-%m-%dT%H:%M:%S.%fZ')
        return data_json

    list_json_data = list(map(preprocess_data, json_data['data']))
    if objectId:
        list_ids = [ObjectId(doc['_id']) for doc in list_json_data]
    else :
        list_ids = [doc['_id'] for doc in list_json_data]

    db = client.get_database(schema)
    cursor = list(db[collection].find({ "_id" : {"$in": list_ids }}).limit(20))
    count = db[collection].count()

    total = len(list_json_data)
    falseTotal = 0

    def process_result(doc_mongo):
        doc_id = doc_mongo['_id']
        doc_mongo['_id'] = str(doc_id)
        return doc_mongo

    res_doc = list(map(process_result, cursor))
    
    for doc_json in list_json_data:
        res = next((sub for sub in res_doc if sub['_id'] == doc_json['_id']), None)
        diff = DeepDiff(res, doc_json)
        if bool(diff):
            print("-------------------------------------")
            print('db.' + collection + '.find({ "_id": ObjectId("' + doc_json['_id'] + '")})')
            print(diff)
            falseTotal += 1

    return f'\nTổng document : {count}\nSố doc sai : {falseTotal}\nTổng số doc check: {total}\n'


if __name__ == "__main__":
  app.run()