
from pymongo import MongoClient
import os
import log_maker


def get_evaluation(first_date, date):
    client = MongoClient(os.environ['mongo_url'], maxPoolSize=200)
    db = client.get_database(os.environ['db_name'])
    colles_insights = db.insights.aggregate([{'$lookup': {'from': "evaluation", 'localField': "ad_id",
                                                          'foreignField': "ad_id", 'as': "evaluation"}},
                                             {'$match': {'evaluation': {'$ne': []},
                                                         'report_date':{'$gte': first_date, '$lt': date},
                                                         '$or': [{'data.pay': {"$gte": "1"}}, {'data.spend': {"$gte": "1"}}]
                                                         }
                                              }]).batch_size(1)
    tinsights = list()
    for tp in colles_insights:
        if 'evaluation' in tp and len(tp['evaluation']) > 0:
            ads = tp['evaluation'][0]
            if 'pt'in ads and ads['pt']:
                if 'platform' not in ads['pt'] and 'name' in ads['pt']:
                    if 'ios' in ads['pt']['name'].lower():
                        ads['pt']['platform'] = 'iOS'
                    elif 'android' in ads['pt']['name'].lower():
                        ads['pt']['platform'] = 'Android'
                tinsights.append({'ad_id': tp['ad_id'], 'spend': float(tp['data']['spend']),
                                  'pay': int(tp['data']['pay']), 'pt': ads['pt'], 'report_date': tp['report_date']})
    client.close()
    log_maker.logger.info('load data')
    return tinsights


def find_deltname():
    client = MongoClient(os.environ['mongo_url'], maxPoolSize=200)
    db = client.get_database(os.environ['db_name'])
    delt_names = db.delivery.find({}, {'_id': 0, 'account_id': 1, 'name': 1})
    tdelt_names = dict()
    for dname in delt_names:
        if dname['account_id'] not in tdelt_names:
            tdelt_names[dname['account_id']] = []
        tdelt_names[dname['account_id']].append(dname['name'])
    client.close()
    return tdelt_names


def insert_tdn_base(data):
    client = MongoClient(os.environ['mongo_url'], maxPoolSize=200)
    db = client.get_database(os.environ['db_name'])
    log_maker.logger.info('clear the tdsn_base')
    db.tdsn_base.remove({})
    for ds in data:
        db.tdsn_base.insert(ds)
    log_maker.logger.info('add to tdsn_base')
    client.close()
