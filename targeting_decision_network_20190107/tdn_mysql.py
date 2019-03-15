
import pymysql
import pandas as pd
import os


def transfer_week():
    conn = pymysql.connect(host=os.environ['mysql_db_host'], user=os.environ['mysql_db_user'],
                           password=os.environ['mysql_db_pwd'], db=os.environ['mysql_db_name'],
                           port=int(os.environ['mysql_db_port']))
    sql = 'UPDATE tdsn_base set w4sac=w3sac,w3sac=w2sac,w2sac=w1sac,w4pac=w3pac,w3pac=w2pac,w2pac=w1pac where id>0'
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def select_method_type():
    conn = pymysql.connect(host=os.environ['mysql_db_host'], user=os.environ['mysql_db_user'],
                           password=os.environ['mysql_db_pwd'], db=os.environ['mysql_db_name'],
                           port=int(os.environ['mysql_db_port']))
    sql = "SELECT audience_id, `type` FROM (SELECT audience_id, 'app_activity' AS `type` FROM app_activity_lookalike " \
          "UNION ALL SELECT audience_id, 'customer_file' AS `type` FROM seeds_lookalike UNION ALL " \
          "SELECT audience_id, 'value_based_lookalike' AS `type` FROM value_based_lookalike)tt"
    outs = pd.read_sql(sql, conn)
    method_type = dict()
    for index in range(len(outs)):
        row = outs.iloc[index]
        method_type[row['audience_id']] = row['type']
    conn.close()
    return method_type


def select_object(method, stype):
    conn = pymysql.connect(host=os.environ['mysql_db_host'], user=os.environ['mysql_db_user'],
                           password=os.environ['mysql_db_pwd'], db=os.environ['mysql_db_name'],
                           port=int(os.environ['mysql_db_port']))
    mstp = {'seed': {'customer_file': 'seeds_lookalike',
                     'app_activity': 'app_activity_lookalike',
                     'value_based_lookalike': 'value_based_lookalike'},
            'random': {'interest': 'dw_dim_interest',
                       'behavior': 'dw_dim_behavior'}
            }
    dct = dict()
    if method == 'random' and stype in mstp[method]:
        sql = "SELECT distinct id FROM "+mstp[method][stype]
        outs = pd.read_sql(sql, conn)
        for index in range(len(outs)):
            row = outs.iloc[index]
            dct[str(row['id'])] = [0, 0, 0, 0, 0, 0, 0, 0]
        conn.close()
        return dct
    elif method == 'seed' and stype in mstp[method]:
        sql = "SELECT distinct audience_id as id,delivery_name FROM {0} where audience_id is not null".format(mstp[method][stype])
        outs = pd.read_sql(sql, conn)
        for index in range(len(outs)):
            row = outs.iloc[index]
            if row['delivery_name'] not in dct:
                dct[row['delivery_name']] = {}
            dct[row['delivery_name']][str(row['id'])] = [0, 0, 0, 0, 0, 0, 0, 0]
        conn.close()
        return dct
    else:
        conn.close()
        return {}


def select_delivery_name():
    conn = pymysql.connect(host=os.environ['mysql_db_host'], user=os.environ['mysql_db_user'],
                           password=os.environ['mysql_db_pwd'], db=os.environ['mysql_db_name'],
                           port=int(os.environ['mysql_db_port']))
    sql = "select  audience_id,delivery_name from (SELECT  audience_id,delivery_name FROM seeds_lookalike union all " \
          "SELECT  audience_id,delivery_name FROM app_activity_lookalike union all " \
          "SELECT  audience_id,delivery_name FROM value_based_lookalike)tt"
    outs = pd.read_sql(sql, conn)
    dct = dict()
    for index in range(len(outs)):
        row = outs.iloc[index]
        audience = str(row['audience_id'])
        if audience not in dct:
            dct[audience] = {}
        platform = row['delivery_name'].split('_')
        if len(platform) == 3:
            pl = platform[2].lower()
            if pl not in dct[audience]:
                dct[audience][pl] = row['delivery_name']
    return dct


def select_videoid():
    conn = pymysql.connect(host=os.environ['mysql_db_host'], user=os.environ['mysql_db_user'],
                           password=os.environ['mysql_db_pwd'], db=os.environ['mysql_db_name'],
                           port=int(os.environ['mysql_db_port']))
    sql = "SELECT distinct videoId FROM dw_dim_creative_media"
    outs = pd.read_sql(sql, conn)
    dct = dict()
    for index in range(len(outs)):
        row = outs.iloc[index]
        dct[str(row['videoId'])] = [0, 0, 0, 0, 0, 0, 0, 0]
    conn.close()
    return dct

