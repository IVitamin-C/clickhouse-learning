import json
from models import UserDataProducer, ItemDataProducer, DB
import time
from faker import Factory
import random
import datetime
import os
if __name__ == '__main__':
    #判断是否生成文件，如果生成直接读取，未生成再生成。 
    Faker = Factory.create
    fake = Faker("zh_CN")
    user_dict={}
    item_dict={}
    if os.path.exists('files/user_dim.txt'):
        print('user_dim存在，直接读取')
        with open('files/user_dim.txt', 'r', encoding='utf-8') as f1:
            for line in f1:
               x=json.loads(line)
               user_dict[x['uid']]=x
    else:
        print('user_dim不存在，重新生成')
        # 生成安卓uuid 个数
        and_num = 10000
        # 生成ios uuid 个数
        ios_num = 5000
        user_pro = UserDataProducer(and_num=and_num, ios_num=ios_num)
        user_dict = user_pro.pro_user_info()

    host = "127.0.0.1"
    port = 9000
    user = "default"
    passwd = ""
    database = "ods"
    insert_table = 'user_dim_local'
    db_con = DB(host=host, port=port, user=user, passwd=passwd)
    data_list = []
    st = time.time()
    insert_cols = "day,uid,platform,country,province,isp,app_version,os_version,mac,ip,gender,age"
    #维表删除当天分区数据，兼容重复导入。
    db_con.execute("alter table {database}.{table} on cluster cluster drop partition '{day}';".format(database=database,table=insert_table,day=datetime.datetime.now().strftime('%Y-%m-%d')))
    print('删除当天分区数据成功')
    for  uid,info in user_dict.items():
        et = time.time()
        if len(data_list) > 2000 and et - st <= 5:
            time.sleep(5 - (et - st))
        if len(data_list) > 2000 or et - st >= 10:
            t0 = time.time()
            db_con.write_data(data=data_list, database=database, table=insert_table, insert_cols=insert_cols)
            t1 = time.time()
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\tInsert table successfully,sum:" + str(
                len(data_list)) + "\t time(s):" + str(round(t1 - t0, 4)))
            st = time.time()
            data_list=[]
        day=datetime.datetime.now().date()
        data=(day,uid,info['platform'],info['country'],info['province'],info['isp'],info['app_version'],
             info['os_version'],info['mac'],info['ip'],info['gender'],info['age'])
        data_list.append(data)
    if len(data_list)>0:
        db_con.write_data(data=data_list, database=database, table=insert_table, insert_cols=insert_cols)
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\tInsert table successfully,sum:" + str(
                len(data_list)))
    #维表删除历史分区数据，在导入期间，如果加载数据会重复。
    db_con.execute("alter table {database}.{table} on cluster cluster delete where day<>'{day}'".format(database=database,table=insert_table,day=datetime.datetime.now().strftime('%Y-%m-%d')))
    

