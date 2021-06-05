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
    item_dict={}
    if os.path.exists('files/item_dim.txt'):
        print('item_dim存在，直接读取')
        with open('files/item_dim.txt', 'r', encoding='utf-8') as f1:
            for line in f1:
                x=json.loads(line)
                item_dict[x['item_id']]=x
    else:
        print('item_dim不存在，重新生成')
        item_pro = ItemDataProducer(item_num=100)
        item_dict = item_pro.pro_item()

    host = "127.0.0.1"
    port = 9000
    user = "default"
    passwd = ""
    database = "ods"
    insert_table = 'item_dim_local'
    db_con = DB(host=host, port=port, user=user, passwd=passwd)
    data_list = []
    st = time.time()
    insert_cols = "day,item_id,type_id,price"
    db_con.execute("alter table {database}.{table} on cluster cluster  drop partition '{day}';".format(database=database,table=insert_table,day=datetime.datetime.now().strftime('%Y-%m-%d')))
    for item_id,info in item_dict.items():
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
        data=(day,item_id,info['type_id'],info['price'])
        data_list.append(data)
    if len(data_list)>0:
        db_con.write_data(data=data_list, database=database, table=insert_table, insert_cols=insert_cols)
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\tInsert table successfully,sum:" + str(
                len(data_list)))
    db_con.execute("alter table {database}.{table} on cluster cluster  delete where day<>'{day}'".format(database=database,table=insert_table,day=datetime.datetime.now().strftime('%Y-%m-%d')))

    

