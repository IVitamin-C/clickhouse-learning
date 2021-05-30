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
    uid_list = list(user_dict.keys())
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
    item_list = list(item_dict.keys())
    host = "127.0.0.1"
    port = 9000
    user = "default"
    passwd = ""
    database = "ods"
    insert_table = 'action_002_local'
    db_con = DB(host=host, port=port, user=user, passwd=passwd)
    data_list = []
    st = time.time()
    insert_cols = "second,platform,ip,isp,uid,ver,item_id,action_a_cnt,action_b_cnt,action_c_cnt,\
        action_a_time,action_b_time,action_c_time,action_d_sum,action_e_sum,action_f_sum"
    while 1:
        et = time.time()
        if len(data_list) > 10000 and et - st <= 5:
            time.sleep(5 - (et - st))
        if len(data_list) > 10000 or et - st >= 10:
            t0 = time.time()
            db_con.write_data(data=data_list, database=database, table=insert_table, insert_cols=insert_cols)
            t1 = time.time()
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\tInsert table successfully,sum:" + str(
                len(data_list)) + "\t time(s):" + str(round(t1 - t0, 4)))
            st = time.time()
            data_list=[]
            continue
        second = fake.date_time_between(start_date="-1d", end_date="now", tzinfo=None)
        uid = random.choice(uid_list)
        platform = user_dict[uid]['platform']
        ip = user_dict[uid]['ip']
        isp = user_dict[uid]['isp']
        ver = user_dict[uid]['app_version']
        item_id = random.choice(item_list)
        action_a_cnt = random.randint(1, 100)
        action_b_cnt = random.randint(0, action_a_cnt)
        action_c_cnt =  random.randint(0, action_b_cnt) if action_b_cnt > 0 else 0
        action_a_time = int(action_a_cnt *  random.randint(3000,5000)/10000 * random.randint(1000,2000))
        action_b_time = int(action_b_cnt *  random.randint(5000,8000)/10000 * random.randint(1000,3000))
        action_c_time = int(action_c_cnt *  random.randint(7000,10000)/10000 * random.randint(1000,4000))
        action_d_sum =  random.randint(0,10)
        action_e_sum = item_dict[item_id]['price'] * action_d_sum
        action_f_sum = int(action_e_sum *  random.randint(750,1000)/1000 * 100)
        data = (second,platform,ip,isp,uid,ver,item_id,action_a_cnt,action_b_cnt,action_c_cnt,action_a_time,
                action_b_time,action_c_time,action_d_sum,action_e_sum,action_f_sum)
        data_list.append(data)
        time.sleep(random.randint(5, 50) // 1000)
