from scripts.python.models import UserDataProducer, ItemDataProducer, DB
import time
from faker import Factory
import random
import datetime

if __name__ == '__main__':
    Faker = Factory.create
    fake = Faker("zh_CN")
    # 生成安卓uuid 个数
    and_num = 10000
    # 生成ios uuid 个数
    ios_num = 5000
    user_pro = UserDataProducer(and_num=and_num, ios_num=ios_num)
    user_dict = user_pro.pro_user_info()
    uid_list = list(user_dict.keys())
    item_pro = ItemDataProducer(item_num=100)
    item_dict = item_pro.pro_item()
    item_list = list(item_dict.keys())
    host = "172.20.111.124"
    port = 9000
    user = "default"
    passwd = ""
    database = "ods"
    insert_table = 'action_001_local'
    db_con = DB(host=host, port=port, user=user, passwd=passwd)
    data_list = []
    st = time.time()
    insert_cols = "second,ip,isp,uid,ver,item_id,show_cnt,click_cnt,show_time"
    while 1:
        et = time.time()
        if len(data_list) > 1000 and et - st <= 1:
            time.sleep(1 - (et - st))
        if len(data_list) > 1000 or et - st >= 10:
            t0 = time.time()
            db_con.write_data(data=data_list, database=database, table=insert_table, insert_cols=insert_cols)
            t1 = time.time()
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\tInsert table successfully,sum:" + str(
                len(data_list)) + "\t time(s):" + str(round(t1 - t0, 4)))
            st = time.time()
        second = fake.date_time_between(start_date="-1d", end_date="now", tzinfo=None)
        uid = random.choice(uid_list)
        ip = user_dict[uid]['ip']
        isp = user_dict[uid]['isp']
        ver = user_dict[uid]['app_version']
        item_id = random.choice(item_list)
        show_cnt = random.randint(1, 100)
        click_cnt = random.randint(0, show_cnt)
        show_time = random.randint(1000, 30000)  # 毫秒
        data = """{second},{ip},{isp},{uid},{ver},{item_id},{show_cnt},{click_cnt},{show_time}""".format(
            second=second, ip=ip, isp=isp, uid=uid, ver=ver, item_id=item_id, show_cnt=show_cnt,
            click_cnt=click_cnt, show_time=show_time)
        data_list.append(data)
        time.sleep(random.randint(10, 100) // 1000)
