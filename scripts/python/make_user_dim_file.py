from scripts.python.models import UserDataProducer
import time
import json

if __name__ == '__main__':
    t = time.time()
    # 生成安卓uuid 个数
    and_num = 10000
    # 生成ios uuid 个数
    ios_num = 5000
    pro = UserDataProducer(and_num=and_num, ios_num=ios_num)
    user_dict = pro.pro_user_info()
    with open('../../files/user_dim.txt', 'w', encoding='utf-8') as f1:
        n = 0
        for k, v in user_dict.items():
            v['uid'] = k
            mainTxt = json.dumps(v, ensure_ascii=False)
            f1.write(mainTxt + '\n')
    t1 = time.time()
    print('（整体）生成用户信息完成' + "\t time(s):" + str(round(t1 - t, 4)))
