from test_scripts.make_user_dim_info import Producer
import time
import json

if __name__ == '__main__':
    pro = Producer()
    t = time.time()
    user_dict = pro.pro_user_info()
    # 生成安卓uuid 个数
    andnum = 10000
    # 生成ios uuid 个数
    iosnum = 5000
    with open('../test_files/user_dim.txt', 'w', encoding='utf-8') as f1:
        n = 0
        for k, v in user_dict.items():
            v['uid'] = k
            mainTxt = json.dumps(v, ensure_ascii=False)
            f1.write(mainTxt + '\n')
    t1 = time.time()
    print('（整体）生成用户信息完成' + "\t time(s):" + str(round(t1 - t, 4)))