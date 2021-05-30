from models import ItemDataProducer
import time
import json

if __name__ == '__main__':
    t = time.time()
    item_pro = ItemDataProducer(item_num=100)
    item_dict = item_pro.pro_item()
    with open('files/item_dim.txt', 'w', encoding='utf-8') as f1:
        n = 0
        for k, v in item_dict.items():
            v['item_id'] = k
            mainTxt = json.dumps(v, ensure_ascii=False)
            f1.write(mainTxt + '\n')
    t1 = time.time()
    print('（整体）生成item信息完成' + "\t time(s):" + str(round(t1 - t, 4)))
