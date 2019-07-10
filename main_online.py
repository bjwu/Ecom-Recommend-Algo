### 这里是在线模型的主进程
import pandas as pd
import numpy as np
import pickle
import gc
import redis
import socket
import threading
import time

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

jdata_user = pd.read_csv('./raw/jdata_user.csv',sep=',')
jdata_product = pd.read_csv('./raw/jdata_product.csv',sep=',')

### redis
r_79 = redis.Redis(host='localhost', port=6379, db=0)
r_80 = redis.Redis(host='localhost', port=6380, db=0)

### 当新的items过来
### TODO: 判断user或者item没在set中
def items2ffm(user:int, itemset, mapper, valid_cols:list):
    df_test = pd.DataFrame(itemset, columns=['sku_id'])
    tmp = df_test['sku_id']
    df_test['user_id'] = user
    df_test = df_test.merge(jdata_product,on='sku_id',how='left')
    df_test = df_test.merge(jdata_user,on='user_id',how='left')
    df_test = df_test.fillna(0)
    df_test = df_test[valid_cols]
    # 进行categocial过程
    for c in df_test.columns:
        df_test[c] = df_test[c].astype(mapper[c]['dtype']).astype(int) + mapper[c]['base']
    df_test['item_id'] = tmp   ### 这一行是推荐行，保存原始item id
    return df_test

### 在线CF
def items2items(items_rtn, k):
    ### using brand CF
    product_rtn = jdata_product[jdata_product['sku_id'].isin(items_rtn)]
    brand_rtn = set(product_rtn.brand.to_list())
    Rec = np.zeros(12000)
    for brand in brand_rtn:
        Rec += SIM_brand_list[brand]
    idx = set(np.argpartition(Rec, -k)[-k:])
    item_out = []
    import random
    for i in idx:
        item_out.extend(r_80.zrange('brand:'+str(int(i)), 0, k))
    return {int(i) for i in random.sample(item_out, k)}

def tcplink(sock, addr):
    print('Accept new connection from %s:%s...' % addr)
    sock.send(b'Welcome!!!\n')

    cnt = 0
    while True:
        data = sock.recv(1024)
        try:
            user, item = data.decode('UTF-8').strip().split(' ')
            assert item.isdigit()
            assert user.isdigit()
        except:
            error = "please give a btye object like b'userid itemid', thanks"
            sock.send(error.encode())
            continue

        items_before = [int(i[:-2]) for i in r_79.lrange("latest:"+ str(int(user)), 0, 9)]

        ### TODO:应该把用户信息存在redis中
        city = int(jdata_user[jdata_user['user_id']==int(user)].city.values[0])
        age = int(jdata_user[jdata_user['user_id']==int(user)].age.values[0])

        ### 得到city和age的items candidates

        items_rtn_city = {int(i) for i in r_80.zrange('city:'+ str(int(user)), 0, 10, desc=True)}
        items_rtn_age = {int(i) for i in r_80.zrange('age:'+ str(int(user)), 0, 10, desc=True)}

        ### 得到hottest candidiates
        item_rtn_hot = {int(i) for i in r_80.zrange('hottest_one_day_before', 0, 10, desc=True)}

        ### 得到CF的items candidates
        item_rtn_cf = items2items(items_before, 10)

        ### 整合items
        item_rtn = items_rtn_city | items_rtn_age | item_rtn_hot | item_rtn_cf

        ### items2ffm
        valid_cols = ['age','sex','city','county','brand','shop_id','cate','user_id','sku_id']
        ffm = items2ffm(int(user), list(item_rtn), cate_memo, valid_cols)
        
        towrite = str(user) + ':\n'
        for item_ffm in ffm.iterrows():
            towrite += '\t'.join([str(a) for a in list(item_ffm[1].values)])
            towrite += '\n'
        try:
            sock.send(towrite.encode())
        except:
            break
    sock.close()
    print('Connection from %s:%s closed.' % addr)

if __name__ == '__main__':
    
    ### 打开端口
    s.bind(('localhost', 8022))
    s.listen(5)
    print('Waiting for connection...')
    
    ### 得到cate_memo 和 SIM
    f = open('./model/cate_memo.pkl', 'rb')
    cate_memo = pickle.load(f)
    
    f = open('./model/brand_sim_metrix.pkl', 'rb')
    SIM_brand_list = pickle.load(f)
    
    while True:
    # 接受一个新连接:
        sock, addr = s.accept()
        # 创建新线程来处理TCP连接:
        t = threading.Thread(target=tcplink, args=(sock, addr))
        t.start()
    
    ### to flink
    
    