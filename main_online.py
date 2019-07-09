### 这里是在线模型的主进程
import pandas as pd
import numpy as np
import pickle
import gc
import redis

jdata_user = pd.read_csv('./raw/jdata_user.csv',sep=',')
jdata_product = pd.read_csv('./raw/jdata_product.csv',sep=',')

### redis
r_79 = redis.Redis(host='localhost', port=6379, db=0)
r_80 = redis.Redis(host='localhost', port=6380, db=0)

### 当新的items过来
### TODO: 判断user或者item没在set中
def items2ffm(user:int, itemset, mapper, valid_cols:list):
    df_test = pd.DataFrame(itemset, columns=['sku_id'])
    df_test['user_id'] = user
    df_test = df_test.merge(jdata_product,on='sku_id',how='left')
    df_test = df_test.merge(jdata_user,on='user_id',how='left')
    df_test = df_test.fillna(0)
    df_test = df_test[valid_cols]
    # 进行categocial过程
    for c in df_test.columns:
        df_test[c] = df_test[c].astype(mapper[c]['dtype']).astype(int) + mapper[c]['base']
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
        item_out.extend(r.zrange('brand:'+str(int(i)), 0, k))
    return {int(i) for i in random.sample(item_out, k)}

if __name__ == '__main__':
    
    user = 1
    item = 1
    
    items_before = [int(i[:-2]) for i in r_79.lrange("latest:"+user, 0, 9)]
    ### TODO:应该把用户信息存在redis中
    city = jdata_user[jdata_user['user_id']==int(user)].city.values[0]
    age = jdata_user[jdata_user['user_id']==int(user)].age.values[0]
    
    ### 得到cate_memo 和 SIM
    f = open('./model/cate_memo.pkl', 'rb')
    cate_memo = pickle.load(f)
    
    f = open('./model/brand_sim_metrix.pkl', 'rb')
    SIM_brand_list = pickle.load(f)
    
    ### 得到city和age的items candidates
    
    items_rtn_city = r_80.zrange('city:'+ user, 0, 10, desc=True)
    items_rtn_age = r_80.zrange('age:'+ user, 0, 10, desc=True)
    
    ### 得到hottest candidiates
    item_rtn_hot = r_80.zrange('hottest_one_day_before'+ user, 0, 10, desc=True)
    
    ### 得到CF的items candidates
    item_rtn_cf = items2items(items_before, 10)
    
    ### 整合items
    
    ### items2ffm
    
    ### to flink
    
    