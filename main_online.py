### 这里是在线模型的主进程
import pandas as pd
import numpy as np
import pickle
import gc
import redis

jdata_user = pd.read_csv('./raw/jdata_user.csv',sep=',')
jdata_product = pd.read_csv('./raw/jdata_product.csv',sep=',')

### redis
r = redis.Redis(host='localhost', port=6380, db=0)

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
        df_test[c] = df_test[c].astype(mapper[c]['dytpe']).astype(int) + mapper[c]['base']
    return df_test

if __name__ == '__main__':
    
    user = ''
    item = ''
    
    ### 得到cate_memo
    f = open('./model/cate_memo.pkl', 'rb')
    cate_memo = pickle.load(f)
    
    ### 得到city和age的items candidates
    
    
    ### 得到hottest candidiates
    
    
    ### 得到CF的items candidates
    
    
    ### items2ffm
    
    ### to flink
    
    