import pandas as pd
import numpy as np
import xlearn as xl
import pickle
import gc
import redis

from format_usage import df2libffm

### redis

r = redis.Redis(host='localhost', port=6380, db=0)

### 导入数据

# TODO:第一步应该换为hbase的导入
jdata_action_train = pd.read_csv('./data/jdata_action_sorted.csv', sep=',')  

jdata_comment = pd.read_csv('./raw/jdata_comment.csv',sep=',')
jdata_product = pd.read_csv('./raw/jdata_product.csv',sep=',')
jdata_shop = pd.read_csv('./raw/jdata_shop.csv',sep=',')
jdata_user = pd.read_csv('./raw/jdata_user.csv',sep=',')


### 建立正负样本
df_train = jdata_action_train.groupby(['user_id','sku_id'])['type'].count()
df_train = df_train.reset_index().rename(columns={'type':'cnt'})
df_train['label'] = df_train['cnt'].map(lambda x: 1 if x > 1 else 0)
df_train = df_train.merge(jdata_product,on='sku_id',how='left')
df_train = df_train.merge(jdata_user,on='user_id',how='left')
df_train = df_train.fillna(0)  #为了能将nan转换为int

### 优化内存
cols_int8 = ['label','age','sex','city_level']
cols_int16 = ['city','cate','province']
cols_int32 = ['county']
cols_int64 = ['shop_id','brand','user_id','sku_id']
for col in cols_int8:
    df_train[[col]] = df_train[[col]].astype('int8', errors='ignore')
for col in cols_int16:
    df_train[[col]] = df_train[[col]].astype('int16', errors='ignore')
for col in cols_int32:
    df_train[[col]] = df_train[[col]].astype('int32', errors='ignore')
for col in cols_int64:
    df_train[[col]] = df_train[[col]].astype('int64', errors='ignore')
    
### 转换格式，拿到cate_type
cate_type = df2libffm(df_train, y='label', save_file='./data/train_ffm.txt')
# 保存宝贵的cate_type 的mapper
with open('./model/cate_type.pkl', 'wb') as f:
    pickle.dump(cate_type, f)
    
del df_train
del jdata_action_train
gc.collect()

### model
fm_model = xl.create_fm()
fm_model.setTrain('./data/train_ffm.txt')
param = {'task':'binary', 
         'lr':0.2, 
         'lambda':0.002, 
         'metric':'auc',
         'opt': 'adagrad',
         'k': 16   #隐向量长度
        }
fm_model.setTXTModel("./model/model.txt")   # 这句话要放在fit之前
fm_model.fit(param, "./model/model.out")
# fm_model.setSigmoid()  # or ffm_model.setSign()

### 将离线模型导入到redis中
with open('./model/model.txt', 'r') as f:
    for i, line in enumerate(f.readlines()):
        key, vec = line.split(':')
        if i % 1000000 == 0:
            r.hset('fm_weights', key, vec[1:-1])
            print('successfully insert {} to redis'.format(i))
