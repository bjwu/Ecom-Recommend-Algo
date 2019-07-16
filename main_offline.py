### 这里是离线模型的主进程
import pandas as pd
import numpy as np
import pickle
import gc
import redis
import xlearn as xl

from format_usage import df2libffm
from recall import recall_functions

### redis
r = redis.Redis(host='localhost', port=6380, db=0)

############################## 导入数据 ###########################

# TODO:第一步应该换为hbase的导入
jdata_action_train = pd.read_csv('./data/action_train.csv', sep=',')  

jdata_comment = pd.read_csv('./raw/jdata_comment.csv',sep=',')
jdata_product = pd.read_csv('./raw/jdata_product.csv',sep=',')
jdata_shop = pd.read_csv('./raw/jdata_shop.csv',sep=',')
jdata_user = pd.read_csv('./raw/jdata_user.csv',sep=',')
print('Data imported success...')

if False:

    ### 建立正负样本
    df_train = jdata_action_train.groupby(['user_id','sku_id'])['type'].count()
    df_train = df_train.reset_index().rename(columns={'type':'cnt'})
    df_train['label'] = df_train['cnt'].map(lambda x: 1 if x > 1 else 0)
    df_train = df_train.merge(jdata_product,on='sku_id',how='left')
    df_train = df_train.merge(jdata_user,on='user_id',how='left')
    df_train = df_train.fillna(0)  #为了能将nan转换为int
    print('Building training sample success...')

    ### 优化内存
    print('Optimized memory....')
    cols_int8 = ['label','age','sex']
    cols_int16 = ['city','cate']
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

    
    
    ### 转换格式，拿到cate_memo
    valid_cols = ['label','age','sex','city','county','brand','shop_id','cate','user_id','sku_id']
    df_train = df_train[valid_cols]

    cate_memo = df2libffm(df_train, y='label', save_file='./data/train_ffm.txt')

    # 保存宝贵的cate_memo 的mapper
    with open('./model/cate_memo.pkl', 'wb') as f:
        pickle.dump(cate_memo, f)
    print('Save categorical memo success...')

    del df_train, cate_memo
    gc.collect()

############################## RECALL ###########################

if False:
    ### 新建recall实例

    df = jdata_action_train[['user_id','sku_id','type','action_time']].merge(jdata_product,on='sku_id',how='left')
    df = df.merge(jdata_user[['user_id','age','city']], on='user_id', how='left')
    print('raw data merge success...') 
    print(df.shape)

    del jdata_action_train
    gc.collect()

    R = recall_functions(df)

    ### 计算city和age的items candidates，并传入redis

    R.content_based(num=10, City = True, Age = True, Cate=True, Brand=True, to_redis = True)

    ### 计算hottest candidiates，并传入redis

    R.hottest_based(num=10, to_redis = True)

    ### 计算CF的sim metrix

    SIM = R.brandCF(to_pkl=True)

    del SIM, R
    gc.collect()

############################## FM MODEL ###########################

if False:

    ### 计算FM离线模型

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
    print('Model fitting success')
    # fm_model.setSigmoid()  # or ffm_model.setSign()

    ### 将离线模型导入到redis中
    print('Start insert model to redis...')
    with open('./model/model.txt', 'r') as f:
        for line in tqdm(f.readlines()):
            key, vec = line.split(':')
            r.hset('fm_weights', key, vec[1:-1])
    print('Success insert model to redis!')
                




