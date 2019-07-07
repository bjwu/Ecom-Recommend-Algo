### 这里包含着所有召回recall的算法

import pandas as pd
import numpy as np
import math
import gc
import datetime
import redis

from collections import defaultdict

### Redis
r = redis.Redis(host='localhost', port=6380, db=0)

### 导入数据

# TODO:第一步应该换为hbase的导入
jdata_action_train = pd.read_csv('./data/jdata_action_sorted.csv', sep=',')  

jdata_comment = pd.read_csv('./raw/jdata_comment.csv',sep=',')
jdata_product = pd.read_csv('./raw/jdata_product.csv',sep=',')
jdata_shop = pd.read_csv('./raw/jdata_shop.csv',sep=',')
jdata_user = pd.read_csv('./raw/jdata_user.csv',sep=',')

def content_based(city = False, age = True, to_redis = False):
    
    if city:
        df = jdata_action_train[['user_id','sku_id','type','action_time']].merge(jdata_product,on='sku_id',how='left')
        df = df.merge(jdata_user[['user_id','age','city']], on='user_id', how='left')
        all_city = df.city.unique()
        tmp = df.groupby('city')['sku_id'].value_counts()
        if to_redis:
            for city in all_city:
                if not np.isnan(city):
                    r.zadd('city:'+str(int(city)), tmp[city][:20].to_dict())
    if age:
        all_age = df.age.unique()
        tmp = df.groupby('age')['sku_id'].value_counts()
        if ro_redis:
            for age in all_age:
                if not np.isnan(age):
                    r.zadd('age:'+str(int(age)), tmp[age][:20].to_dict())
                    
                    
                    
def hottest_based(num=10, to_redis = False):
    latest = datetime.datetime.strptime(df.action_time.max(),"%Y-%m-%d %H:%M:%S")
    one_day = datetime.timedelta(days=1)
    one_week = datetime.timedelta(days=7)
    one_month = datetime.timedelta(days=30)
    
    one_day_before = latest - one_day
    one_week_before = latest - one_week
    one_month_before = latest - one_month
    
    hottest_one_day_before = df[df['action_time'] > one_day_before.strftime("%Y-%m-%d %H:%M:%S")].sku_id.value_counts()[:num].to_dict()
    hottest_one_week_before = df[df['action_time'] > one_week_before.strftime("%Y-%m-%d %H:%M:%S")].sku_id.value_counts()[:num].to_dict()
    hottest_one_month_before = df[df['action_time'] > one_month_before.strftime("%Y-%m-%d %H:%M:%S")].sku_id.value_counts()[:num].to_dict()
        
    if to_redis:
        r.zadd('hottest_one_day_before', hottest_one_day_before)
        r.zadd('hottest_one_week_before', hottest_one_week_before)
        r.zadd('hottest_one_month_before', hottest_one_month_before)
        
    