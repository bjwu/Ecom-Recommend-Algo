### 这里包含着所有召回recall的算法
import pandas as pd
import numpy as np
import math
import gc
import datetime
import redis
import pickle

from collections import defaultdict

### Redis
r = redis.Redis(host='localhost', port=6380, db=0)

### TODO: itemset & userset 传入redis

### 导入数据

class recall_functions:
    
    def __init__(self, data):
        self.df = data

    def content_based(self, num:int, City = False, Age = False, Cate=False, Brand=False, to_redis = True):
        if City:
            all_city = self.df.city.unique()
            tmp = self.df.groupby('city')['sku_id'].value_counts()
            if to_redis:
                for city in all_city:
                    if not np.isnan(city):
                        r.zadd('city:'+str(int(city)), tmp[city][:num].to_dict())
            print('city to redis success...')
        if Age:
            all_age = self.df.age.unique()
            tmp = self.df.groupby('age')['sku_id'].value_counts()
            if to_redis:
                for age in all_age:
                    if not np.isnan(age):
                        r.zadd('age:'+str(int(age)), tmp[age][:num].to_dict())
            print('age to redis success...')
        if Cate:
            all_cate = self.df.cate.unique()
            tmp = self.df.groupby('cate')['sku_id'].value_counts()
            if to_redis:
                for cate in all_cate:
                    if not np.isnan(cate):
                        r.zadd('cate:'+str(int(age)), tmp[cate][:num].to_dict())
            print('cate to redis success...')
        if Brand:
            all_brand = self.df.brand.unique()
            tmp = self.df.groupby('brand')['sku_id'].value_counts()
            if to_redis:
                for brand in all_brand:
                    if not np.isnan(brand):
                        r.zadd('brand:'+str(int(brand)), tmp[brand][:num].to_dict())
            print('brand to redis success...')            
                    
    def hottest_based(self, num=10, to_redis = False):
        latest = datetime.datetime.strptime(self.df.action_time.max(),"%Y-%m-%d %H:%M:%S")
        one_day = datetime.timedelta(days=1)
        one_week = datetime.timedelta(days=7)
        one_month = datetime.timedelta(days=30)

        one_day_before = latest - one_day
        one_week_before = latest - one_week
        one_month_before = latest - one_month

        hottest_one_day_before = self.df[self.df['action_time'] > one_day_before.strftime("%Y-%m-%d %H:%M:%S")].sku_id.value_counts()[:num].to_dict()
        hottest_one_week_before = self.df[self.df['action_time'] > one_week_before.strftime("%Y-%m-%d %H:%M:%S")].sku_id.value_counts()[:num].to_dict()
        hottest_one_month_before = self.df[self.df['action_time'] > one_month_before.strftime("%Y-%m-%d %H:%M:%S")].sku_id.value_counts()[:num].to_dict()

        if to_redis:
            r.zadd('hottest_one_day_before', hottest_one_day_before)
            r.zadd('hottest_one_week_before', hottest_one_week_before)
            r.zadd('hottest_one_month_before', hottest_one_month_before)
            print('hottest to redis success...')
        
        
    def brandCF(self, to_pkl=False):
        print('brandCF begins...')
        # 筛选出这两个月内购买物品超过5的用户，使得矩阵不那么稀疏
        data_user = pd.DataFrame(self.df.groupby('user_id').count()['sku_id'].sort_values(ascending=False))
        data_user.columns = ['item_cnts']
        data_user.reset_index(inplace=True)

        # 按照以上筛选建立新的self.df
        tmp = data_user[data_user['item_cnts']>=5]
        df_new = self.df.merge(tmp, on='user_id',how='inner').sort_values(by='action_time')[['user_id','sku_id','brand','cate','type']]
        del tmp 
        gc.collect()

        print('give every rating a artificail weight...')
        # 给每个行为赋其rating权重
        df_new['score'] = df_new['type'].map({1:0.1, 2:0.5, 3:0.2, 4:0.3, 5:0.4})

        # 建立最终的训练dataframe: BRAND
        df_train_brand = pd.DataFrame(df_new.groupby(['user_id','brand']).score.sum()).reset_index()
        df_train_brand[['brand']] = df_train_brand[['brand']].astype('int32', errors='ignore')
        train_brand = df_train_brand.groupby('brand')['user_id'].apply(set).to_dict()
        # Brand_similarity matrix
#         SIM_brand_list = defaultdict(lambda : np.zeros(12000))
        SIM_brand_list = dict()
        cnt = 0
        train_brand_items = train_brand.items()
        print('begin building similarity metrix...')
        for brand1, users1 in train_brand_items:
            cnt +=1 
            if cnt % 2000 == 0:
                print('Finished {} brands..'.format(cnt))
            SIM_brand_list[brand1] = np.zeros(12000)
            for brand2, users2 in train_brand_items:
                if brand1 != brand2:
                # 避免生成与自己的相似度
                    sim = len(users1&users2)/math.sqrt(len(users1)*len(users2))
                    SIM_brand_list[brand1][brand2] = sim
        ### 返回brand sim矩阵
        if to_pkl:
            with open('./model/brand_sim_metrix.pkl', 'wb') as f:
                pickle.dump(SIM_brand_list, f)
            print('Save CF similarity metrix success...')
        return None
    