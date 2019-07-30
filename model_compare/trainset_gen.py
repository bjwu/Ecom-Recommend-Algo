"""
此文件生成通往Ranking模型的训练集，正样本为Jdata数据集，负样本为随机产生
"""

import random
import pandas as pd
from tqdm import tqdm

jdata_action_train = pd.read_csv('./data/jdata_action_sorted.csv', sep=',')

# 生成所有user和items集合
users = jdata_action_train.user_id.unique()
items = list(jdata_action_train.sku_id.unique())

# user的总点击次数
usr_cnt = jdata_action_train.user_id.value_counts().to_dict()
# user点击的items情况
usr_item = jdata_action_train.groupby('user_id')['sku_id'].apply(set)

# 产生的负样本log
logs = []

for u in users:
    # 在总的itemsset中选择跟user点击数量正相关的items（且带一点随机性）
    candidates = set(random.sample(items, usr_cnt[u] + random.randint(-usr_cnt[u]//10, usr_cnt[u]//10+2)))
    # 去除user已经点过的items
    shoots = candidates.difference(set(usr_item[u]))
    for item in shoots:
        logs.append([u, item])
        
neg_df = pd.DataFrame(logs, columns=['user_id','sku_id'])
# 完全打乱
neg_df_shuffle = neg_df.sample(frac=1).reset_index(drop=True)

jdata_action_train = pd.read_hdf('./data/action_train.h5', key='df', mode='r')
jdata_action_test1 = pd.read_hdf('./data/action_test1.h5', key='df', mode='r')
jdata_action_test2 = pd.read_hdf('./data/action_test2.h5', key='df', mode='r')
jdata_action_test3 = pd.read_hdf('./data/action_test3.h5', key='df', mode='r')
jdata_action_test4 = pd.read_hdf('./data/action_test4.h5', key='df', mode='r')
jdata_action_test5 = pd.read_hdf('./data/action_test5.h5', key='df', mode='r')


neg_df_shuffle['label'] = 0
jdata_action_train['label'] = 1
jdata_action_test1['label'] = 1
jdata_action_test2['label'] = 1
jdata_action_test3['label'] = 1
jdata_action_test4['label'] = 1
jdata_action_test5['label'] = 1

# 前3000w个给训练集，剩余的给测试集
jdata_action_train_with_neg = pd.concat([jdata_action_train[['user_id','sku_id','label']], neg_df_shuffle[:30000000]], axis = 0)
jdata_action_train_with_neg.to_hdf('./data/action_train_with_neg.h5', key='df', mode='w')

jdata_action_test_with_neg = pd.DataFrame(columns=['user_id','sku_id','label'])
jdata_action_test_with_neg = pd.concat([jdata_action_test_with_neg, jdata_action_test1[['user_id','sku_id','label']]], axis = 0)
jdata_action_test_with_neg = pd.concat([jdata_action_test_with_neg, jdata_action_test2[['user_id','sku_id','label']]], axis = 0)
jdata_action_test_with_neg = pd.concat([jdata_action_test_with_neg, jdata_action_test3[['user_id','sku_id','label']]], axis = 0)
jdata_action_test_with_neg = pd.concat([jdata_action_test_with_neg, jdata_action_test4[['user_id','sku_id','label']]], axis = 0)
jdata_action_test_with_neg = pd.concat([jdata_action_test_with_neg, jdata_action_test5[['user_id','sku_id','label']]], axis = 0)
jdata_action_test_with_neg = pd.concat([jdata_action_test_with_neg, neg_df_shuffle[30000000:]], axis = 0)                   
jdata_action_test_with_neg.to_hdf('./data/action_test_with_neg.h5', key='df', mode='w')