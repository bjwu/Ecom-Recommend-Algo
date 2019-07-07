import pandas as pd
import numpy as np
from collections import defaultdict

print('Loading all the data needed...')
jdata_action_test1 = pd.read_hdf('./data/action_test1.h5', key='df', mode='r')
jdata_action_test2 = pd.read_hdf('./data/action_test2.h5', key='df', mode='r')
jdata_action_test3 = pd.read_hdf('./data/action_test3.h5', key='df', mode='r')
jdata_action_test4 = pd.read_hdf('./data/action_test4.h5', key='df', mode='r')
jdata_action_test5 = pd.read_hdf('./data/action_test5.h5', key='df', mode='r')
jdata_product = pd.read_csv('./raw/jdata_product.csv',sep=',')
jdata_action_test = [jdata_action_test1,jdata_action_test2,jdata_action_test3,jdata_action_test4,jdata_action_test5]

class metrics:
    """
    args:
        period: 第几个test period，默认为1，即训练集为前两个月数据，测试集为接下来3天数据。
    """
    def __init__(self, period=1):
        self.train = None
        self.test = jdata_action_test[period-1]
        tmp = self.test[self.test['type']==2].merge(jdata_product,on='sku_id',how='left')  ## 仅考虑下单的客户
        tmp[['brand']] = tmp[['brand']].fillna(0).astype('int32')
        self.test = tmp.groupby('user_id')['brand'].apply(set).to_dict()
        print('Generate Testset successfully....')
        
    """
    args:
        func: recommendation function, 输入:(user->int, N->int)，表示为给user推荐的topN个东西。
                                       输出:(set(item->int))，如没有items推荐，请返回{}。
    """
    def PrecisonRecall(self, func, N):
        hit = 0
        n_recall = 0
        n_precision = 0
        for user, items in self.test.items():
            rank = func(user, N)
            hit += len(rank & items)
            n_recall += len(items)
            n_precision += N
        return [hit/(1.0*n_recall), hit/(1.0*n_precision)]