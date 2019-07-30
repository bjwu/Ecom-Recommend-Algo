import xlearn as xl

lambs = [0.0002, 0.002, 0.02]
ks = [4,6,8]

for lamb in lambs:
    for k in ks:
    
        ffm_model = xl.create_ffm()


        ffm_model.setTrain('./data/train_ffm_model.txt')
        ffm_model.setValidate("./data/test_ffm_model.txt")
        param = {'task':'binary', 
                 'lambda':lamb, 
                 'metric':'auc',
                 'opt': 'adagrad',
                  'k': k,   #隐向量长度
                }
        print('FFM Model---[lambda:{}] [k:{}]'.format(lamb, k))
        ffm_model.fit(param, "./model/model_ffm.out")
