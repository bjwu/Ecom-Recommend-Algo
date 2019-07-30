import xlearn as xl

lambs = [0.0002, 0.002, 0.2]
ks = [16, 64]

for lamb in lambs:
    for k in ks:
        
        fm_model = xl.create_fm()

        fm_model.setTrain('./data/train_ffm_model.txt')
        fm_model.setValidate("./data/test_ffm_model.txt")
        param = {'task':'binary', 
                 'lambda': lamb, 
                 'metric':'auc',
                 'opt': 'adagrad',
                 'k': k   #隐向量长度
                }
        print('FM Model---[lambda:{}] [k:{}]'.format(lamb, k))
        fm_model.fit(param, "./model/model_fm.out")