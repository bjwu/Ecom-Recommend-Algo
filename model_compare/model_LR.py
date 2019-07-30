import xlearn as xl

lambs = [0.0002, 0.002, 0.02, 0.2]

for lamb in lambs:
    
    lr_model = xl.create_linear()


    lr_model.setTrain('./data/train_ffm_model.txt')
    lr_model.setValidate("./data/test_ffm_model.txt")
    param = {'task':'binary', 
             'lambda':lamb, 
             'metric':'auc',
             'opt': 'adagrad',
            }
    print('LR Model---[lambda:{}]'.format(lamb))
    lr_model.fit(param, "./model/model_lr.out")

