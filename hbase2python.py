import happybase
import pandas as pd

def HBase2Python(limitation = None,reverse=True,start_row=None, stop_row=None,size_batch=1000):
    # 建立python和hbase的连接
    connection = None

    def hbase_connect(table_name):
        connection = happybase.Connection('localhost', autoconnect=False)
        connection.open()
        table = connection.table(table_name)
        return table
    try:
    
        table = hbase_connect('action_train_')
        data = table.scan(columns=['info:it','info:ty'],  limit=limitation
                          ,row_start=start_row, row_stop=stop_row,batch_size=size_batch,reverse=reverse)
        my_generator = ([int(k.decode('utf-8').split(',')[1]),int(v[b'info:it']), int(v[b'info:ty'])] for k, v in data)
#         my_generator = ([k.decode('utf-8').split(',')[0],int(k.decode('utf-8').split(',')[1]),int(v[b'info:it']), int(v[b'info:ty'])] for k, v in data)

#         这一步生成列表可能会有问题，生成器能直接转换成 dataframe 吗？-->可以，如下所示
#         my_list = list(my_generator)

#         print(my_list)


        # my_data 就是离线数据的输入源
#         my_data = pd.DataFrame.from_records(my_generator,columns = ['timestamp','userid','itemid','type'])
        my_data = pd.DataFrame.from_records(my_generator,columns = ['userid','itemid','type'])
#         my_data.columns = ['userid','itemid','type']
        my_data[['userid']] = my_data[['userid']].astype('int32')
        my_data[['itemid']] = my_data[['itemid']].astype('int32')
        my_data[['type']] = my_data[['type']].astype('int8')
#         print(my_data.dtypes)


    finally:
        if connection:
            connection.close()
            
    return my_data

if __name__ == '__main__':
#     HBase2Python(10).dtypes
    data =  HBase2Python(10)
    print(data)


