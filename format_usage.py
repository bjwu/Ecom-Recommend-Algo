import pandas as pd
import datetime


def df2libffm(df, save_file=None, y=None, non_categorical_cols = []) :
    """
    df: data source, should be pandas dataframe
    save_file: save file name, if None, will skip saving action
    y: label columns name in the pandas dataframe
    non_categorical_cols: columns in this list will not be one hot encoder. 
       example: 
           price : 0.99$
           click rate: 0.03
    """
    assert y is not None

    row_cnt = df.shape[0]

    out = pd.DataFrame({
        y: df[y]
    })

    print("out.shape:", out.shape )
    df = df.drop(columns=[y], axis=1)

    feature_base = 0
    
    cate_memo = {}

    for idx, col in enumerate(df.columns.tolist()) :
        dt = datetime.datetime.now()
        print( str(dt), idx, col)

        cur_field_id = idx
        field_series = pd.Series([cur_field_id] * row_cnt).astype(str)

        if col in non_categorical_cols :
            # if not categorical feature, do not consider how many different values
            feature_series = pd.Series([feature_base] * row_cnt).astype(str)
            feature_base += 1
            value_series = df[col].astype(str)

            new_col = field_series + ":" + feature_series + ":" + value_series
            out[str(cur_field_id)] = new_col.values
        else :
            # if is categorical feature
            ## IMPORTANT: 加入categorical dtype，方便之后的数据转化
            cate_memo[col] = {}
            cate_memo[col]['base'] = feature_base
            cate_memo[col]['dytpe'] = df[col].astype('category').dtypes
            ### 使用s.astype(cate_type)进行转换
            
            ## 为每一个category特征编号
            feature_series = df[col].astype('category').values.codes  
            ## 编号加上base（前一列的最大编号）
            feature_series = feature_series + feature_base
            feature_series = pd.Series(feature_series).astype(str)
            ## 更新base
            feature_base += feature_series.unique().shape[0]
            print("next feature base:", feature_base)
            
            new_col = field_series + ":" + feature_series + ":1" 
            out[str(cur_field_id)] = new_col.values
            

    if save_file:
        file_name = save_file
        if not file_name.endswith(".txt"):
            file_name += ".txt"
        print("save file name:", file_name)
        out.to_csv(file_name, sep=" ", header=False, index=False)
        return cate_memo
    else:
        return out, cate_memo


# Usage Sample
# data = pd.read_parquet("xxx.parquet") # your data

# df_ffm = df2libffm(data, y="click", save_file="data/train_int.ffm", non_categorical_cols=['hour'])

