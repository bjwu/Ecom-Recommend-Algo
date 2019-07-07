### dataset

行为表:jdata_action

用户表:jdata_user

店铺表:jdata_shop

商品表::jdata_product

评论表:jdata_comment


### 安装redis

在虚拟机上，我的redis的port为6380而不是6379，所以启动redis为：

port为6379的被阿朱占领，我的所有数据皆放在port为6380的redis上
```bash
$ ./bin/redis-cli -p 6380
```

在python中，使用`redis-py`来操作redis，其函数名基本与redis的command的名称一致
```python
import redis
r = redis.Redis(host='localhost', port=6380, db=0)
```

### 建立Category Recall

```python
### 首先收集所有的城市名
all_city = df.city.unique()
### 按照城市名分类，这里没有时间区间。感觉还是要加一下
tmp = df.groupby('city')['sku_id'].value_counts()
### 上传到redis中
for city in all_city:
    if not np.isnan(city):
        r.zadd('city:'+str(int(city)), tmp[city][:20].to_dict())
```

### python 对时间处理

```python
import datetime

### 将字符串转化为datetime对象
latest = datetime.datetime.strptime('2019-07-04 00:00:00' ,"%Y-%m-%d %H:%M:%S")

### 定义时间段
one_day = datetime.timedelta(days=1)
one_week = datetime.timedelta(days=7)
one_month = datetime.timedelta(days=30)

### 相加或相减
one_day_before = latest - one_day
one_week_before = latest - one_week
one_month_before = latest - one_month

### datetime对象转换为字符串
one_day_before.strftime("%Y-%m-%d %H:%M:%S")
```

### 负样本生成

对于数据集来说，如果要应用FM算法，那么label将是一个二分类的问题，clicked or not。但是对于jdata的数据集，只有用户点击的数据，没有被观看但没有点击的数据，这样就要人为的生成负样本数据。

对于节目系统，有博客给了如下定义：

> 正样本可以定义为用户当天播放过的节目，也就是“喜欢”。负样本则有两种选择方案：
>（1）负样本指的是对用户曝光过的节目，但是用户至始至终都没有播放过，也就是说该节目并不在“历史”和“喜欢”两个分类里面。（2）负样本指的是在整个抽样的池子里面，但是用户至始至终都没有播放过，也就是说该节目并不在“历史”和“喜欢”这两个分类里面。

很明显，对于我们的电商系统，只能使用第二种人工生成方法。但是这样的随机性实在是太强了，所以最后我决定：将一个用户只点击过一次的商品看作负样本，点击过两次及以上的商品看作正样本。

### 模型评估

在评估模型效果的时候，用全量日志，且按照上述定义正负样本的方法建立训练集和测试集。

但是在实际推荐系统的时候，那就要将全量日志按照时间分开。


### dataframe转换为libffm格式

对于xlearn的输入，需要是libffm格式，其实也更加直观，这里采用了这个博客提供的一个方式：

https://www.flyml.net/2019/02/19/df2libffm/

### pandas的categorical类型

这个东西也是最近才发现的宝贝了，他可以将特征转化为categorical的类别
```python
df[col].astype('category')
```
然后有很多功能呀，比如，对于新来的数据，可以按照之前的类型映射进行保存
```python
cate_memo[col]['dytpe'] = df[col].astype('category').dtypes
```
具体操作见：
https://pandas.pydata.org/pandas-docs/stable/user_guide/categorical.html

### 离线模型

离线模型当然由flink触发我，首先从hbase里拿取前两个月的数据，然后进行所有的召回过程，这个过程可能得进行一会儿，这个时候会依次更新redis里的数据。

召回过程包括：

1. item-item CF， 计算每个item对应的向量，然后将每个user最喜欢的50个items放入redis中。（另外一种，将item对应的向量放入redis中，当用户来时，实时计算用户最喜欢的物品）
2. content-based：目前是city个age，分别将不同city和不同age对应的最火num个item放入redis中。
3. hottest：将最火的商品集合返回到redis中

### user log来时的判断（在线模型）

当一个user log来的时候，首先，我们要通过redis返回user前10次点击过的items，然后

### 查看python 对象内存

```python
foo.__sizeof__()
```


