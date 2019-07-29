### dataset

行为表:jdata_action

用户表:jdata_user

店铺表:jdata_shop

商品表::jdata_product

评论表:jdata_comment


### 离线模型

离线模型由flink调度，根据数据集中的timestamp，按照事件时间，两小时触发一次离线模型。模型过程首先从hbase里拿取从当前至前两个月的全量用户log，然后进行所有的l离线过程，整个离线模型运行的时间会有点长。在整个过程中内存，redis里的数据都会相应更新，供接下来的在线模型使用。

离线模型步骤如下：

1. 从Hbase从拿取从当前至前两个月的全量用户log，生成迄今为止的全量用户集和物品集，用来判断之后用户是否为新用户，是否需要冷启动。这个过程将集合储存在内存中。
2. Join log和元数据，建立正负样本，用作排序模型的输入。在这个过程中，需要优化整个数据，因为设备内存有限。优化方法为：根据特征的类型的个数，进行数据类型的优化。同样，因为内存有限，最后我们只选取了`['age','sex','city','county','brand','shop_id','cate','user_id','sku_id']`这9个特征作为模型的输入。
3. 转换格式，由于FM的输入需要特定的libffm格式，所以需要将dataframe转换成libffm格式。转换后，需要保存相应的category memo，用作新来的log进行相应映射的转换。这个memo保存在内存中，方便实时调用。
4. 加下来是召回过程，我们的系统主要采用了三种召回方法：item-item CF，计算每个item对应的向量，然后将每个user最喜欢的20个items放入redis中。（另外一种，将item对应的向量放入redis中，当用户来时，实时计算用户最喜欢的物品）。content-based：我们选取了city和age作为筛选的特征，将不同city和age的用户所喜欢的k个items放入redis中。Hottest：将不同时间段（1月，1周，1天）内最火的商品集合返回到redis中。
5. FM模型训练。模型训练完成后将该模型的参数保存到redis中。如下图，将参数名设为key，参数值为value保存在redis中。
模型参数选择：![](http://ww1.sinaimg.cn/large/006tNc79gy1g5fuenvdv7j307d03qaa8.jpg)

### user log来时的判断（在线模型）

当一个user log通过tcp socket从flink传来的时候，一个新的线程将启动，线程将进行如下过程：

1. 通过redis返回user前10次点击过的items。Flink在接受新log的时候，不仅将log传入到Hbase全量日志中，也会将log传到以用户为key的Redis中，数据结构为list。
2. 取得该用户的city和age，返回该city和age对应的topN的items candidates。
3. 得到当前hottest的item candidates
4. 通过1，得到协同过滤后的items candidates。通过1中的10个latest items，根据协同过滤的物品相似度矩阵，将与每个item相似的向量按照时间衰减加权相加，最后得到最终的用户物品向量，返回该向量中数值最大的top物品作为candidates。
5. 整合2，3，4中的items。
6. 利用之前的category memo, 生成相应的items2ffm向量。
7. 将向量通过kafka传给flink。由于libffm格式数据量比较大，且每行格式相同，这里不在适用socket方式传递，而是适用Kafka作为传递工具。



