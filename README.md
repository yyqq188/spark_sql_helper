### 以sparksql的方式写流式作业的助手框架
#### 1.如果想查看kafka的原始数据信息

在reader中添加新的键 isShow ,并将该键值设为true.
```json
"reader": {
    "isShow": "true", ✅
    "name": "kafkaRowReader",
    "topic": ["T_PREM_ARAP"],
    "parameter": {
    "bootstrap.servers": "nod1.newchinalife.com:9092,nod3.newchinalife.com:9092,nod4.newchinalife.com:9092",
    "group.id": "aaa",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": "false"
    }
},
```
如果不想查看kafka的原始值的话，删除isShow的键,或将isShow设为false
#### 2.如果想查看解析之后的数据
在transformer的sql组件中,添加新的键 isShow ,并将该键值设为true.
tableName可以自由指定名称,但必须指定
sql语句可以写select {指定的列值} from {你指定的表名} ,或 select * from {你指定的表名}
```json
"transformer": [
    {
        "isShow": "true",✅
        "name": "sql",
        "parameter": {
            "tableName": "T",
            "sqlStr": "select BUSI_PROD_CODE,BUSI_PROD_NAME,DERIV_TYPE,case DERIV_TYPE when '003' then '003333' else 'other' end as myfield from T"
        }
    }
]
```
如果不想查看的话，删除isShow的键,或将isShow设为false

