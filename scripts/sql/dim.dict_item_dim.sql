--drop DICTIONARY  dim.dict_item_dim on cluster cluster;
CREATE DICTIONARY dim.dict_item_dim on cluster cluster (
 item_id UInt64 ,
 type_id UInt32 default 0,
 price UInt32 default 0
) PRIMARY KEY item_id 
SOURCE(
  CLICKHOUSE(
    HOST 'localhost' PORT 9000 USER 'default' PASSWORD '' DB 'dim' TABLE 'item_dim_dis'
  )
) LIFETIME(MIN 1800 MAX 3600) LAYOUT(HASHED())
 
--使用
--dictGet('dim.dict_item_dim', 'type_id',toUInt64(item_id))
 