--drop DICTIONARY dim.dict_user_dim on cluster cluster;
CREATE DICTIONARY dim.dict_user_dim on cluster cluster (
 uid UInt64 ,
 platform String default '' ,
 country String default '' ,
 province String default '' ,
 isp String default '' ,
 app_version String default '' ,
 os_version String default '',
 mac String default '' ,
 ip String default '',
 gender String default '',
 age Int16 default -1
) 
PRIMARY KEY uid 
SOURCE(
  CLICKHOUSE(
    HOST 'localhost' PORT 9000 USER 'default' PASSWORD '' DB 'dim' TABLE 'user_dim_dis'
  )
 ) LIFETIME(MIN 1800 MAX 3600) LAYOUT(HASHED())
 
--使用
--dictGet('dim.dict_user_dim', 'platform',toUInt64(uid))
 