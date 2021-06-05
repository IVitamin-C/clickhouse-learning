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
 
--单value使用
--方法1:dictGet('dim.dict_user_dim', 'platform',toUInt64(uid))
select dictGet('dim.dict_user_dim', 'platform',toUInt64(uid)) as platform,uniqCombined(uid) as uv 
from dws.action_001_dis
where day='2021-06-05'
group by platform
--方法2:通过join
select t2.platform as platform,uniqCombined(t1.uid) as uv 
from dws.action_001_dis t1
join dim.dict_user_dim t2
on toUInt64(t1.uid)=t2.uid
where day='2021-06-05'
group by platform


--多value使用
select t2.platform as platform,t2.gender as gender,uniqCombined(t1.uid) as uv 
from dws.action_001_dis t1
join dim.dict_user_dim t2
on toUInt64(t1.uid)=t2.uid
where day='2021-06-05'
group by platform,gender



select dictGet('dim.dict_user_dim', 'platform',toUInt64(uid)) as platform,dictGet('dim.dict_user_dim', 'gender',toUInt64(uid)) as gender,uniqCombined(uid) as uv 
from dws.action_001_dis
where day='2021-06-05'
group by platform,gender