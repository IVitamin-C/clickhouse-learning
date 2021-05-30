create table ods.user_dim_local on cluster cluster 
(
 day Date comment '数据分区-天',
 uid UInt32 default 0 comment 'uid',
 platform String default '' comment '平台 android/ios',
 country String default '' comment '国家',
 province String default '' comment '省及直辖市',
 isp String default '' comment '运营商',
 app_version String default '' comment '应用版本',
 os_version String default '' comment '系统版本',
 mac String default '' comment 'mac',
 ip String default '' comment 'ip',
 gender String default '' comment '性别',
 age Int16 default -1 comment '年龄'
)
engine = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/ods.user_dim_local','{replica}')
PARTITION BY day
PRIMARY KEY day
ORDER BY day
TTL day + toIntervalDay(3) + toIntervalHour(3)
SETTINGS index_granularity = 8192

--drop table dim.user_dim_dis on cluster cluster;
create table dim.user_dim_dis on cluster cluster
as ods.user_dim_local
engine=Distributed(cluster,ods,user_dim_local,rand());