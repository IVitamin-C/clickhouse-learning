create table ods.item_dim_local on cluster cluster 
(
 day Date comment '数据分区-天',
 item_id UInt32 default 0 comment 'item_id',
 type_id UInt32 default 0 comment 'type_id',
 price UInt32 default 0 comment 'price'
)
engine = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/ods.item_dim_local','{replica}')
PARTITION BY day
PRIMARY KEY day
ORDER BY day
TTL day + toIntervalDay(3) + toIntervalHour(3)
SETTINGS index_granularity = 8192

--drop table dim.item_dim_dis on cluster cluster;
create table dim.item_dim_dis on cluster cluster
as ods.item_dim_local
engine=Distributed(cluster,ods,item_dim_local,rand());