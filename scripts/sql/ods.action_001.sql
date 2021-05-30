--drop table ods.action_001_local on cluster cluster;
create table ods.action_001_local on cluster cluster (
day Date default toDate(second) comment '数据分区-天(Date)'
,hour DateTime default toStartOfHour(second) comment '数据时间-小时(DateTime)'
,second DateTime default '1970-01-01 08:00:00' comment '数据时间-秒'
,insert_second DateTime default now() comment '数据写入时间'
,ip String default '' comment 'client-ip'
,isp String default '' comment '运营商'
,uid UInt32 default 0 comment 'uid'
,ver String default '' comment '版本'
,item_id String default '' comment '商品id'
,show_cnt UInt32 default 0 comment '曝光次数'
,click_cnt UInt32 default 0 comment '点击次数'
,show_time UInt32 default 0 comment '曝光时间'
)
engine=ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/ods.action_001_local','{replica}')
PARTITION BY day
PRIMARY KEY (day,hour)
ORDER BY (day,hour,item_id)
TTL day + toIntervalDay(10) + toIntervalHour(4)
SETTINGS index_granularity = 8192
;
--drop table dws.action_001 on cluster cluster;
create table dws.action_001 on cluster cluster
as ods.action_001_local
engine=Distributed(cluster,ods,action_001_local,rand());