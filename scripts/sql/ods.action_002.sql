--drop table ods.action_002_local on cluster cluster;
create table ods.action_002_local on cluster cluster (
day Date default toDate(second) comment '数据分区-天(Date)'
,hour DateTime default toStartOfHour(second) comment '数据时间-小时(DateTime)'
,second DateTime default '1970-01-01 08:00:00' comment '数据时间-秒'
,insert_second DateTime default now() comment '数据写入时间'
,platform String default '' comment '平台 android/ios'
,ip String default '' comment 'client-ip'
,isp String default '' comment '运营商'
,uid UInt32 default 0 comment 'uid'
,ver String default '' comment '版本'
,item_id UInt32 default 0 comment '商品id'
,action_a_cnt UInt32 default 0 comment 'actionA次数'
,action_b_cnt UInt32 default 0 comment 'actionB次数'
,action_c_cnt UInt32 default 0 comment 'actionC次数'
,action_a_time UInt32 default 0 comment 'actionA时间'
,action_b_time UInt32 default 0 comment 'actionA时间'
,action_c_time UInt32 default 0 comment 'actionA时间'
,action_d_sum UInt32 default 0 comment 'action_d_sum'
,action_e_sum UInt32 default 0 comment 'action_e_sum'
,action_f_sum UInt32 default 0 comment 'action_f_sum'
)
engine=ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/ods.action_002_local','{replica}')
PARTITION BY day
PRIMARY KEY (day,hour)
ORDER BY (day,hour,platform,item_id)
TTL day + toIntervalDay(10) + toIntervalHour(4)
SETTINGS index_granularity = 8192
;
--drop table dws.action_002 on cluster cluster;
create table dws.action_002 on cluster cluster
as ods.action_002_local
engine=Distributed(cluster,ods,action_002_local,rand());