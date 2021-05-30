--drop table dwm.mainpage_stat_mv_local on cluster cluster;
create table dwm.mainpage_stat_mv_local on cluster cluster
(
day Date comment '数据分区-天'
,hour DateTime comment '数据时间-小时(DateTime)'
,platform String comment '平台 android/ios'
,ver String comment '版本'
,item_id UInt32 comment '物品id'
,shown_uv AggregateFunction(uniqCombined,UInt32) comment '曝光人数'
,shown_cnt SimpleAggregateFunction(sum,UInt64) comment '曝光次数'
,click_uv AggregateFunction(uniqCombined,UInt32) comment '点击人数'
,click_cnt SimpleAggregateFunction(sum,UInt64) comment '点击次数'
,show_time_sum  SimpleAggregateFunction(sum,UInt64) comment '总曝光时间/秒'
)
engine=ReplicatedAggregatingMergeTree('/clickhouse/tables/{layer}-{shard}/dwm.mainpage_stat_mv_local','{replica}')
PARTITION by day
PRIMARY KEY (day,hour)
ORDER by (day,hour,platform,ver,item_id)
TTL day + toIntervalDay(92) + toIntervalHour(5)
SETTINGS index_granularity = 8192

--drop table dws.mainpage_stat_mv_dis on cluster cluster
create table dws.mainpage_stat_mv_dis on cluster cluster
as dwm.mainpage_stat_mv_local
engine=Distributed(cluster,dwm,mainpage_stat_mv_local,rand());

--drop table dwm.mv_main_page_stat_mv_local on cluster cluster;
create  MATERIALIZED VIEW dwm.mv_main_page_stat_mv_local on cluster cluster to dwm.mainpage_stat_mv_local (
day Date comment '数据分区-天'
,hour DateTime comment '数据时间-小时(DateTime)'
,platform String comment '平台 android/ios'
,ver String comment '版本'
,item_id UInt32 comment '物品id'
,shown_uv AggregateFunction(uniqCombined,UInt32) comment '曝光人数'
,shown_cnt SimpleAggregateFunction(sum,UInt64) comment '曝光次数'
,click_uv AggregateFunction(uniqCombined,UInt32) comment '点击人数'
,click_cnt SimpleAggregateFunction(sum,UInt64) comment '点击次数'
,show_time_sum  SimpleAggregateFunction(sum,UInt64) comment '总曝光时间/秒'
)
AS 
explain ast SELECT day
     ,hour
     ,platform
     ,ver
     ,item_id
     ,uniqCombinedStateIf(uid,a.show_cnt>0) as shown_uv
     ,sum(a.show_cnt) as show_cnt
     ,uniqCombinedStateIf(uid,a.click_cnt>0) as click_uv
     ,sum(a.click_cnt) as click_cnt
     ,sum(toUInt64(show_time/1000)) as show_time_sum
from ods.action_001_local as a
group by
      day
     ,hour
     ,platform
     ,ver
     ,item_id

