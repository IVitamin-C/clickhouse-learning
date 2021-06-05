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
SELECT day
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

--查询数据
SELECT day
     ,platform
     ,uniqCombinedMerge(shown_uv) as shown_uv
     ,sum(shown_cnt) as shown_cnt
     ,uniqCombinedMerge(click_uv) as click_uv
     ,sum(click_cnt) as click_cnt
     ,sum(show_time_sum) as show_time_sum
from dws.mainpage_stat_mv_dis
group by
      day
     ,platform


--新增维度和指标的ddl操作

--新增维度并添加到索引
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists gender String comment '性别' after item_id,modify order by 
(day,hour,platform,ver,item_id,gender);
alter table dwm.mainpage_stat_mv_local on cluster cluster modify column if exists gender String default '未知' comment '性别' after item_id;
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists gender String comment '性别' after item_id;


--新增指标
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists show_time_median AggregateFunction(medianExact,UInt32) comment '曝光时长中位数';

alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists show_time_median AggregateFunction(medianExact,UInt32) comment '曝光时长中位数';

drop TABLE dwm.mv_main_page_stat_mv_local on cluster cluster;
CREATE MATERIALIZED VIEW dwm.mv_main_page_stat_mv_local on cluster cluster to dwm.mainpage_stat_mv_local (
day Date comment '数据分区-天'
,hour DateTime comment '数据时间-小时(DateTime)'
,platform String comment '平台 android/ios'
,ver String comment '版本'
,item_id UInt32 comment '物品id'
,gender String  comment '性别'
,shown_uv AggregateFunction(uniqCombined,UInt32) comment '曝光人数'
,shown_cnt SimpleAggregateFunction(sum,UInt64) comment '曝光次数'
,click_uv AggregateFunction(uniqCombined,UInt32) comment '点击人数'
,click_cnt SimpleAggregateFunction(sum,UInt64) comment '点击次数'
,show_time_sum  SimpleAggregateFunction(sum,UInt64) comment '总曝光时间/秒'
,show_time_median AggregateFunction(medianExact,UInt32) comment '曝光时长中位数'
)
AS 
 SELECT day
     ,hour
     ,platform
     ,ver
     ,item_id
     ,dictGet('dim.dict_user_dim', 'gender',toUInt64(uid)) as gender
     ,uniqCombinedStateIf(uid,a.show_cnt>0) as shown_uv
     ,sum(a.show_cnt) as show_cnt
     ,uniqCombinedStateIf(uid,a.click_cnt>0) as click_uv
     ,sum(a.click_cnt) as click_cnt
     ,sum(toUInt64(show_time/1000)) as show_time_sum
     ,medianExactState(toUInt32(show_time/1000)) as show_time_median
from ods.action_001_local as a
group by
      day
     ,hour
     ,platform
     ,ver
     ,item_id
     ,gender



--实现物化视图宽表化逻辑

alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists acta_uv AggregateFunction(uniqCombined,UInt32) comment 'acta_uv';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists acta_cnt SimpleAggregateFunction(sum,UInt64) comment 'acta_cnt';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists actb_uv AggregateFunction(uniqCombined,UInt32) comment 'actb_uv';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists actb_cnt SimpleAggregateFunction(sum,UInt64) comment 'actb_cnt';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists actc_uv AggregateFunction(uniqCombined,UInt32) comment 'actc_uv';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists actc_cnt SimpleAggregateFunction(sum,UInt64) comment 'actc_cnt';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists show_bm AggregateFunction(groupBitmap,UInt32) comment 'show_bm';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists click_bm AggregateFunction(groupBitmap,UInt32) comment 'click_bm';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists acta_bm AggregateFunction(groupBitmap,UInt32) comment 'acta_bm';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists actb_bm AggregateFunction(groupBitmap,UInt32) comment 'actb_bm';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists actc_bm AggregateFunction(groupBitmap,UInt32) comment 'actc_bm';
alter table dwm.mainpage_stat_mv_local on cluster cluster add column if not exists actd_bm AggregateFunction(groupBitmap,UInt32) comment 'actd_bm';


alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists acta_uv AggregateFunction(uniqCombined,UInt32) comment 'acta_uv';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists acta_cnt SimpleAggregateFunction(sum,UInt64) comment 'acta_cnt';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists actb_uv AggregateFunction(uniqCombined,UInt32) comment 'actb_uv';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists actb_cnt SimpleAggregateFunction(sum,UInt64) comment 'actb_cnt';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists actc_uv AggregateFunction(uniqCombined,UInt32) comment 'actc_uv';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists actc_cnt SimpleAggregateFunction(sum,UInt64) comment 'actc_cnt';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists show_bm AggregateFunction(groupBitmap,UInt32) comment 'show_bm';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists click_bm AggregateFunction(groupBitmap,UInt32) comment 'click_bm';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists acta_bm AggregateFunction(groupBitmap,UInt32) comment 'acta_bm';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists actb_bm AggregateFunction(groupBitmap,UInt32) comment 'actb_bm';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists actc_bm AggregateFunction(groupBitmap,UInt32) comment 'actc_bm';
alter table dws.mainpage_stat_mv_dis on cluster cluster add column if not exists actd_bm AggregateFunction(groupBitmap,UInt32) comment 'actd_bm';


drop TABLE dwm.mv_main_page_stat_mv_local on cluster cluster;
CREATE MATERIALIZED VIEW dwm.mv_main_page_stat_mv_001_local on cluster cluster to dwm.mainpage_stat_mv_local (
day Date comment '数据分区-天'
,hour DateTime comment '数据时间-小时(DateTime)'
,platform String comment '平台 android/ios'
,ver String comment '版本'
,item_id UInt32 comment '物品id'
,gender String  comment '性别'
,shown_uv AggregateFunction(uniqCombined,UInt32) comment '曝光人数'
,shown_cnt SimpleAggregateFunction(sum,UInt64) comment '曝光次数'
,click_uv AggregateFunction(uniqCombined,UInt32) comment '点击人数'
,click_cnt SimpleAggregateFunction(sum,UInt64) comment '点击次数'
,show_time_sum  SimpleAggregateFunction(sum,UInt64) comment '总曝光时间/秒'
,show_bm AggregateFunction(groupBitmap,UInt32) comment 'show_bm'
,click_bm AggregateFunction(groupBitmap,UInt32) comment 'click_bm'
)
AS 
 SELECT day
     ,hour
     ,platform
     ,ver
     ,item_id
     ,dictGet('dim.dict_user_dim', 'gender',toUInt64(uid)) as gender
     ,uniqCombinedStateIf(uid,a.show_cnt>0) as shown_uv
     ,sum(a.show_cnt) as show_cnt
     ,uniqCombinedStateIf(uid,a.click_cnt>0) as click_uv
     ,sum(a.click_cnt) as click_cnt
     ,sum(toUInt64(show_time/1000)) as show_time_sum
     ,groupBitmapStateIf(uid,a.show_cnt>0) as show_bm
     ,groupBitmapStateIf(uid,a.click_cnt>0) as click_bm
from ods.action_001_local as a
group by
      day
     ,hour
     ,platform
     ,ver
     ,item_id
     ,gender

drop table dwm.mv_main_page_stat_mv_002_local on cluster cluster;
CREATE MATERIALIZED VIEW dwm.mv_main_page_stat_mv_002_local on cluster cluster to dwm.mainpage_stat_mv_local (
day Date comment '数据分区-天'
,hour DateTime comment '数据时间-小时(DateTime)'
,platform String comment '平台 android/ios'
,ver String comment '版本'
,item_id UInt32 comment '物品id'
,gender String  comment '性别'
,acta_uv AggregateFunction(uniqCombined,UInt32) comment 'acta_uv'
,acta_cnt SimpleAggregateFunction(sum,UInt64) comment 'acta_cnt'
,actb_uv AggregateFunction(uniqCombined,UInt32) comment 'actb_uv'
,actb_cnt SimpleAggregateFunction(sum,UInt64) comment 'actb_cnt'
,actc_uv AggregateFunction(uniqCombined,UInt32) comment 'actc_uv'
,actc_cnt SimpleAggregateFunction(sum,UInt64) comment 'actc_cnt'
,acta_bm AggregateFunction(groupBitmap,UInt32) comment 'acta_bm'
,actb_bm AggregateFunction(groupBitmap,UInt32) comment 'actb_bm'
,actc_bm AggregateFunction(groupBitmap,UInt32) comment 'actc_bm'
,actd_bm AggregateFunction(groupBitmap,UInt32) comment 'actd_bm'
)
AS 
 SELECT day
     ,hour
     ,platform
     ,ver
     ,item_id
     ,dictGet('dim.dict_user_dim', 'gender',toUInt64(uid)) as gender
     ,uniqCombinedStateIf(uid,a.action_a_cnt>0) as acta_uv
     ,sum(a.action_a_cnt) as acta_cnt
     ,uniqCombinedStateIf(uid,a.action_b_cnt>0) as actb_uv
     ,sum(a.action_b_cnt) as actb_cnt
     ,uniqCombinedStateIf(uid,a.action_c_cnt>0) as actc_uv
     ,sum(a.action_c_cnt) as actc_cnt
     ,groupBitmapStateIf(uid,a.action_a_cnt>0) as acta_bm
     ,groupBitmapStateIf(uid,a.action_b_cnt>0) as actb_bm
     ,groupBitmapStateIf(uid,a.action_c_cnt>0) as actc_bm
     ,groupBitmapStateIf(uid,a.action_d_sum>0) as actd_bm
from ods.action_002_local as a
group by
      day
     ,hour
     ,platform
     ,ver
     ,item_id
     ,gender



--查询
select day
     ,gender
     ,uniqCombinedMerge(shown_uv) as shown_uv
     ,uniqCombinedMerge(click_uv) as click_uv
     ,uniqCombinedMerge(acta_uv) as acta_uv 
     ,uniqCombinedMerge(actb_uv) as actb_uv 
     ,uniqCombinedMerge(actc_uv) as actc_uv 
from dws.mainpage_stat_mv_dis
where day='2021-06-06'
group by day,gender


select t1.day,t1.gender,shown_uv,click_uv,acta_uv,actb_uv,actc_uv
from (
 SELECT day
     ,dictGet('dim.dict_user_dim', 'gender',toUInt64(uid)) as gender
     ,uniqCombinedIf(uid,a.show_cnt>0) as shown_uv
     ,uniqCombinedIf(uid,a.click_cnt>0) as click_uv
from dws.action_001_dis as a 
where day='2021-06-06'
group by day,gender
) as t1 
left join (
      SELECT day
     ,dictGet('dim.dict_user_dim', 'gender',toUInt64(uid)) as gender
     ,uniqCombinedIf(uid,a.action_a_cnt>0) as acta_uv
     ,uniqCombinedIf(uid,a.action_b_cnt>0) as actb_uv
     ,uniqCombinedIf(uid,a.action_c_cnt>0) as actc_uv
from dws.action_002_dis as a
group by
      day
     ,gender
) as t2 
using day,gender


--
select day
     ,gender
     ,bitmapCardinality(groupBitmapMergeState(show_bm)) as shown_uv 
     ,bitmapAndCardinality(groupBitmapMergeState(show_bm),groupBitmapMergeState(click_bm)) as show_click_uv
     ,bitmapAndCardinality(groupBitmapMergeState(show_bm),bitmapAnd(groupBitmapMergeState(click_bm),groupBitmapMergeState(acta_bm))) as show_click_a_uv
     ,bitmapAndCardinality(groupBitmapMergeState(show_bm),bitmapAnd(bitmapAnd(groupBitmapMergeState(click_bm),groupBitmapMergeState(acta_bm)),groupBitmapMergeState(actb_bm))) as show_click_ab_uv
     ,bitmapAndCardinality(groupBitmapMergeState(show_bm),bitmapAnd(bitmapAnd(bitmapAnd(groupBitmapMergeState(click_bm),groupBitmapMergeState(acta_bm)),groupBitmapMergeState(actb_bm)),groupBitmapMergeState(actc_bm))) as show_click_abc_uv
     ,bitmapAndCardinality(groupBitmapMergeState(show_bm),bitmapAnd(bitmapAnd(bitmapAnd(bitmapAnd(groupBitmapMergeState(click_bm),groupBitmapMergeState(acta_bm)),groupBitmapMergeState(actb_bm)),groupBitmapMergeState(actc_bm)),groupBitmapMergeState(actd_bm))) as show_click_abcd_uv
from dws.mainpage_stat_mv_dis
where day='2021-06-06'
group by day,gender

