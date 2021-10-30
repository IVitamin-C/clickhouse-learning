import codecs
import locale
import sys
import logging
import random
import time
import traceback
from optparse import OptionGroup, OptionParser
from typing import List, Tuple
from collections import deque
from operator import itemgetter
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from clickhouse_driver import Client, errors

if sys.version >'2' and sys.version >= '3.7':
    sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
elif sys.version >'2' and sys.version < '3.7':
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s: - %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class CHClient:
    def __init__(self, user: str = 'default', password: str = '', host: str = "", port: int = None, database: str = ''):
        """
        初始化clickhouse client.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.settings = {'receive_timeout': 1200, 'send_timeout': 1200}
        self.chcli = Client(host=self.host, port=self.port, database=self.database, user=self.user,
                            password=self.password, send_receive_timeout=1200, settings=self.settings)

    def execute(self, sql: str = None, with_column_types: bool = False) -> Tuple[int, List[Tuple]]:
        """
        :param sql: 待执行sql
        :param with_column_types:result[-1] 返回列的类型
        :return: result -> list<tuple>
        """
        try:
            logger.info(f'host:<{self.host}>: will execute sql :\n{sql}')
            res = self.chcli.execute(sql, with_column_types=with_column_types)
            return 0, res
        except Exception:
            err = traceback.format_exc()
            # part正在被merge或者mutation
            if 'Code: 384.' in err:
                logger.warning(f'host:<{self.host}>,ERROR:2: failed to execute sql:\n {sql},\nerr:\n {err}')
                return 2, [()]
            # part在fetch part前已经被merge或者drop，is not exists
            if 'Code: 234.' in err:
                logger.warning(f'host:<{self.host}>,ERROR:3: failed to execute sql:\n {sql},\nerr:\n {err}')
                return 3, [()]
            # detached part 不存在 No part xx in committed state.
            if 'Code: 232.' in err:
                logger.warning(f'host:<{self.host}>,ERROR:4: failed to execute sql:\n {sql},\nerr:\n {err}')
                return 4, [()]
            # 过滤掉重放是出现的删除drop detach part不存在的错误，污染log
            if 'Code: 233.' in err:
                return 5, [()]
            # part 过大，拉取超过python client 最大时间，触发超时，但是任务实际仍然在跑
            if 'socket.timeout: timed out' in err:
                logger.warning(f'host:<{self.host}>,ERROR:90: failed to execute sql:\n {sql},\nerr:\n {err}')
                return 90, [()]
            # 写锁，因为part还在拉取，如果执行detach时，因为写锁，无法正常detach
            if 'Code: 473.' in err:
                logger.warning(f'host:<{self.host}>,ERROR:91: failed to execute sql:\n {sql},\nerr:\n {err}')
                return 91, [()]
            # zk会话过期，命令提交zk，可能失败也可能成功
            if 'Code: 999.' in err and 'Session expired' in err:
                logger.warning(f'host:<{self.host}>,ERROR:92: failed to execute sql:\n {sql},\nerr:\n {err}')
                return 92, [()]
            # zk node 连接丢失，和会话过期场景类似，命令提交zk，可能失败也可能成功
            if 'Code: 999.' in err and 'Connection loss' in err:
                logger.warning(f'host:<{self.host}>,ERROR:93: failed to execute sql:\n {sql},\nerr:\n {err}')
                return 93, [()]
            # 副本状态不一致，触发不能执行drop和detach part，
            if 'Code: 529.' in err and 'DROP PART|PARTITION cannot be done on this replica because it is not a leader' in err:
                logger.warning(f'host:<{self.host}>,ERROR:94: failed execute sql:\n {sql},\nerr:\n {err}')
                return 94, [()]
            # code 16 主要表现为一些列的操作有问题，导致历史part和现在的表结构不一致，一旦part detach后就不能attach，
            # 属于特别极端，且难通过程序恢复的错误，需要人为恢复，恢复方式是解决列问题后，重新执行最后输出的attach_sql_list中的命令。
            if 'Code: 16.' in err:
                logger.error(f'host:<{self.host}>,ERROR:999: failed to execute sql:\n {sql},\nerr:\n {err}')
                return 999, [()]
            logger.error(f"host:<{self.host}>,ERROR:1: failed to execute sql:\n {sql},\nerr:\n {err}")
            return 1, [()]

    def exclose(self):
        """
        关闭clickhouse连接
        :return:
        """
        self.chcli.disconnect()

    def ping(self) -> Client:
        """
        执行 select 1,如果失败，将会重新初始化client
        :return:
        """
        try:
            self.chcli.execute('select 1')
        except (errors.NetworkError, errors.ServerException):
            self.chcli = Client(host=self.host, port=self.port, database=self.database, user=self.user,
                                password=self.password)
        return self.chcli


class CHSQL:
    # 获得节点ip列表
    get_nodes = """select shard_num,host_address
        from system.clusters
        where cluster='{cluster}'
        order by shard_num,replica_num"""
    # 获得待均衡分区
    get_partitions = """select partition
    from (
        SELECT
            a.shard,
            a.partition,
            sum(b.partition_bytes) AS partition_bytes
        FROM (
            select t1.shard
                ,t2.partition
            from (
                select hostName() AS shard from clusterAllReplicas('{cluster}', system, one)
            )t1
            cross join (
                select distinct partition
                from clusterAllReplicas('{cluster}', system, parts)
                WHERE (database = '{database}') AND (table = '{table}')
                    AND (toDate(parseDateTimeBestEffortOrZero(toString(partition))) <= (today() - 7))
                    AND (bytes_on_disk > ((100 * 1024) * 1024))
                    AND disk_name<>'hdfs'
                group by partition
            )t2
        )a
        left join(
            select hostName() as shard
                ,partition
                ,sum(toUInt32(bytes_on_disk/1024/1024)) AS partition_bytes
            from clusterAllReplicas('{cluster}', system, parts)
            WHERE (database = '{database}') AND (table = '{table}')
                AND (toDate(parseDateTimeBestEffortOrZero(toString(partition))) <= (today() - 7))
                AND (bytes_on_disk > ((100 * 1024) * 1024))
                AND disk_name<>'hdfs'
            group by shard,partition
        )b
        on a.shard=b.shard and a.partition=b.partition
        group by a.shard,
            a.partition
    )
    GROUP BY partition
    HAVING (min(partition_bytes) <= (avg(partition_bytes) * {low_rate}))
    and (max(partition_bytes) >= (avg(partition_bytes) * {high_rate}))
    order by partition desc"""
    # 获得part的列表，并排除掉小于100M的part
    get_parts = """select _shard_num
        ,name as part_name
        ,rows
        ,toUInt32(bytes_on_disk/1024/1024) as bytes_on_disk
        ,disk_name
    from cluster('{cluster}',system,parts)
    where database='{database}'
        and table='{table}'
        and partition='{partition}'
        and bytes_on_disk>100
        and disk_name<>'hdfs'"""
    # 借助zk，fetch part
    fetch_part = """ALTER TABLE {database}.{table} FETCH PART '{part_name}' FROM '/clickhouse/tables/{layer}-{shard}/{database}.{table}'"""

    # 设置参数允许删除detached part
    set_drop_detached = """set allow_drop_detached = 1"""
    # 删除detached part
    drop_detach_part = """ALTER TABLE {database}.{table} DROP DETACHED PART '{part_name}'"""
    # attach part 这个是复制的，在一个节点执行，另一个节点会在detached 目录找一致的part，如果有，则挂载，没有从另外一个节点拉取
    attach_part = """ALTER TABLE {database}.{table} ATTACH PART '{part_name}'"""
    # detach part 这个是复制的，将一个active part detach，part将不会被查询
    detach_part = """ALTER TABLE {database}.{table} DETACH PART '{part_name}'"""
    # 获得layer宏变量,补充建表时的znode path
    get_layer = """select substitution from system.macros where macro='layer'"""
    # 获取shard宏变量，补充建表时的znode path
    get_shard = """select substitution from system.macros where macro='shard'"""
    # 判断part是否存在，存在则会detach，不存在将会返回0或者连接丢失报错
    part_is_exists = """select 1 from system.parts where name='{part_name}'"""
    # 检查分区的行数和大小，做对账，保证数据一致
    check_partitions = """ select sum(rows) as rows,sum(toUInt32(bytes_on_disk/1024/1024)) as bytes 
                FROM cluster('{cluster}', system, parts)
               where database='{database}'
                and table='{table}'
                and partition='{partition}'
                and bytes_on_disk>100
                and disk_name<>'hdfs'"""
    check_fetch_part_running = """select 1 from system.processes where query like '%FETCH PART \\'{part_name}\\'%'"""
    check_attach_part_is_exists = """select 1 from system.parts 
    where database={database} 
    and table={table}
    and rows={rows}
    and toUInt32(bytes_on_disk/1024/1024)={bytes}
    and toDate(modification_time)=today()"""


def partition_rebalance(partition: str):
    # 获得part信息
    logger.info(f"{partition} is rebalancing!")
    err_code, part_list = cli.execute(
        chsql.get_parts.format(cluster=cluster, database=database, table=table, partition=partition))
    node_part_dict = {}
    node_stat_dict = {}
    # 在搬移之前计算总量和总大小，做对账
    err_code, before_check = cli.execute(
        chsql.check_partitions.format(cluster=cluster, database=database, table=table, partition=partition))
    before_sum_bytes = before_check[0][1]
    before_sum_rows = before_check[0][0]
    # 得到part信息和搬移前的每个shard的存储和行数信息
    for part in part_list:
        shard_num = part[0]
        if shard_num not in node_part_dict.keys():
            node_part_dict[shard_num] = []
        if shard_num not in node_stat_dict.keys():
            node_stat_dict[shard_num] = {'start_rows': 0, 'start_bytes': 0, 'plan_end_rows': 0, 'plan_end_bytes': 0,
                                         'move_rows': 0, 'move_bytes': 0}
        node_part_dict[shard_num].append(part[1:])
        node_stat_dict[shard_num]['start_rows'] += part[2]
        node_stat_dict[shard_num]['plan_end_rows'] += part[2]
        node_stat_dict[shard_num]['start_bytes'] += part[3]
        node_stat_dict[shard_num]['plan_end_bytes'] += part[3]
    # 补齐节点，可能存在节点没有表的part数据。
    for shard_num in node_dict.keys():
        if shard_num not in node_stat_dict.keys():
            node_stat_dict[shard_num] = {'start_rows': 0, 'start_bytes': 0, 'plan_end_rows': 0, 'plan_end_bytes': 0}
    logger.info(f"partition<{partition}>:rows<{before_sum_rows}>,bytes<{before_sum_bytes}MB>")
    # 排序，从小的part到大的part 从list pop
    avg_bytes = int(before_sum_bytes / len(node_stat_dict.keys()))
    for shard_num, part_list in node_part_dict.items():
        node_part_dict[shard_num] = sorted(part_list, key=itemgetter(3, 2), reverse=True)

    # 从小的part到大的part，依次加入待搬移队列，直到小于平均存储
    move_part_list = []
    for shard_num, part in node_part_dict.items():
        if not part:
            continue
        m_part = part.pop()
        if node_stat_dict[shard_num]['plan_end_bytes'] - m_part[2] >= avg_bytes * high_rate:
            while node_stat_dict[shard_num]['plan_end_bytes'] - m_part[2] >= avg_bytes * cp_rate:
                move_part_list.append((shard_num, m_part))
                node_stat_dict[shard_num]['plan_end_bytes'] -= m_part[2]
                node_stat_dict[shard_num]['plan_end_rows'] -= m_part[1]
                node_stat_dict[shard_num]['is_push'] = 1
                if not part:
                    break
                m_part = part.pop()
    logger.info(f"partition<{partition}>:move_part_list<{move_part_list}>")
    # 获得需要接收part的节点
    need_get_shard_list = []
    for shard_num, status in node_stat_dict.items():
        if status.get('is_push', 0) == 1:
            continue
        if status['start_bytes'] <= avg_bytes * low_rate:
            need_get_shard_list.append(shard_num)
    logger.info(f"partition<{partition}>:need_get_shard_list<{need_get_shard_list}>")
    if not move_part_list or not need_get_shard_list:
        logger.info(f"partition<{partition}>:no fetch part or partition data is balance")
        return 0
    # 生成每个part的搬移task
    move_op_list = []
    while move_part_list and need_get_shard_list:
        m_part = move_part_list.pop()
        shard_num = random.choice(need_get_shard_list)
        if node_stat_dict[shard_num]['plan_end_bytes'] > avg_bytes * rev_rate:
            need_get_shard_list.remove(shard_num)
            move_part_list.append(m_part)
            continue
        move_op_list.append((m_part[0], shard_num, m_part[1]))
        node_stat_dict[shard_num]['plan_end_bytes'] += m_part[1][2]
        node_stat_dict[shard_num]['plan_end_rows'] += m_part[1][1]
    logger.info(f"partition<{partition}>:move_op_list<{move_op_list}>")
    # shard间并行，但限制：shard同时间只能发一个part，每个接收shard 同时间只能接收一个part，
    # mvp 结构 (part src shard, part target shard,part info<part_name,rows,bytes,disk>)
    move_op_list = sorted(move_op_list, key=itemgetter(1, 0), reverse=True)
    sed_set = set()
    recv_set = set()
    # pool 定义为100，最大支持200个节点的集群，极限情况下做数据均衡。
    pool = ThreadPoolExecutor(100, thread_name_prefix='thread')
    move_op_deque = deque(move_op_list)

    def callback(res):  # 定义回调函数
        err_code, mvp = res.result()
        send_shard = mvp[0]
        recv_shard = mvp[1]
        rows = mvp[2][1]
        bytes = mvp[2][2]
        sed_set.remove(send_shard)
        recv_set.remove(recv_shard)
        if err_code:
            part_name = mvp[2][0]
            disk_name = mvp[2][3]
            logger.warning(f"partition<{partition}>:{disk_name}:{part_name}:{rows}row:{bytes}MB:"
                           f"from shard<{send_shard}> move shard<{recv_shard}> is failed")
            logger.info(f"{partition}:sed_set remove:{send_shard},recv_shard remove:{recv_shard}")
            return
        node_stat_dict[send_shard]['move_rows'] -= rows
        node_stat_dict[recv_shard]['move_rows'] += rows
        node_stat_dict[send_shard]['move_bytes'] -= bytes
        node_stat_dict[recv_shard]['move_bytes'] += bytes
        node_partition_stat_dict[send_shard]['move_rows_sum'] -= rows
        node_partition_stat_dict[recv_shard]['move_rows_sum'] += rows
        node_partition_stat_dict[send_shard]['move_bytes_sum'] -= bytes
        node_partition_stat_dict[recv_shard]['move_bytes_sum'] += bytes
        logger.info(f"{partition}:sed_set remove:{send_shard},recv_shard remove:{recv_shard}")

    task_obj_list = []
    th_num = 10001
    while move_op_deque:
        mvp = move_op_deque.pop()
        if mvp[0] not in sed_set and mvp[1] not in recv_set:
            sed_set.add(mvp[0])
            recv_set.add(mvp[1])
            obj = pool.submit(fetch_part, th_num, partition, mvp)
            obj.add_done_callback(callback)
            task_obj_list.append(obj)
            th_num += 1
        else:
            move_op_deque.appendleft(mvp)
            time.sleep(2)
    wait(task_obj_list, timeout=None, return_when=ALL_COMPLETED)
    time.sleep(20)
    err_code, after_check = cli.execute(
        chsql.check_partitions.format(cluster=cluster, database=database, table=table, partition=partition))
    after_sum_bytes = after_check[0][1]
    after_sum_rows = after_check[0][0]
    logger.debug(f"{partition}:end:node_stat_dict:{node_stat_dict}")
    logger.info(f"{partition}:node_partition_stat_dict:{node_partition_stat_dict}")
    if before_sum_rows == after_sum_rows:
        logger.info(
            f"{partition}: Check:partition rows is same: {after_sum_rows},{after_sum_bytes}MB")
        return 0  # error=0
    else:
        logger.error(f"{partition}:Check:partition rows is not same: before:{before_sum_rows},"
                     f"after:{after_sum_rows}")
        not_same_partition.append((partition, before_sum_rows, after_sum_rows))
        return 1  # error=1


def fetch_part(th_num, partition, mvp):
    send_shard = mvp[0]
    recv_shard = mvp[1]
    part_name = mvp[2][0]
    rows = mvp[2][1]
    bytes = mvp[2][2]
    disk_name = mvp[2][3]
    send_rp1_cli = node_dict[send_shard][0][1]
    recv_rp1_cli = node_dict[recv_shard][0][1]
    logger.info(f"<{th_num}>:{partition}:disk<{disk_name}>part<{part_name}> is moving,"
                f"from shard<{send_shard}>to shard<{recv_shard}>,rows:<{rows}>,bytes:<{bytes}MB>")
    if len(node_dict[send_shard]) == 2:
        is_source_two_replica = True
    else:
        is_source_two_replica = False
    # 获得来源节点的shard 宏变量
    err_code, shard_macro = send_rp1_cli.execute(chsql.get_shard)
    if err_code and is_source_two_replica:
        err_code, shard_macro = node_dict[send_shard][1][1].execute(chsql.get_shard)
        if err_code:
            logger.error(f"send_shard<{send_shard}>: can not get right shard_macro")
            return 1, mvp
    elif err_code:
        logger.error(f"send_shard<{send_shard}>: can not get right shard_macro")
        return 1, mvp
    try:
        shard_macro = shard_macro[0][0]
    except IndexError:
        logger.error(f"send_shard<{send_shard}>: can not get right shard_macro")
        return 1, mvp

    log_header1 = f"<{th_num}>:{partition}:disk<{disk_name}>:part<{part_name}><{send_shard}>to shard<{recv_shard}>"
    log_header2 = f"<{th_num}>:{partition}:disk<{disk_name}>:part<{part_name}>"

    fetch_part_sql = chsql.fetch_part.format(database=database, table=table, part_name=part_name,
                                             layer=layer, shard=shard_macro)
    flag = True
    for node in node_dict[recv_shard]:
        node_host = node[0]
        # 为了支持重放，在搬移之前删除part，这里大多数情况会报part不存在错误，忽略。
        logger.info(f"{log_header1}:node<{node_host}> Before fetch part,drop detached part")
        node[1].execute(chsql.set_drop_detached)
        node[1].execute(chsql.drop_detach_part.format(database=database, table=table, part_name=part_name))
        err_code, res = node[1].execute(fetch_part_sql)
        if err_code:
            # 如果出现错误，会将拉取的part删除，避免异常。
            # 这里判断err 类型，如果是socket timeout，需要sleep 判断拉取是否成功。
            is_succ_fetch = False
            if err_code == 90:
                while 1:
                    time.sleep(10)
                    err_code, res = node[1].execute(chsql.check_fetch_part_running.format(part_name=part_name))
                    if not err_code and len(res) > 0 and res[0][0] == 1:
                        continue
                    if not err_code and not res:
                        is_succ_fetch = True
                        break
                    if err_code:
                        break
            if not is_succ_fetch:
                logger.error(f"{log_header1}:node<{node_host}> fetch part failed")
                logger.warning(f"{log_header1}:node<{node_host}> drop detached part")
                node[1].execute(chsql.set_drop_detached)
                node[1].execute(chsql.drop_detach_part.format(database=database, table=table, part_name=mvp[2][0]))
                flag = False
                break
        if not version_flag:
            break
    is_move_succ = False
    is_detached_part = False
    if flag:
        # 在挂载之前需要判断源头的part是否存在，如果判断存在将其detach，然后在搬移后节点attach。
        logger.info(f"{log_header2}:send_shard<{send_shard}>:<{node_dict[send_shard][0][0]}>,"
                    f"Check if it exists before attaching")
        err_code, res = send_rp1_cli.execute(chsql.part_is_exists.format(part_name=part_name))
        if not err_code and len(res) > 0 and len(res[0]) > 0 and res[0][0] == 1:
            logger.info(f"{log_header2}:send_shard<{send_shard}>:<{node_dict[send_shard][0][0]}>,"
                        f"is exists,execute detach part")
            err_code, res = send_rp1_cli.execute(chsql.detach_part.format(database=database, table=table,
                                                                          part_name=part_name))
            if not err_code:
                is_detached_part = True
                err_code, res = recv_rp1_cli.execute(chsql.attach_part.format(database=database, table=table,
                                                                              part_name=part_name))
                if not err_code:
                    logger.info(f"{log_header1} move part successfully!,"
                                f"rows:<{rows}>,bytes:<{bytes}MB> ")
                    is_move_succ = True
                else:
                    time.sleep(10)
                    recv_rp1_cli.execute(chsql.attach_part.format(database=database, table=table, part_name=part_name))
                    err_code, res = recv_rp1_cli.execute(
                        chsql.check_attach_part_is_exists.format(database=database, table=table, rows=rows,
                                                                 bytes=bytes))
                    if not err_code and len(res) > 0 and res[0][0] == 1:
                        logger.info(f"{log_header1} move part successfully!,rows:<{rows}>,bytes:<{bytes}MB> ")
                        is_move_succ = True
            elif err_code in (2, 3, 4, 94):
                is_detached_part = False
                logger.warning(f"{log_header2}:send_shard<{send_shard}>:<{node_dict[send_shard][0][0]}>,"
                               f"detach part is failed,will remove tmp part file")
            else:
                is_detached_part = True

        else:
            logger.warning(f"{log_header2}:send_shard<{send_shard}>:<{node_dict[send_shard][0][0]}>,"
                           "is not exists,will remove tmp part file")
    if is_move_succ:
        logger.info(f"{log_header2}:send_shard<{send_shard}>:<{node_dict[send_shard][0][0]}> drop part.")
        send_rp1_cli.execute(chsql.set_drop_detached)
        send_rp1_cli.execute(chsql.drop_detach_part.format(database=database, table=table, part_name=part_name))
        if is_source_two_replica:
            send_rp1_cli = node_dict[send_shard][1][1]
            logger.info(f"{log_header2}:send_shard<{send_shard}>:<{node_dict[send_shard][1][0]}> drop part.")
            send_rp1_cli.execute(chsql.set_drop_detached)
            send_rp1_cli.execute(chsql.drop_detach_part.format(database=database, table=table, part_name=part_name))
        return 0, mvp
    logger.warning(f"{log_header2}:rev_shard<{recv_shard}> move part,attach part is failed,Rollback!")
    if is_detached_part:
        err_code, res = send_rp1_cli.execute(chsql.attach_part.format(database=database, table=table,
                                                                      part_name=part_name))
        if err_code in (92, 93, 94):
            time.sleep(10)
            send_rp1_cli.execute(chsql.attach_part.format(database=database, table=table, part_name=part_name))
        err_code, res = send_rp1_cli.execute(
            chsql.check_attach_part_is_exists.format(database=database, table=table, rows=rows, bytes=bytes))
        if not err_code and len(res) > 0 and res[0][0] == 1:
            logger.warning(f"{log_header2}:send_shard<{send_shard}> Rollback successfully!")
        else:
            attach_fail_sql_list.append([node_dict[send_shard][0][0], partition, part_name,
                                         chsql.attach_part.format(database=database, table=table,
                                                                  part_name=part_name)])
            logger.error(f"{log_header2}:send_shard<{send_shard}>:attach failed, need manual recovery")
    for node in node_dict[send_shard]:
        node[1].execute(chsql.set_drop_detached)
        node[1].execute(
            chsql.drop_detach_part.format(database=database, table=table, part_name=part_name))

    return 1, mvp


def re_init_cli():
    global node_dict
    for shard, nodes in node_dict.items():
        for node in nodes:
            node[1].exclose()
            node[1] = CHClient(host=node[0], port=int(port), database='default', user=user, password=passwd)


if __name__ == "__main__":
    usage = "Usage: %prog [options] clickhouse python client"
    parser = OptionParser(usage=usage)
    ch_group = OptionGroup(parser, "Clickhouse Connect Config", "...")
    ch_group.add_option('-c', '--cluster', help='cluster name')
    ch_group.add_option('-i', '--ip', help="host")
    ch_group.add_option('-u', '--user', default="default", help='user default "default"')
    ch_group.add_option('-a', '--password', default="", help='password default ""')
    ch_group.add_option('-p', '--port', help='port default ""')
    ch_group.add_option('-d', '--database', help='database default ""')
    ch_group.add_option('-t', '--table', default="", help='table name default "" nothing to do')
    parser.add_option_group(ch_group)
    options, args = parser.parse_args(sys.argv[1:])
    cluster = options.cluster
    host = options.ip
    user = options.user
    passwd = options.password
    port = options.port
    database = options.database
    table = options.table
    if not cluster or not user or not port or not passwd or not database or not table:
        raise ValueError("<argv> --cluster --user --password --port --database --table")
    cli = CHClient(host=host, port=int(port), database='default', user=user, password=passwd)
    chsql = CHSQL()
    err_code, node_list = cli.execute(chsql.get_nodes.format(cluster=cluster))
    if not node_list:
        raise ValueError("集群输入有误，请输入正确的集群信息")

    # 使用FileHandler输出到文件
    fh = logging.FileHandler(f'rebalance_{cluster}-{database}-{table}.log', encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    # 4个阈值
    # high_rete 高占用节点搬移超过平均值多少百分比开始计算
    high_rate = 1.00
    # low_rate 低占用节点小于平均值多少百分比开始计算
    low_rate = 0.75
    # 发送阈值 如果开始发，允许高占用节点发之后和平均值对比小于多少百分比时停止
    cp_rate = 1.00
    # 接收阈值 如果开始收，允许低占用节点接收之后和平均值对比大于多少百分比时停止
    rev_rate = 1.00
    # 回滚是attach失败的命令记录，需要手动恢复
    attach_fail_sql_list = []
    # 每个节点搬移之后的状态统计
    node_partition_stat_dict = {}
    # 获得节点信息并初始化每个节点的client
    version = '21.8'
    if version == '21.8':
        version_flag = True
    node_dict = {}
    for node in node_list:
        if node[0] not in node_dict.keys():
            node_dict[node[0]] = []
        node_dict[node[0]].append([node[1]])
    for shard, nodes in node_dict.items():
        for node in nodes:
            node.append(CHClient(host=node[0], port=int(port), database='default', user=user, password=passwd))
    for shard in node_dict.keys():
        node_partition_stat_dict[shard] = {'move_rows_sum': 0, 'move_bytes_sum': 0}

    # 获得layer，补全zknode
    err_code, layer = cli.execute(chsql.get_layer)
    if not layer or not layer[0][0]:
        ValueError("集群不包含layer，需要调整代码适配，目前暂不支持")
    layer = layer[0][0]
    # 获得分区信息
    err_code, partition_tuple = cli.execute(
        chsql.get_partitions.format(cluster=cluster, database=database, table=table, high_rate=high_rate,
                                    low_rate=low_rate))
    if not partition_tuple:
        logger.warning("没有分区需要均衡!")
        exit(0)
    partition_list = [partition[0] for partition in partition_tuple]
    # 记录对账后不一致的partition，最后会再核对一次
    not_same_partition = []
    # 每10个分区执行之后开始等待1分钟
    exec_num = len(partition_list) // 10 + 1
    while exec_num:
        exec_num -= 1
        execute_parititon_list = partition_list[exec_num * 10:(exec_num + 1) * 10]
        while execute_parititon_list:
            partition = execute_parititon_list.pop()
            re_init_cli()
            partition_rebalance(partition)
        logger.info("延迟60秒后进行新的分区均衡！")
        time.sleep(60)
    logger.info(f"{cluster}:{database}:{table}:node_partition_stat_dict:{node_partition_stat_dict}")
    logger.warning(f"{cluster}:{database}:{table}:not_same_partition:{not_same_partition}")
    last_not_same_partition = []
    for diff_partition, bef, aft in not_same_partition:
        err_code, res = cli.execute(
            chsql.check_partitions.format(cluster=cluster, database=database, table=table, partition=diff_partition))
        last_sum_bytes = res[0][1]
        last_sum_rows = res[0][0]
        if last_sum_rows != bef:
            last_not_same_partition.append((diff_partition, bef, aft, last_sum_rows))
    logger.error(f"{cluster}:{database}:{table}:last_not_same_partition:{last_not_same_partition}")
    logger.warning(f"{cluster}:{database}:{table}:attach_fail_sql_list:{attach_fail_sql_list}")
