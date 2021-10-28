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



class CHClient:
    def __init__(self, user: str = 'default', password: str = '', host: str = "", port: str = '', database: str = ''):
        """
        init clickhouse ch_client.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.settings = {'receive_timeout':1200,'send_timeout':1200}
        self.chcli = Client(host=self.host, port=self.port, database=self.database, user=self.user,
                            password=self.password, send_receive_timeout=1200, settings=self.settings)

    def execute(self, sql: str = None, with_column_types: bool = False) -> Tuple[int, List[Tuple]]:
        """
        :param sql: execute this sql
        :param with_column_types:result[-1] return column types
        :return: result ->list<tuple>>
        """
        try:
            res = self.chcli.execute(sql, with_column_types=with_column_types)
            return 0, res
        except Exception:
            err = traceback.format_exc()
            if 'Code: 233.' in err: # 过滤掉重放是出现的删除drop detach part不存在的错误，污染log
                return 1, [()]
            logging.error(f"Failed to execute {sql},err:" + err)
            return 1, [()]

    def exclose(self):
        """
        close clickhouse connection
        :return:
        """
        self.chcli.disconnect()

    def ping(self) -> Client:
        """
        execute select 1
        :return:
        """
        try:
            self.chcli.execute('select 1')
        except (errors.NetworkError, errors.ServerException):
            self.chcli = Client(host=self.host, port=self.port, database=self.database, user=self.user,
                                password=self.password)
        return self.chcli


class CHSQL:
    get_nodes = """select shard_num,host_address
        from system.clusters
        where cluster='{cluster}'
        order by shard_num,replica_num
    """
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
                    AND (toDate(parseDateTimeBestEffortOrZero(toString(partition))) <= (today() - 3))
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
                AND (toDate(parseDateTimeBestEffortOrZero(toString(partition))) <= (today() - 3))
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
    order by partition desc;
    """
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
        and disk_name<>'hdfs'
        
    """
    fetch_part = """ALTER TABLE {database}.{table} FETCH PART '{part_name}' FROM '/clickhouse/tables/{layer}-{shard}/{database}.{table}'
    """
    drop_detach_part = """ALTER TABLE {database}.{table} DROP DETACHED PART '{part_name}'"""
    attach_part = """ALTER TABLE {database}.{table} ATTACH PART '{part_name}'"""
    detach_part = """ALTER TABLE {database}.{table} DETACH PART '{part_name}'"""
    get_layer = """select substitution from system.macros where macro='layer'"""
    get_shard = """select substitution from system.macros where macro='shard'"""
    set_drop_detached = """set allow_drop_detached = 1"""
    part_is_exists = """select 1 from system.parts where name='{part_name}'"""
    check_partitions = """ select sum(rows) as rows,sum(toUInt32(bytes_on_disk/1024/1024)) as bytes 
                FROM cluster('{cluster}', system, parts)
               where database='{database}'
                and table='{table}'
                and partition='{partition}'
                and bytes_on_disk>100
                and disk_name<>'hdfs'
    """


def partition_rebalance(partition):
    # 获得part信息
    logger.info(f"{database}:{table}:{partition} is rebalancing!")
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
        if part[0] not in node_part_dict.keys():
            node_part_dict[part[0]] = []
        if part[0] not in node_stat_dict.keys():
            node_stat_dict[part[0]] = {'start_rows': 0, 'start_bytes': 0, 'end_rows': 0, 'end_bytes': 0}
        node_part_dict[part[0]].append(part[1:])
        node_stat_dict[part[0]]['start_rows'] += part[2]
        node_stat_dict[part[0]]['end_rows'] += part[2]
        node_stat_dict[part[0]]['start_bytes'] += part[3]
        node_stat_dict[part[0]]['end_bytes'] += part[3]
    # 补齐节点，可能存在节点没有表的part数据。
    for shard_num in node_dict.keys():
        if shard_num not in node_stat_dict.keys():
            node_stat_dict[shard_num] = {'start_rows': 0, 'start_bytes': 0, 'end_rows': 0, 'end_bytes': 0}
    logger.info(f"{database}:{table}:{partition}:start:node_stat_dict:{node_stat_dict}")
    logger.info(f"{database}:{table}:{partition}:rows<{before_sum_rows}>,bytes<{before_sum_bytes}MB>")
    # 排序，从小的part到大的part 从list pop
    avg_bytes = int(before_sum_bytes / len(node_stat_dict.keys()))
    for node, part_list in node_part_dict.items():
        node_part_dict[node] = sorted(part_list, key=itemgetter(3, 2), reverse=True)

    # 从小的part到大的part，依次加入待搬移队列，直到小于平均存储
    move_part_list = []
    for shard_num, part in node_part_dict.items():
        if not part:
            continue
        m_part = part.pop()
        if node_stat_dict[shard_num]['end_bytes'] - m_part[2] >= avg_bytes * high_rate:
            while node_stat_dict[shard_num]['end_bytes'] - m_part[2] >= avg_bytes * cp_rate:
                move_part_list.append((shard_num, m_part))
                node_stat_dict[shard_num]['end_bytes'] -= m_part[2]
                node_stat_dict[shard_num]['end_rows'] -= m_part[1]
                node_stat_dict[shard_num]['is_push'] = 1
                if not part:
                    break
                m_part = part.pop()
    logger.info(f"{database}:{table}:{partition}:move_part_list: {move_part_list}")
    # 获得需要接收part的节点
    need_get_shard_list = []
    for shard_num, status in node_stat_dict.items():
        if status.get('is_push', 0) == 1:
            continue
        if status['start_bytes'] <= avg_bytes * low_rate:
            need_get_shard_list.append(shard_num)
    logger.info(f"{database}:{table}:{partition}:need_get_shard_list: {need_get_shard_list}")
    if not move_part_list or not need_get_shard_list:
        logger.info(f"{database}:{table}:{partition}:no fetch part or partition data is balance")
        return 0
    # 生成每个part的搬移task
    move_op_list = []
    while move_part_list and need_get_shard_list:
        m_part = move_part_list.pop()
        shard_num = random.choice(need_get_shard_list)
        if node_stat_dict[shard_num]['end_bytes'] > avg_bytes * rev_rate:
            need_get_shard_list.remove(shard_num)
            move_part_list.append(m_part)
            continue
        move_op_list.append((m_part[0], shard_num, m_part[1]))
        node_stat_dict[shard_num]['end_bytes'] += m_part[1][2]
        node_stat_dict[shard_num]['end_rows'] += m_part[1][1]
    logger.info(f"{database}:{table}:{partition}:move_op_list: {move_op_list}")
    # 获得layer，补全zknode
    err_code, layer = cli.execute(chsql.get_layer)
    layer = layer[0][0]
    # 每个操作顺序执行。后续考虑加个线程池异步执行。
    # mvp 结构 (part src shard, part target shard,part info<part_name,rows,bytes,disk>)
    move_op_list = sorted(move_op_list, key=itemgetter(1, 0), reverse=True)
    sed_set = set()
    recv_set = set()
    pool = ThreadPoolExecutor(100)
    move_op_deque = deque(move_op_list)

    def callback(res):  # 定义回调函数
        res = res.result()
        sed_shard = res[0]
        recv_shard = res[1]
        sed_set.remove(sed_shard)
        recv_set.remove(recv_shard)
        logger.info(f"{database}:{table}:{partition}:sed_set remove:{sed_shard},recv_shard remove:{recv_shard}")

    task_obj_list = []
    th_num = 10001
    while move_op_deque:
        mvp = move_op_deque.pop()
        if mvp[0] not in sed_set and mvp[1] not in recv_set:
            sed_set.add(mvp[0])
            recv_set.add(mvp[1])
            obj = pool.submit(fetch_part, th_num, partition, layer, mvp, node_stat_dict)
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
    node_partition_stat_dict[shard_num][partition] = node_stat_dict
    for shard_num in node_stat_dict.keys():
        node_stat_dict[shard_num]['move_rows'] = node_stat_dict[shard_num]['end_rows'] - node_stat_dict[shard_num]['start_rows']
        node_stat_dict[shard_num]['move_bytes'] = node_stat_dict[shard_num]['end_bytes'] - node_stat_dict[shard_num]['start_bytes']
        node_partition_stat_dict[shard_num]['move_rows_sum'] += node_stat_dict[shard_num]['end_rows'] - node_stat_dict[shard_num]['start_rows']
        node_partition_stat_dict[shard_num]['move_bytes_sum'] += node_stat_dict[shard_num]['end_bytes'] - node_stat_dict[shard_num]['start_bytes']
    node_partition_stat_dict[shard_num][partition] = node_stat_dict
    logger.info(f"{database}:{table}:{partition}:end:node_stat_dict:{node_stat_dict}")
    if before_sum_rows == after_sum_rows:
        logger.info(
            f"{database}:{table}:{partition}: Check:partition rows is same: {after_sum_rows},{after_sum_bytes}MB")
        return 0  # error=0
    else:
        logger.error(f"{database}:{table}:{partition}:Check:partition rows is not same: before:{before_sum_rows},"
                      f"after:{after_sum_rows}")
        not_same_partition.append((partition, before_sum_rows, after_sum_rows))
        return 1  # error=1


def fetch_part(th_num, partition, layer, mvp, node_stat_dict):
    source_shard_num = mvp[0]
    to_shard_num = mvp[1]
    part_name = mvp[2][0]
    rows = mvp[2][1]
    bytes = mvp[2][2]
    dask_name = mvp[2][3]
    logger.info(f"<{th_num}>:{database}:{table}:{partition}:shard<{source_shard_num}>:dask<{dask_name}> will move "
                 f"part <{part_name}>to shard<{to_shard_num}>,rows:<{rows}>,bytes:<{bytes}MB>")
    if len(node_dict[source_shard_num]) == 2:
        is_source_two_replica = True
    else:
        is_source_two_replica = False
    # 获得来源节点的shard 宏变量
    err_code, shard_macro = node_dict[source_shard_num][0][1].execute(chsql.get_shard)
    if err_code and is_source_two_replica:
        err_code, shard_macro = node_dict[source_shard_num][1][1].execute(chsql.get_shard)
        if err_code:
            logger.error(f"source_shard_num<{source_shard_num}>: can not get right shard_macro")
            return mvp
    elif err_code:
        logger.error(f"source_shard_num<{source_shard_num}>: can not get right shard_macro")
        return mvp
    shard_macro = shard_macro[0][0]
    fetch_part_sql = chsql.fetch_part.format(database=database, table=table, part_name=part_name,
                                             layer=layer, shard=shard_macro)
    log_header1 = f"<{th_num}>:{database}:{table}:{partition}:dask<{dask_name}><{source_shard_num}>to shard<{to_shard_num}>"
    log_header2 = f"<{th_num}>:{database}:{table}:{partition}:source_shard_num<{source_shard_num}>"
    flag = True
    # 这里每个replica是顺序拉取，当一个失败，这个task就不会继续执行，后续可以改成并行拉取，但是可能会增加网络负载。
    for node in node_dict[to_shard_num]:
        node_host = node[0]
        # 为了支持重放，在搬移之前删除part，这里大多数情况会报part不存在错误，忽略。
        logger.info(f"{log_header1}:node<{node_host}> Before fetch part,drop detached part")
        node[1].execute(chsql.set_drop_detached)
        node[1].execute(chsql.drop_detach_part.format(database=database, table=table, part_name=part_name))
        err_code, res = node[1].execute(fetch_part_sql)
        if err_code:
            # 如果出现错误，会将拉取的part删除，避免异常。
            logger.error(f"{log_header1}:node<{node_host}> fetch part<{part_name}> failed")
            logger.warning(f"{log_header1}:node<{node_host}> drop detached part:{part_name}")
            node[1].execute(chsql.set_drop_detached)
            node[1].execute(chsql.drop_detach_part.format(database=database, table=table, part_name=mvp[2][0]))
            flag = False
            break
    if flag:
        if_attach_flag = False
        # 在挂载之前需要判断源头的part是否存在，如果判断存在将其detach，然后在搬移后节点attach。
        logger.info(f"{log_header2}:{part_name}:"
                     f"Check if it exists before attaching")
        err_code, res = node_dict[source_shard_num][0][1].execute(chsql.part_is_exists.format(part_name=part_name))
        if err_code == 0 and len(res)>0 and len(res[0])>0 and res[0][0] == 1:
            logger.info(f"{log_header2}:{part_name}:is exists,execute detach part")
            err_code, res = node_dict[source_shard_num][0][1].execute(chsql.detach_part.format(database=database,
                                                                                               table=table,
                                                                                               part_name=part_name))
            if err_code == 0:
                err_code, res = node_dict[to_shard_num][0][1].execute(chsql.attach_part.format(database=database,
                                                                                               table=table,
                                                                                               part_name=part_name))
                if err_code == 0:
                    if_attach_flag = True
                    node_stat_dict[mvp[1]]['end_bytes'] += bytes
                    node_stat_dict[mvp[1]]['end_rows'] += rows
                    logger.info(f"{log_header1} move part<{part_name}>:"
                                 f"rows:<{rows}>,bytes:<{bytes}MB> successfully!")
                    logger.info(f"{log_header2} drop part:{part_name}.")
                    node_dict[source_shard_num][0][1].execute(chsql.set_drop_detached)
                    node_dict[source_shard_num][0][1].execute(chsql.drop_detach_part.format(database=database,
                                                                                            table=table,
                                                                                            part_name=part_name))
                    node_dict[source_shard_num][1][1].execute(chsql.set_drop_detached)
                    node_dict[source_shard_num][1][1].execute(chsql.drop_detach_part.format(database=database,
                                                                                            table=table,
                                                                                            part_name=part_name))
        if not if_attach_flag:
            logger.warning(f"{log_header2} move part,attach part part<{part_name}> is failed ,Rollback!")
            err_code, res = node_dict[source_shard_num][0][1].execute(chsql.attach_part.format(database=database,
                                                                                               table=table,
                                                                                               part_name=part_name))
            if err_code:
                attach_fail_sql_list.append([node_dict[source_shard_num][0][0],
                                            database, 
                                            table,
                                            partition, 
                                            part_name,
                                            chsql.attach_part.format(database=database,table=table,
                                                                    part_name=part_name)])
                logger.error(f"{log_header2}:{part_name} attach failed, Data may be lost")

            logger.warning(f"{database}:{table}:{partition}:shard<{to_shard_num}>: drop detached part:{part_name}")
            for node in node_dict[to_shard_num]:
                node[1].execute(chsql.set_drop_detached)
                node[1].execute(
                    chsql.drop_detach_part.format(database=database, table=table, part_name=part_name))
    return mvp

if __name__ == "__main__":
    usage = "Usage: %prog [options] test clickhouse python client"
    parser = OptionParser(usage=usage)
    ch_group = OptionGroup(parser, "clickhouse Config", "...")
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
        raise ValueError("cluster is wrong, please input again")
    # logger 控制台输出info log文件输出debug
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # 使用FileHandler输出到文件
    fh = logging.FileHandler(f'rebalance_{cluster}-{database}-{table}.log')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

    high_rate = 1.00
    low_rate = 0.75
    cp_rate = 0.70
    rev_rate = 1.20
    # 获得节点信息
    attach_fail_sql_list = []
    node_dict = {}
    node_partition_stat_dict = {}
    for node in node_list:
        if node[0] not in node_dict.keys():
            node_dict[node[0]] = []
        node_dict[node[0]].append([node[1]])
    for shard, nodes in node_dict.items():
        for node in nodes:
            node.append(CHClient(host=node[0], port=int(port), database='default', user=user, password=passwd))
        if shard not in node_partition_stat_dict.keys():
            node_partition_stat_dict[shard] = {'move_rows_sum':0, 'move_bytes_sum':0}
    # 获得分区信息
    err_code, partition_list = cli.execute(
        chsql.get_partitions.format(cluster=cluster, database=database, table=table, high_rate=high_rate,
                                    low_rate=low_rate))
    x = 0
    if not partition_list:
        logger.warning("no partition need to rebalance!")
        exit(0)
    for partition in partition_list:
        partition_list[x] = partition[0]
        x += 1
    not_same_partition = []
    exec_num = len(partition_list) // 10 + 1
    while exec_num:
        exec_num -= 1
        execute_parititon_list = partition_list[exec_num * 10:(exec_num + 1) * 10]
        while execute_parititon_list:
            partition = execute_parititon_list.pop()
            partition_rebalance(partition)
        time.sleep(60)
    logger.info(node_partition_stat_dict)
    logger.warning(not_same_partition)
    last_not_same_partition = []
    for diff_partition, bef, aft in not_same_partition:
        err_code, res = cli.execute(
            chsql.check_partitions.format(cluster=cluster, database=database, table=table, partition=diff_partition))
        last_sum_bytes = res[0][1]
        last_sum_rows = res[0][0]
        if last_sum_rows != bef:
            last_not_same_partition.append((diff_partition, bef, aft, last_sum_rows))
    logger.error(last_not_same_partition)
    logger.warning(attach_fail_sql_list)
