from random_walk import *
import sys

sys.path.append("..")
from root_cause_utils import *
from utils import *
import pandas as pd
import re, os, pickle
import getopt
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

anomaly_detection_dir = '/home/v-zrr19/alibaba/result/spot'
root_cause_result_dir = '/home/v-zrr19/alibaba/result/root_cause_cut_graph'
anomaly_detection_metric_dir = '/home/v-zrr19/alibaba/result/spotCurve'
pcmci_causal_graph_dir = '/home/v-zrr19/alibaba/result/tigramite/alibaba'
pc_causal_graph_dir = '/home/v-zrr19/alibaba/result/pc'
data_dir = '/home/v-zrr19/alibaba/data'


def str_find_list(strs,lists):
    for item in lists:
        if strs.find(item)+1:
            return True
    return False
def get_metric(metric_list,level=None,types='.'):
    result=[]
    if level=='up':
        level_list = ['metaq.receive','notify.receive','web','hsf.provider']
    elif level=='down':
        level_list = ['metaq.send','notify.send','tair.read','tair.write','tddl.read','tddl.write','hsf.consumer']
    elif level=='sys':
        level_list = ['system']
    elif level=='jvm':
        level_list = ['jvm']
    else:
        level_list = []
    for item in metric_list:
        if str_find_list(item,level_list) and item.find(types)+1:
            result.append(item)
    return result


def cut_graph(G):
    up_qps_metrics = get_metric(list(G.nodes), 'up', 'qps')
    up_rt_metrics = get_metric(list(G.nodes), 'up', 'rt')
    down_qps_metrics = get_metric(list(G.nodes), 'down', 'qps')
    down_rt_metrics = get_metric(list(G.nodes), 'down', 'rt')
    sys_metrics = get_metric(list(G.nodes), 'sys')
    jvm_metrics = get_metric(list(G.nodes), 'jvm')
    rt_metrics = get_metric(list(G.nodes), types='rt')
    # remove parents of up qps
    for up_qps_metric in up_qps_metrics:
        for parent in list(G.pred[up_qps_metric]):
            if parent in set(down_qps_metrics + sys_metrics + jvm_metrics + up_rt_metrics):
                G.remove_edge(parent, up_qps_metric)
    # remove parents of jvm system
    for jvm_system_metric in set(jvm_metrics + rt_metrics):
        for parent in list(G.pred[jvm_system_metric]):
            if parent in set(rt_metrics+down_qps_metrics):
                G.remove_edge(parent, jvm_system_metric)
    # remove parents of down qps
    for down_qps_metric in down_qps_metrics:
        for parent in list(G.pred[down_qps_metric]):
            if parent in set(down_rt_metrics):
                G.remove_edge(parent, down_qps_metric)

    return G


def remove_self(list_a, a):
    list_a = list(list_a)
    if a in list_a:
        list_a.remove(a)
        return list_a
    else:
        return list_a


def get_anomaly_parent_list(kpi, anomaly_metrics, G):
    parent_list = remove_self(list(G.pred[kpi]), kpi)
    anomaly_parent_list = set(parent_list) & set(anomaly_metrics)
    return list(anomaly_parent_list)


def trace_root_cause_cut_graph(G, kpi, anomaly_metrics):
    result = []
    anomaly_parent_list = get_anomaly_parent_list(kpi, anomaly_metrics, G)
    history_list = anomaly_parent_list
    while len(anomaly_parent_list):
        next_anomaly_parent_list = []
        for item in anomaly_parent_list:
            parents_list = get_anomaly_parent_list(item, anomaly_metrics, G)
            new_parents_list = list(set(parents_list) - set(history_list))
            if len(new_parents_list) == 0:
                result.append(item)
            else:
                next_anomaly_parent_list.extend(new_parents_list)
                history_list.extend(new_parents_list)
        anomaly_parent_list = next_anomaly_parent_list
    return result
def root_cause_cut_graph(anomaly_cases, anomaly_date):
    root_cause_time_range_before = 10  # hours before kpi anomaly time
    root_cause_time_range_after = 1
    result_dir = os.path.join(root_cause_result_dir, anomaly_date)
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    for case in anomaly_cases:
        print('case', case)
        # if case['app']!='89188915304899430089948996' or case['kpi']!='avg(web.rt)':
        #     continue
        case_result_dir = os.path.join(
            result_dir, case['app'], '%s_%s_%s' % (
                case['kpi'], timestamp_to_date(case['time_start']),
                timestamp_to_date(case['time_end'])))
        if not os.path.exists(case_result_dir):
            os.makedirs(case_result_dir)

        # get anomaly metrics
        metric_anomaly_file = os.path.join(anomaly_detection_metric_dir, anomaly_date, case['app'], 'metric',
                                           'anomaly_time.pickle')
        with open(metric_anomaly_file, 'rb') as fr:
            metrics_anomaly_time = pickle.load(fr)
        anomaly_metrics = \
            get_anomaly_metric_spot(metrics_anomaly_time, case['time_start'], case['time_end'],
                                    root_cause_time_range_before,
                                    root_cause_time_range_after)


        result_file = os.path.join(case_result_dir, 'cut_graph')

        data_file = os.path.join(data_dir, anomaly_date, '%s.csv' % (case['app']))
        with open(result_file, 'w') as fw:
            print('Anomaly:%s_%s KPI: %s time: %s-%s max_rate: %f'
                  % (anomaly_date, case['app'], case['kpi'], timestamp_to_date(case['time_start']),
                     timestamp_to_date(case['time_end']), case['max_rate']), file=fw)
            causal_time_id = anomaly_date

            graph_file = '%s_%s_%s_%s' % (
                case['app'], case['kpi'], timestamp_to_date(case['time_start']), timestamp_to_date(case['time_end']))

            # pcmci
            for file in os.listdir(
                    os.path.join(pcmci_causal_graph_dir, '%s' % (causal_time_id))):
                if file.find(graph_file) + 1 and file.split('.')[-1] == 'dmp':
                    print('Graph:%s_%s' % (causal_time_id, file), file=fw)

                    with open(os.path.join(pcmci_causal_graph_dir, '%s' % (causal_time_id),
                                           file), 'rb') as fr:
                        # get graph
                        G = pickle.load(fr)


                    cut_G = cut_graph(G)

                    if re.search('avg\((.*)\)', case['kpi']).group(1) not in list(cut_G.nodes()):
                        print('Kpi has no neighbors', file=fw)
                        continue

                    anomaly_metrics_list = []
                    for item in anomaly_metrics.keys():
                        anomaly_metrics_list.append(re.search('avg\((.*)\)', item).group(1))

                    result_list = trace_root_cause_cut_graph(cut_G, re.search('avg\((.*)\)', case['kpi']).group(1), anomaly_metrics_list)



                    if result_list is not None:

                        metric_anomaly_threshold_file = os.path.join(anomaly_detection_metric_dir, anomaly_date,
                                                                     case['app'],
                                                                     'metric', 'threshold.pickle')
                        with open(metric_anomaly_threshold_file, 'rb') as fr:
                            metrics_anomaly_threshold = pickle.load(fr)

                        # get max rate for anomaly parents
                        data_df = pd.read_csv(
                            os.path.join(data_dir, anomaly_date, '%s.csv' % (case['app']))).drop(
                            columns=['app_name']).dropna(axis='columns', how='all')

                        result_max_rate ={}
                        for item in result_list:
                            item = 'avg(%s)' % (item)

                            df_sequence = data_df[['timestamp', item]].copy()
                            df_sequence.loc[:, item] = df_sequence[item].fillna(df_sequence[item].mean())
                            if item == 'avg(jvm.mem.heap.usage)' or item == 'avg(system.mem.util)' or item.find(
                                    'qps') + 1:
                                max_rate1 = get_max_threshold_metric_success(anomaly_metrics[item],
                                                                             metrics_anomaly_threshold[item][
                                                                                 'lower'], df_sequence)
                                max_rate2 = get_max_threshold_metric(anomaly_metrics[item],
                                                                     metrics_anomaly_threshold[item]['upper'],
                                                                     df_sequence)
                                max_rate = max(max_rate1, max_rate2)
                            else:
                                if item.find('rate') + 1:
                                    max_rate = get_max_threshold_metric_success(anomaly_metrics[item],
                                                                                metrics_anomaly_threshold[item][
                                                                                    'lower'], df_sequence)
                                else:
                                    max_rate = get_max_threshold_metric(anomaly_metrics[item],
                                                                        metrics_anomaly_threshold[item]['upper'],
                                                                        df_sequence)
                            result_max_rate[item]=max_rate

                        R_order = dict(sorted(result_max_rate.items(), key=lambda x: x[1], reverse=True))
                        print('Trace result:',file=fw)
                        for key,value in R_order.items():
                            print('metric:%s, start anomaly time:%s, max rate:%f' % (
                                key, timestamp_to_date(anomaly_metrics[key][0]), value), file=fw)





            # pc
            for file in os.listdir(
                    os.path.join(pc_causal_graph_dir, '%s' % (causal_time_id))):
                if file.find(graph_file) + 1 and file.split('.')[-1] == 'dmp':
                    print('Graph:%s_%s' % (causal_time_id, file), file=fw)

                    with open(os.path.join(pc_causal_graph_dir, '%s' % (causal_time_id),
                                           file), 'rb') as fr:
                        # get graph
                        G = pickle.load(fr)

                    cut_G = cut_graph(G)

                    if case['kpi'] not in list(cut_G.nodes()):
                        print('Kpi has no neighbors', file=fw)
                        continue

                    result_list = trace_root_cause_cut_graph(cut_G, case['kpi'],
                                                             anomaly_metrics.keys())
                    if result_list is not None:

                        metric_anomaly_threshold_file = os.path.join(anomaly_detection_metric_dir, anomaly_date,
                                                                     case['app'],
                                                                     'metric', 'threshold.pickle')
                        with open(metric_anomaly_threshold_file, 'rb') as fr:
                            metrics_anomaly_threshold = pickle.load(fr)

                        # get max rate for anomaly parents
                        data_df = pd.read_csv(
                            os.path.join(data_dir, anomaly_date, '%s.csv' % (case['app']))).drop(
                            columns=['app_name']).dropna(axis='columns', how='all')
                        anomaly_parents_rate = {}
                        result_max_rate = {}
                        for item in result_list:

                            df_sequence = data_df[['timestamp', item]].copy()
                            df_sequence.loc[:, item] = df_sequence[item].fillna(df_sequence[item].mean())
                            if item == 'avg(jvm.mem.heap.usage)' or item == 'avg(system.mem.util)' or item.find(
                                    'qps') + 1:
                                max_rate1 = get_max_threshold_metric_success(anomaly_metrics[item],
                                                                             metrics_anomaly_threshold[item][
                                                                                 'lower'], df_sequence)
                                max_rate2 = get_max_threshold_metric(anomaly_metrics[item],
                                                                     metrics_anomaly_threshold[item]['upper'],
                                                                     df_sequence)
                                max_rate = max(max_rate1, max_rate2)
                            else:
                                if item.find('rate') + 1:
                                    max_rate = get_max_threshold_metric_success(anomaly_metrics[item],
                                                                                metrics_anomaly_threshold[item][
                                                                                    'lower'], df_sequence)
                                else:
                                    max_rate = get_max_threshold_metric(anomaly_metrics[item],
                                                                        metrics_anomaly_threshold[item]['upper'],
                                                                        df_sequence)
                            result_max_rate[item] = max_rate

                        R_order = dict(sorted(result_max_rate.items(), key=lambda x: x[1], reverse=True))
                        print('Trace result:', file=fw)
                        for key, value in R_order.items():
                            print('metric:%s, start anomaly time:%s, max rate:%f' % (
                                key, timestamp_to_date(anomaly_metrics[key][0]), value), file=fw)


if __name__ == '__main__':


    opts, args = getopt.getopt(sys.argv[1:], '-d:', [])
    for opt_name, opt_value in opts:
        if opt_name in ('-d'):
            anomaly_dates = opt_value.split(',')



    for anomaly_date in anomaly_dates:
        print(anomaly_date)
        anomaly_cases = get_anomaly_case(anomaly_date, anomaly_detection_dir, valid=True)

        root_cause_cut_graph(anomaly_cases, anomaly_date)


    # print('%r'%(bool(remove_kpi)))
    # print(sys.argv[1:])
    # anomaly_dates = sys.argv[1:]
    # print(remove_kpi)
    # for anomaly_date in anomaly_dates:
    #    print(anomaly_date)
    #    anomaly_cases = get_anomaly_case(anomaly_date, anomaly_detection_dir)
    #    print(anomaly_cases)
#
#    root_cause_random_walk(anomaly_cases, anomaly_date,r,beta,num_loop,p_corr,remove_kpi)
