from random_walk import *
from root_cause_cut_graph import cut_graph
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
root_cause_result_dir = '/home/v-zrr19/alibaba/result/online/new'
anomaly_detection_metric_dir = '/home/v-zrr19/alibaba/result/spotCurve'
pcmci_causal_graph_dir = '/home/v-zrr19/alibaba/result/newPage/alibaba'
pc_causal_graph_dir = '/home/v-zrr19/alibaba/result/newPage/pc'
data_dir = '/home/v-zrr19/alibaba/data'

def load_data_pcmci(G, data_file, anomaly_time, root_cause_time_range_before, root_cause_time_range_after):
    time_length_before = root_cause_time_range_before * 60 * 60 * 1000
    time_length_after = root_cause_time_range_after * 60 * 60 * 1000
    df = pd.read_csv(data_file).drop(columns=['app_name']).dropna(axis='columns', how='all')
    df = df.fillna(method='ffill')
    for i in G.nodes:
        G.nodes[i]['timelist'] = \
            df[(df['timestamp'] > (anomaly_time - time_length_before)) & (
                    df['timestamp'] < (anomaly_time + time_length_after))]['avg(%s)' % (i)].values

    return G


def load_data_pc(G, data_file, anomaly_time, root_cause_time_range_before, root_cause_time_range_after):
    time_length_before = root_cause_time_range_before * 60 * 60 * 1000
    time_length_after = root_cause_time_range_after * 60 * 60 * 1000
    df = pd.read_csv(data_file).drop(columns=['app_name']).dropna(axis='columns', how='all')
    df = df.fillna(method='ffill')
    for i in G.nodes:
        G.nodes[i]['timelist'] = \
            df[(df['timestamp'] > (anomaly_time - time_length_before)) & (
                    df['timestamp'] < (anomaly_time + time_length_after))][i].values

    return G

def prune_graph_pcmci(G,anomaly_metrics):
    #print(anomaly_metrics)

    for item in list(G.nodes):
        #print('avg(%s)'%(item))
        if 'avg(%s)'%(item) in anomaly_metrics:
            continue
        else:
            G.remove_node(item)
    return G


def prune_graph_pc(G,anomaly_metrics):
    for item in list(G.nodes):
        if item in anomaly_metrics:
            continue
        else:
            G.remove_node(item)
    return G
def prune_graph_based_time_pc(G,anomaly_metrics):
    for item in list(G.nodes):
        for p_item in list(G.pred[item]):
            try:
                if anomaly_metrics[p_item][0] > anomaly_metrics[item][0]:
                    G.remove_edge(p_item, item)
            except:
                continue
    return G

def prune_graph_based_time_pcmci(G,anomaly_metrics):
    for item in list(G.nodes):
        for p_item in list(G.pred[item]):
            try:
                if anomaly_metrics['avg(%s)'%(p_item)][0]>anomaly_metrics['avg(%s)'%(item)][0]:
                    G.remove_edge(p_item, item)
            except:
                continue
    return G

def root_cause_random_walk(anomaly_cases, anomaly_date, r, beta, num_loop, p_corr, remove_kpi,prune,cut):
    root_cause_time_range_before = 4  # hours before kpi anomaly time
    root_cause_time_range_after =1
    
    result_dir = os.path.join(root_cause_result_dir, anomaly_date)
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    for case in anomaly_cases:
        
        if case['app']!='891189998996304':
                continue
        print('case', case)
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
        anomaly_metrics_list = list(anomaly_metrics.keys())
        anomaly_metrics_list.append(case['kpi'])

        if beta == 1.:
            result_file = os.path.join(case_result_dir,
                                       'random_cutgraph_cut_%r_prune_%r_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r' % (cut,prune,
                                       r, beta, num_loop, p_corr, remove_kpi))

            # result_file = os.path.join(case_result_dir, 'random_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r_timebefore_%d'%(r,beta,num_loop,p_corr,remove_kpi,root_cause_time_range_before))
        else:
            result_file = os.path.join(case_result_dir, 'random_second_cutgraph_cut_%r_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r_prune_%r' % (
                cut,r, beta, num_loop, p_corr, remove_kpi,prune))
            # result_file = os.path.join(case_result_dir, 'random_second_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r_timebefore_%d' % (r, beta, num_loop, p_corr, remove_kpi,root_cause_time_range_before))
        top_early_result = []
        top_max_rate_result = []
        anomaly_parents_rate_all = {}
        data_file = os.path.join(data_dir, anomaly_date, '%s.csv' % (case['app']))
        #if os.path.exists(result_file) and os.path.getsize(result_file):
            #continue
        with open(result_file, 'a') as fw:
            print('Anomaly:%s_%s KPI: %s time: %s-%s max_rate: %f'
                  % (anomaly_date, case['app'], case['kpi'], timestamp_to_date(case['time_start']),
                     timestamp_to_date(case['time_end']), case['max_rate']), file=fw)
            causal_time_id = anomaly_date

            graph_file = '%s_%s_%s_%s' % (
                case['app'], case['kpi'], timestamp_to_date(case['time_start']), timestamp_to_date(case['time_end']))

            # pcmci
            for file in os.listdir(
                    os.path.join(pcmci_causal_graph_dir, '%s' % (causal_time_id))):
                if file.find(graph_file) + 1 and file.find('before_4_end_1') + 1 and file.split('.')[-1] == 'dmp':
                    
                    print('Graph:%s_%s' % (causal_time_id, file), file=fw)

                    with open(os.path.join(pcmci_causal_graph_dir, '%s' % (causal_time_id),
                                           file), 'rb') as fr:
                        # get graph
                        G = pickle.load(fr)

                    # print(G)
                    if re.search('avg\((.*)\)', case['kpi']).group(1) not in list(G.nodes()):
                        print('Kpi has no neighbors', file=fw)
                        continue
                    G = load_data_pcmci(G, data_file, case['time_start'], root_cause_time_range_before,
                                        root_cause_time_range_after)
                    if prune:
                        G = prune_graph_based_time_pcmci(G,anomaly_metrics)
                    if cut:
                        G = cut_graph(G)
                    try:
                        P = probablity_matrix(G, re.search('avg\((.*)\)', case['kpi']).group(1), r, p_corr, remove_kpi)
                    except:
                        print('P error')
                        continue

                    # r = 0.5
                    # beta = 1
                    # num_loop = 1000

                    if beta == 1.:
                        result = random_walk(G, P, re.search('avg\((.*)\)', case['kpi']).group(1), r, beta, num_loop,
                                             remove_kpi)
                    else:
                        result = random_walk_second_order(G, P, re.search('avg\((.*)\)', case['kpi']).group(1), r, beta,
                                                          num_loop)

                    result = dict(result)
                    rank_list = list(result.keys())
                    rank_list.remove(re.search('avg\((.*)\)', case['kpi']).group(1))

                    if rank_list is not None:

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
                        print('Random walk rank result', file=fw)
                        # print('rank_list',rank_list)
                        # print('anomaly_metrics', anomaly_metrics)
                        for item in rank_list:
                            item = 'avg(%s)' % (item)
                            if item in anomaly_metrics:
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
                                print('metric:%s, start anomaly time:%s, max rate:%f, visit time:%d' % (
                                    item, timestamp_to_date(anomaly_metrics[item][0]), max_rate,
                                    result[re.search('avg\((.*)\)', item).group(1)]), file=fw)
                            else:
                                print('metric:%s, start anomaly time:%s, max rate:%f, visit time:%d' % (
                                    item, 'No anomaly', 0., result[re.search('avg\((.*)\)', item).group(1)]), file=fw)

            # pc
            #for file in os.listdir(
            #        os.path.join(pc_causal_graph_dir, '%s' % (causal_time_id))):
            #    if file.find(graph_file) + 1 and file.find('before_4_after_1') + 1 and file.split('.')[-1] == 'dmp':
            #        print('Graph:%s_%s' % (causal_time_id, file), file=fw)
#
            #        with open(os.path.join(pc_causal_graph_dir, '%s' % (causal_time_id),
            #                               file), 'rb') as fr:
            #            # get graph
            #            G = pickle.load(fr)
#
            #        G = load_data_pc(G, data_file, case['time_start'], root_cause_time_range_before,
            #                         root_cause_time_range_after)
            #        if prune:
            #            G = prune_graph_based_time_pc(G,anomaly_metrics)
            #        if cut:
            #            G = cut_graph(G)
            #        try:
            #            P = probablity_matrix(G, case['kpi'], r, p_corr, remove_kpi)
            #        except:
            #            print('P error')
            #            continue
#
            #        # r = 0.5
            #        # beta = 1
            #        # num_loop = 1000
#
            #        if beta == 1.:
            #            result = random_walk(G, P, case['kpi'], r, beta, num_loop, remove_kpi)
            #        else:
            #            result = random_walk_second_order(G, P, case['kpi'], r, beta, num_loop)
#
            #        result = dict(result)
            #        rank_list = list(result.keys())
            #        rank_list.remove(case['kpi'])
#
            #        if rank_list is not None:
#
            #            metric_anomaly_threshold_file = os.path.join(anomaly_detection_metric_dir, anomaly_date,
            #                                                         case['app'],
            #                                                         'metric', 'threshold.pickle')
            #            with open(metric_anomaly_threshold_file, 'rb') as fr:
            #                metrics_anomaly_threshold = pickle.load(fr)
#
            #            # get max rate for anomaly parents
            #            data_df = pd.read_csv(
            #                os.path.join(data_dir, anomaly_date, '%s.csv' % (case['app']))).drop(
            #                columns=['app_name']).dropna(axis='columns', how='all')
            #            anomaly_parents_rate = {}
            #            print('Random walk rank result', file=fw)
            #            # print('rank_list',rank_list)
            #            # print('anomaly_metrics', anomaly_metrics)
            #            for item in rank_list:
            #                if item in anomaly_metrics:
            #                    df_sequence = data_df[['timestamp', item]].copy()
            #                    df_sequence.loc[:, item] = df_sequence[item].fillna(df_sequence[item].mean())
            #                    if item == 'avg(jvm.mem.heap.usage)' or item == 'avg(system.mem.util)' or item.find(
            #                            'qps') + 1:
            #                        max_rate1 = get_max_threshold_metric_success(anomaly_metrics[item],
            #                                                                     metrics_anomaly_threshold[item][
            #                                                                         'lower'], df_sequence)
            #                        max_rate2 = get_max_threshold_metric(anomaly_metrics[item],
            #                                                             metrics_anomaly_threshold[item]['upper'],
            #                                                             df_sequence)
            #                        max_rate = max(max_rate1, max_rate2)
            #                    else:
            #                        if item.find('rate') + 1:
            #                            max_rate = get_max_threshold_metric_success(anomaly_metrics[item],
            #                                                                        metrics_anomaly_threshold[item][
            #                                                                            'lower'], df_sequence)
            #                        else:
            #                            max_rate = get_max_threshold_metric(anomaly_metrics[item],
            #                                                                metrics_anomaly_threshold[item]['upper'],
            #                                                                df_sequence)
            #                    print('metric:%s, start anomaly time:%s, max rate:%f, visit time:%d' % (
            #                        item, timestamp_to_date(anomaly_metrics[item][0]), max_rate, result[item]), file=fw)
            #                else:
            #                    print('metric:%s, start anomaly time:%s, max rate:%f, visit time:%d' % (
            #                        item, 'No anomaly', 0., result[item]), file=fw)


if __name__ == '__main__':
    
    r = 0.5
    beta = 1
    num_loop = 1000
    p_corr = True
    remove_kpi = True
    cores = multiprocessing.cpu_count()
    pool = ProcessPoolExecutor(max_workers=16)

    opts, args = getopt.getopt(sys.argv[1:], '-d:-p:-k:-r:-b:-n:-c:', [])
    for opt_name, opt_value in opts:
        if opt_name in ('-d'):
            anomaly_dates = opt_value.split(',')
        if opt_name in ('-p'):
            # p_corr = bool(int(opt_value))
            p_corrs = opt_value.split(',')
        if opt_name in ('-k'):
            # remove_kpi = bool(int(opt_value))
            remove_kpis = opt_value.split(',')
        if opt_name in ('-r'):
            #r = float(opt_value)
             r_s=opt_value.split(',')
        if opt_name in ('-b'):
            # beta = float(opt_value)
            beta_s = opt_value.split(',')
        if opt_name in ('-n'):
            # beta = float(opt_value)
            prunes = opt_value.split(',')
        if opt_name in ('-c'):
            # beta = float(opt_value)
            cuts = opt_value.split(',')
    # for anomaly_date in anomaly_dates:
    #    print(anomaly_date)
    #    anomaly_cases = get_anomaly_case(anomaly_date, anomaly_detection_dir,valid=True)
    #    print(anomaly_cases)
    #    for p in p_corrs:
    #        p_corr = bool(int(p))
    #        for k in remove_kpis:
    #            remove_kpi = bool(int(k))
    #            for betas in beta_s:
    #                beta=float(betas)
    #                pool.submit(root_cause_random_walk,anomaly_cases, anomaly_date,r,beta,num_loop,p_corr,remove_kpi)
    #                print('ok')
    #                # root_cause_random_walk(anomaly_cases, anomaly_date,r,beta,num_loop,p_corr,remove_kpi)
    # pool.shutdown(wait=True)
    for anomaly_date in anomaly_dates:
        print(anomaly_date)
        anomaly_cases = get_valid_label_case(anomaly_date, anomaly_detection_dir)
        for p in p_corrs:
            p_corr = bool(int(p))
            for k in remove_kpis:
                remove_kpi = bool(int(k))
                for betas in beta_s:
                    beta = float(betas)
                    for rs in r_s:
                        r=float(rs)
                        for prune in prunes:
                            for cut in cuts:
                                root_cause_random_walk(anomaly_cases, anomaly_date, r, beta,num_loop,
                                                       p_corr,remove_kpi,bool(int(prune)),bool(int(cut)))
                            
                    # root_cause_random_walk(anomaly_cases, anomaly_date,r,beta,num_loop,p_corr,remove_kpi)



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
