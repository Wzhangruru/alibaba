import networkx
import networkx.classes.reportviews
import re,os,pickle
import types

import sys


from spotrt0327a import biSPOTR
from spotSuccess0327a import biSPOTS
from get_rate import get_the_rate
import pandas as pd
from utils import timestamp_to_date,transfer_date_to_timestamp
data_dir = '/home/v-zrr19/alibaba/data'
valid_case_file = '/home/v-zrr19/alibaba/result/evalution/given_root_result.pickle'
def get_max_threshold(time_start,time_end,threshold_df,data_df):
    max_rate = 0
    time = time_start
    while time<= time_end:
        # print(time)
        try:
            rate = (data_df.loc[data_df['timestamp'] == time].values[0][1]-threshold_df.loc[threshold_df['timestamp'] == time].values[0][1]) /threshold_df.loc[threshold_df['timestamp'] == time].values[0][1]
            # print('rate',rate)
        except:
            # print('No threshold (the first day data)')
            time = time + 60 * 1000
            continue
        if rate > max_rate:
            max_rate = rate
        time = time + 60 * 1000
    #print('max rate_t', max_rate)
    return max_rate
def get_max_threshold_metric(time_list,threshold_df,data_df):
    max_rate = 0
    for time in time_list:
        try:
            rate = (data_df.loc[data_df['timestamp'] == time].values[0][1]-threshold_df.loc[threshold_df['timestamp'] == time].values[0][1]) /threshold_df.loc[threshold_df['timestamp'] == time].values[0][1]
        except:
            # print('metric',time)
            # print('No threshold (the first day data)')
            continue
        if rate > max_rate:
            max_rate = rate
    #print('max_rate_t_m',max_rate)
    return max_rate


def get_max_threshold_metric_success(time_list, threshold_df, data_df):
    max_rate = 0
    for time in time_list:
        try:
            rate = (threshold_df.loc[threshold_df['timestamp'] == time].values[0][1]-
                    data_df.loc[data_df['timestamp'] == time].values[0][1]
                    ) / \
                   threshold_df.loc[threshold_df['timestamp'] == time].values[0][1]
            #print('data', data_df.loc[data_df['timestamp'] == time].values[0][1])
            #print('threshold', threshold_df.loc[threshold_df['timestamp'] == time].values[0][1])

        except:
            # print('success',time)
            # print('No threshold (the first day data)')
            continue
        if rate > max_rate:
            max_rate = rate
    #print('max_rate_t_m_s',max_rate)
    return max_rate
def get_valid_case(anomaly_date):
    result = []
    with open(valid_case_file,'rb') as fr:
        valid_cases = pickle.load(fr)
    for item in valid_cases:
        if item['date'] == anomaly_date:
            result.append({'app':item['app'],'kpi':item['kpi'],'anomaly_time':item['anomaly_time']})
    #print('vc',result)
    return result
def split_str(str1):
    tmp = str1.split('-')
    return ['%s-%s'%(tmp[0],tmp[1]),'%s-%s'%(tmp[2],tmp[3])]
def get_valid_label_case(date,anomaly_detection_dir):
    result_list = []
    with open (valid_case_file,'rb') as fl:
        roots=pickle.load(fl)
    for case in roots:
        if case['date'] == date:
            app_result_path = os.path.join(anomaly_detection_dir, date, case['app'])
            with open(os.path.join(app_result_path, 'kpi', '%s_anomaly_case.pickle' % (case['kpi'])), 'rb') as fr:
                anomaly_cases = pickle.load(fr)
            with open(os.path.join(app_result_path, 'kpi', '%s_threshold.pickle' % (case['kpi'])), 'rb') as fr:
                threshold = pickle.load(fr)

            data_df = \
            pd.read_csv(os.path.join(data_dir, date, '%s.csv' % (case['app']))).drop(columns=['app_name']).dropna(
                axis='columns',
                how='all')[['timestamp', case['kpi']]]
            data_df[case['kpi']].fillna(data_df[case['kpi']].mean(), inplace=True)

            date_tmp = split_str(case['anomaly_time'])
            start = transfer_date_to_timestamp(date_tmp[0])
            end = transfer_date_to_timestamp(date_tmp[1])
            max_rate = get_max_threshold(start, end, threshold, data_df)
            result_list.append({'root_cause':case['root_cause'],'time_start':start,'time_end':end,'kpi':case['kpi'],'app':case['app'],'max_rate':max_rate})
    return result_list


def get_anomaly_case(anomaly_date,anomaly_detection_dir,valid=False):
    result_all = []
    valid_cases = get_valid_case(anomaly_date)
    # if valid:
    #     for case in valid_cases:
    #         max_rate = get_max_threshold(case[0], case[1], threshold, data_df)
    #         result_all.append(
    #             {'time_start': case[0], 'time_end': case[1], 'kpi': kpi, 'app': app, 'max_rate': max_rate})



    for app in os.listdir(os.path.join(anomaly_detection_dir,anomaly_date)):
        if app[0]=='.':
            continue
        app_result_path = os.path.join(anomaly_detection_dir,anomaly_date, app)
        for file in os.listdir(os.path.join(app_result_path, 'kpi')):
            if file.find('anomaly_time') + 1:
                #print("find it")
                kpi = file.split('_')[0]
                if not os.path.exists(os.path.join(app_result_path, 'kpi','%s_anomaly_case.pickle'%(kpi))):
                    #print("no case")
                    continue
                with open(os.path.join(app_result_path, 'kpi','%s_anomaly_case.pickle'%(kpi)),'rb') as fr:
                    anomaly_cases = pickle.load(fr)
                with open(os.path.join(app_result_path, 'kpi','%s_threshold.pickle'%(kpi)),'rb') as fr:
                    threshold = pickle.load(fr)

                data_df = pd.read_csv(os.path.join(data_dir,anomaly_date, '%s.csv'%(app))).drop(columns=['app_name']).dropna(axis='columns',
                                                                                                  how='all')[['timestamp', kpi]]
                data_df[kpi].fillna(data_df[kpi].mean(),inplace=True)
                for case in anomaly_cases:

                    #if {'app':app,'kpi':kpi,'anomaly_time':'%s-%s'%(timestamp_to_date(case[0]),timestamp_to_date(case[1]))} in valid_cases:
                    if valid:
                        if {'app': app, 'kpi': kpi, 'anomaly_time': '%s-%s' % (
                        timestamp_to_date(case[0]), timestamp_to_date(case[1]))} in valid_cases:

                            max_rate = get_max_threshold(case[0], case[1], threshold, data_df)
                            result_all.append({'time_start':case[0],'time_end':case[1],'kpi':kpi,'app':app,'max_rate':max_rate})
                    else:
                        max_rate = get_max_threshold(case[0], case[1], threshold, data_df)
                        result_all.append(
                            {'time_start': case[0], 'time_end': case[1], 'kpi': kpi, 'app': app, 'max_rate': max_rate})

    #print(result_all)
    return result_all

def get_anomaly_metric(anomaly_result, anomaly_time, time_range):
    result = []
    #print(anomaly_time)
    for metric, value in anomaly_result.items():
        time_length = time_range * 60 * 60 * 1000
        #print(value['timestamp'])
        df_detect = value[
            (value['timestamp'] > anomaly_time - time_length)] 
         #& (value['timestamp'] < anomaly_time + time_length)
        print('get anomaly metric df_detect:')
       # print(df_detect)
        if 1 in df_detect['anomaly_indicator'].unique():
            if metric.find('avg') + 1:
                result.append(re.match(r'.*avg\((.*)\)', metric).group(1))
            else:
                result.append(metric)
    return result

def get_anomaly_metric_spot(anomaly_result, anomaly_time_start, anomaly_time_end, time_range1, time_range2):
    result_all = {}
    #print(anomaly_time)
    
    for metric, value in anomaly_result.items():
        time_length1 = time_range1 * 60 * 60 * 1000
        time_length2 = time_range2 * 60 *60 * 1000
        #print(value['timestamp'])
        # anomaly_in = [timestamp for timestamp in value if (timestamp > anomaly_time - time_length)
        #               & (timestamp <= anomaly_time) ]
        anomaly_in = []
        for timestamp in value:
            if (timestamp > anomaly_time_start - time_length1)  & (timestamp <= anomaly_time_end + time_length2):
                anomaly_in.append(timestamp)
        if len(anomaly_in) != 0:
            result_all[metric]=sorted(anomaly_in)         
    return result_all


def trace_root_cause_spot(G, anomaly_metrics, kpi):
    anomaly_metrics_filter = []
    for item in anomaly_metrics:
        anomaly_metrics_filter.append(re.match(r'.*avg\((.*)\)', item).group(1))
    anomaly_metrics = anomaly_metrics_filter

    if not isinstance(kpi, list):
        kpi = [kpi]
    all_anomaly_parents = []
    for item_kpi in kpi:
        if item_kpi.find('avg') +1 :
            item_kpi = re.match(r'.*avg\((.*)\)', item_kpi).group(1)
        try:
            parents = list(G.predecessors(item_kpi))
            print('trace root cause parents:')
            print(parents)
        except:
            print('KPI: %s is not in the network' % (item_kpi))
            continue
        anomaly_parents = []
        while parents:
            new_parents = []
            new_anomaly_parents = []
            for item in parents:
                if item in anomaly_metrics:
                    new_anomaly_parents.append(item)
                    try:
                        new_parents = new_parents + list(G.predecessors(item))
                    except:
                        print('%s is not in the network' % (item))
                        continue
            print('new parents:')
            print(new_parents)

            if set(new_anomaly_parents).issubset(anomaly_parents):
                break
            else:
                anomaly_parents = list(set(anomaly_parents + new_anomaly_parents))
                parents = list(set(new_parents))

        all_anomaly_parents = list(set(all_anomaly_parents + anomaly_parents))

    all_anomaly_parents_filter = []
    for item in all_anomaly_parents:
        all_anomaly_parents_filter.append('avg(%s)'%(item))
    all_anomaly_parents = all_anomaly_parents_filter

    return all_anomaly_parents

def trace_root_cause(G, anomaly_metrics, kpi, log):
    if not isinstance(kpi, list):
        kpi = [kpi]
    all_anomaly_parents = []
    for item_kpi in kpi:
        if item_kpi.find('avg') +1 :
            item_kpi = re.match(r'.*avg\((.*)\)', item_kpi).group(1)
        try:
            parents = list(G.predecessors(item_kpi))
            print('trace root cause parents:')
            #print(parents)
        except:
            print('KPI: %s is not in the network' % (item_kpi), file=log)
            continue
        anomaly_parents = []
        while parents:
            new_parents = []
            new_anomaly_parents = []
            for item in parents:
                if item in anomaly_metrics:
                    new_anomaly_parents.append(item)
                    try:
                        new_parents = new_parents + list(G.predecessors(item))
                    except:
                        print('%s is not in the network' % (item), file=log)
                        continue
            print('new parents:')
            #print(new_parents)

            if set(new_anomaly_parents).issubset(anomaly_parents):
                break
            else:
                anomaly_parents = list(set(anomaly_parents + new_anomaly_parents))
                parents = list(set(new_parents))

        all_anomaly_parents = list(set(all_anomaly_parents + anomaly_parents))

    return all_anomaly_parents

def get_the_top_early_anomaly_spot(topn, anomaly_result, anomaly_parents):
    result = {}
    # result_metric_rate = {}
    for metric in anomaly_parents:
        result[metric] = sorted(anomaly_result[metric])[0]


    sort_result = sorted(result.items(), key=lambda d: d[1])

    return sort_result[0:topn]
def get_the_sorted_early_anomaly_spot( anomaly_result, anomaly_parents):
    result = {}
    # result_metric_rate = {}
    for metric in anomaly_parents:
        result[metric] = sorted(anomaly_result[metric])[0]


    sort_result = sorted(result.items(), key=lambda d: d[1])

    return sort_result
def get_the_top_max_rate_anomaly_spot(topn, anomaly_parents_rate):
    # result = {}
    # for metric in anomaly_parents:
    #     result[metric] = sorted(anomaly_result[metric])[0]


    sort_result = sorted(anomaly_parents_rate.items(), key=lambda d: d[1],reverse=True)

    return sort_result[0:topn]
def get_the_sorted_max_rate_anomaly_spot(anomaly_parents_rate):
    # result = {}
    # for metric in anomaly_parents:
    #     result[metric] = sorted(anomaly_result[metric])[0]


    sort_result = sorted(anomaly_parents_rate.items(), key=lambda d: d[1],reverse=True)

    return sort_result


def get_the_top_early_anomaly(topn, anomaly_result, anomaly_parents,flag_time):
    result = {}
    result_metric_rate={}
    for metric in anomaly_parents:
        
        for val in anomaly_result['avg(%s)' % (metric)]['timestamp'].values:
            if val>=flag_time-36000000 and val<flag_time:
                result[metric]=val
                break
        #result[metric] = anomaly_result['avg(%s)' % (metric)]['timestamp'].values[0]
    result_metric_rate=get_the_rate(result,flag_time)
    sort_result = sorted(result_metric_rate.items(), key=lambda d: d[1])

    return sort_result[0:topn], {k[0]:anomaly_result['avg(%s)' %(k[0])] for k in sort_result}


def save_anomaly_summary(file, result):
    with open(file, 'w') as fw:
        for key, value in result.items():
            print('Anomaly time for %s' % (key), file=fw)
            print(result[key], file=fw)