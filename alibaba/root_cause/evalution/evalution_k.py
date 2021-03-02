import os
import pickle
import pandas as pd
import numpy as np
import sys
sys.path.append("..")
from random_walk import *
from root_cause_utils import *
from utils import *
import re
import pytz,time
import matplotlib.pyplot as plt

anomaly_detection_dir = '/home/v-zrr19/alibaba/result/spot'
root_cause_result_dir = '/home/v-zrr19/alibaba/result/newEvalution/domain'
anomaly_detection_metric_dir = '/home/v-zrr19/alibaba/result/spotCurve'
#causal_graph_dir = '/home/v-zrr19/alibaba/result/tigramite/alibaba'
data_dir = '/home/v-zrr19/alibaba/data'
pc_causal_graph_dir = '/home/v-zrr19/alibaba/result/newPage/pc'
pcmci_causal_graph_dir = '/home/v-zrr19/alibaba/result/newPage/alibaba'
detected_result_path2="/home/v-zrr19/alibaba/result/newEvalution/score"
real_result_path='/home/v-zrr19/alibaba/result/evalution'

def get_v_metric(case):
    graph_time_range_before = 4
    graph_time_range_after = 1
    metric_anomaly_file = os.path.join(anomaly_detection_metric_dir, case['date'], case['app'], 'metric',
                                           'anomaly_time.pickle')
    with open(metric_anomaly_file, 'rb') as fr:
        metrics_anomaly_time = pickle.load(fr)
    anomaly_metrics = \
                get_anomaly_metric_spot(metrics_anomaly_time, case['time_start'], case['time_end'],
                                    graph_time_range_before,
                                    graph_time_range_after)
    return anomaly_metrics
def str_find_list(item,level_list):
    for l in level_list:
        if item.find(l)+1:
            return True
    return False
def get_index(all_list,check_list):
    result = []
    for item in check_list:
        result.append(all_list.index(item))
    return result
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
        level_list = ['.']
    for item in metric_list:
        if str_find_list(item,level_list) and item.find(types)+1:
            result.append(item)
    return result
def sort_list(list1):
    return sorted(list1, key = lambda x: x[1])   
def move_index(forward_list,back_list,all_list):
    for item in back_list[::-1]:
        index_back = item[1]
        if index_back<forward_list[-1][1]:
            all_list.remove(item)
            all_list.insert(all_list.index(forward_list[-1])+1,item)
    return all_list
def domain(e,one_case,file1,result,anomaly_metrics,parameter):
    rank_list = result['rootCause']
    up_qps_metrics = get_metric(rank_list, 'up', 'qps')
    up_rt_metrics = get_metric(rank_list, 'up', 'rt')
    down_qps_metrics = get_metric(rank_list, 'down', 'qps')
    down_rt_metrics = get_metric(rank_list, 'down', 'rt')
    sys_metrics = get_metric(rank_list, 'sys')
    jvm_metrics = get_metric(rank_list, 'jvm')
    rt_metrics = get_metric(rank_list, types='rt')
    success_metrics = get_metric(rank_list, types='success_rate')
    level1 = up_qps_metrics
    level2 = sys_metrics + jvm_metrics + down_qps_metrics
    level3 = rt_metrics + success_metrics
    level4 = list(set(rank_list)-set(level1)-set(level2)-set(level3))
    level1_top2 = result.loc[result['rootCause'].isin(level1)][0:2]
    level2_top2 = result.loc[result['rootCause'].isin(level2)][0:2]
    level3_top2 = result.loc[result['rootCause'].isin(level3)][0:2]
    level4_top2 = result.loc[result['rootCause'].isin(level4)][0:2]
    df_result = pd.concat([level1_top2,level2_top2,level3_top2,level4_top2])
    for index, row in df_result.iterrows():
        #print(type(anomaly_metrics))
        #print(anomaly_metrics)
        df_result['T_anomaly'] = anomaly_metrics[row['rootCause']][0]
    result_df = df_result.sort_values("T_anomaly",inplace=False)
    result_df['rank']=[index for index in range(1,result_df.shape[0]+1)]
    s_d_path=os.path.join(root_cause_result_dir,str(e),parameter,file1)
    if not os.path.exists(s_d_path):
        os.makedirs(s_d_path)
    with open(os.path.join(s_d_path,'%s_%s_%s_%s%s'%(one_case['date'],one_case['app']
                                     ,one_case['kpi'],one_case['anomaly_time'],'.pickle')),'wb') as fd:
        pickle.dump(result_df,fd)
    
    return result_df

def PR_K(e,detected_result_path,given_root_result,parameter,k=5,file1='',file2=''):
    case_length=len(given_root_result)
    tmp=0.0
    No_results_detected=[]
    exist_top2=[]
    exist_summary=[]
    Not_exist=[]
    summary_path=os.path.join(detected_result_path,file1+file2+'_'+str(k)+'_'+'result.txt')
    #if not os.path.exists(summary_path):
    #    os.makedirs(summary_path)
    with open(summary_path,'w') as fl:
        for one_case in given_root_result:
            if 'avg(system.mem.util)' in one_case['root_cause']:
                case_length=case_length-1
                continue
            date_tmp = split_str(one_case['anomaly_time'])
            start = transfer_date_to_timestamp(date_tmp[0])
            end = transfer_date_to_timestamp(date_tmp[1])
            v_case={'root_cause':one_case['root_cause'],'time_start':start,'time_end':end,
             'kpi':one_case['kpi'],'app':one_case['app'],'date':one_case['date']}
            anomaly_metrics=get_v_metric(v_case)
            #print(anomaly_metrics)
            #print(one_case['root_cause'])
            file_path=os.path.join(one_case['date'],one_case['app'],one_case['kpi'],one_case['anomaly_time'])
            tmpe=[one_case['anomaly_time'][0:11],'_',one_case['anomaly_time'][12:]]
            result_path=os.path.join(detected_result_path,file1,file2,'%s_%s_%s_%s%s'%
                                           (one_case['date'],one_case['app'],one_case['kpi'],''.join(tmpe),'.pickle'))
            with open(os.path.join(detected_result_path,'PR_'+str(k)+'_result.txt'),'a') as ft:
                if not os.path.exists(result_path):
                    print(result_path)
                    case_length=case_length-1
                    No_results_detected.append(file_path)
                    continue
                with open(os.path.join(result_path),'rb') as fr:
                    detected_case=pickle.load(fr)
                final_result = domain(e,one_case,file1,detected_case,anomaly_metrics,parameter)
                #print(detected_case)
                #print(case_length)
                final_result.loc[final_result['rootCause'].isin(one_case['root_cause']),'Ra']=1
    #with open(os.path.join(detected_result_path,'%s_%s_%s_%s%s'%(one_case['date'],one_case['app'],one_case['kpi'],one_case['anomaly_time'],'.pickle')),'wb') as fw:
    #    pickle.dump(detected_case,fw)
                Ra_sum=len(one_case['root_cause'])
                summary_sum=final_result['Ra'].sum()
                
                k_sum=final_result[final_result['rank']<=k]['Ra'].sum()
                
                if k_sum>=1:
                    exist_top2.append(file_path)
                elif summary_sum>=1:
                    exist_summary.append(file_path)
                    print(file_path,file=ft)
                    print('root cause:%s'%(one_case['root_cause']),file=ft)
                    print('root list:',file=ft)
                    print(file_path)
                    #print(detected_case[['rootCause','rate','visittime']])
                    #print(detected_case[['rootCause','rate','visittime']],file=ft)
                    print(final_result[['rootCause']])
                    print(final_result[['rootCause']],file=ft)
                else:
                    
                    Not_exist.append(file_path)
                    print(file_path,file=ft)
                    print('root cause:%s'%(one_case['root_cause']),file=ft)
                    print('root list:',file=ft)
                    print(file_path)
                    #print(detected_case[['rootCause','rate','visittime']])
                    #print(detected_case[['rootCause','rate','visittime']],file=ft)
                    print(final_result[['rootCause']])
                    print(final_result[['rootCause']],file=ft)
            
                min_value=0
                if Ra_sum!=0:
                    min_value=min(k,Ra_sum)
                
                else:
                    min_value=k
                tmp=tmp+k_sum/min_value
                              
            
        
        
        PR_K=tmp/case_length
     
        
    
        print('valid case:',file=fl)
        print(case_length,file=fl)
        print('Total number:',file=fl)
        print(tmp,file=fl)
        print('PR@K:',file=fl)
        print(PR_K,file=fl)
        print('No results detected:',file=fl)
        for j in No_results_detected:
            print(j,file=fl)
        #print(No_results_detected,file=fl)
        print('exist in top2:',file=fl)
        for j in exist_top2:
            print(j,file=fl)
        #print(exist_top2,file=fl)
        print('exist in summary:',file=fl)
        for j in exist_summary:
            print(j,file=fl)
        #print(exist_summary,file=fl)
        print('Not_exist:',file=fl)
        for j in Not_exist:
            print(j,file=fl)
        #print(Not_exist,file=fl)

    return PR_K    


def AVG_k(sum_k):
    mean_k=np.mean(sum_k)
    return mean_k

def one_order(r,prune,pcoor,beta,loop,rmkpi,cut):
    return 'random_cutgraph_cut_%r_prune_%r_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r' % (bool(int(cut)),bool(int(prune)),
                                                                                       r, beta, loop, bool(int(pcorr)), bool(int(rmkpi)))
    #return 'random_cutgraph_prune_%r_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r' % (bool(int(prune)),
                                     #  r, beta, loop, bool(int(pcorr)), bool(int(rmkpi)))
        

def second_order(r,beta,pcoor,cut,prune,loop,rmkpi):
    return 'random_second_cutgraph_cut_%r_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r_prune_%r' % (
                bool(int(cut)),r, beta, loop, bool(int(pcorr)), bool(int(rmkpi)),bool(int(prune)))


if __name__ == '__main__':
    r_s=[0.1]
    e_s=[0.1]
    #e_s=[0.8,0.9]
    #prune=1
    beta=1
    loop=1000
    #pcorr=0
    rmkpi=1
    cut=0
    with open (os.path.join(real_result_path,'given_root_result.pickle'),'rb') as fr:
        
        roots=pickle.load(fr)
    
    k_s=[1,2,3,4,5]
    #tu_parameters=['0.05_pearson_0.01','pc0.1_MCI0.05_tau_10_pearson_0.01','pc0.1_PC0.1_tau_10_pearson_0.01']
    tu_parameters=['before_4_end_1_pc0.1_PC0.1_tau_10'
                   ]
    for pcorr in [1]:
        for prune in [0]:
            for e in e_s:
                detected_result_path1=os.path.join(detected_result_path2,str(e))
                with open (os.path.join(detected_result_path1,'result.txt'),'a') as fa:
                    for r in r_s:
                        if beta==1:
                            parameter=one_order(r,prune,pcorr,beta,loop,rmkpi,cut)
                        else:
                            parameter=second_order(r,beta,pcorr,cut,prune,loop,rmkpi)
                        print(parameter,file=fa)
                        
                        detected_result_path=os.path.join(detected_result_path1,parameter)
                        sum_PR=[]
                        for tu_parameter in tu_parameters:
                            print(tu_parameter,file=fa)
                            for k in k_s:
                                PR=PR_K(e,detected_result_path,roots,parameter,k,tu_parameter)
                                print('PR_%s:%s'%(k,PR),file=fa)
                                sum_PR.append(PR)
                            #print('PR@%s'%(k),file=fa)
                            
                            print('Mean:%s'%(AVG_k(sum_PR)),file=fa)
                    