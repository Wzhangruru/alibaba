import os,sys
os.environ["CVDA_VISIBLE_DEVICES"]="1"
import numpy as np
import matplotlib.pyplot as plt
import time
import pandas as pd
from spotrt import biSPOTR
from spotSuccess import biSPOTS
import pickle
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

hour = 240 
def processData(tmp_result_dir, last_day_begin, column_values, timestamp,success_indicator=False):
    """
    为了方便并行计算，按天处理数据并将数据写入文件中
    """

    previous_data = []

    last_day_end = last_day_begin + hour
    today_begin = last_day_end
    today_end = today_begin + hour

    last_day_data = column_values[last_day_begin:last_day_end]
    today_data = column_values[today_begin:today_end]
    # P1 = df_sequence.iloc[j:k, 1:2]
    # data = P1.values
    # P2 = df_sequence.iloc[i:j, 1:2]
    # init_data = P2.values
    #print(timestamp[last_day_begin], timestamp[last_day_end], timestamp[today_end])
    for val in column_values[:today_end]:
        previous_data.append(val)
    # previous_data = column_values[:today_end]
    q = 1e-5
    d = 10
    if not success_indicator:
        s = biSPOTR(q)
    else:
        s = biSPOTS(q)
    # print(type(last_day_data))
    # print(last_day_data)
    # print(type(today_data))
    # print(today_data)
    # last_day_data=last_day_data.swapaxes(0,1)[0]
    dir_t = os.path.join(tmp_result_dir, 'time_' + str(last_day_begin))
    dir_a = os.path.join(tmp_result_dir, 'anomaly_' + str(last_day_begin))
    tt = []
    try:
        s.fit(last_day_data, today_data)
        s.initialize(verbose=False)
        result1 = s.run()
    #regular_results = s.regular(result1, previous_data, today_begin)
    # print(results)
        if len(result1['alarms']) == hour:
            result1['alarms'] = []

        # lock.acquire()



        for alarm in result1['alarms']:
            tt.append(timestamp[today_begin + alarm])
        if not success_indicator:
            threshold_data =  result1['upper_thresholds']
        else:
            threshold_data = result1['lower_thresholds']
        # print('start time', last_day_begin,'threshold right',len(threshold_data))
    except:
        threshold_data = list(np.zeros(len(today_data)))
        # print('start time', last_day_begin, 'threshold error', len(threshold_data))
        # print(timestamp[today_begin + alarm])
        # anomaly_timestamp.append(timestamp[today_begin + alarm])
    np.savetxt(dir_a + '.txt', tt)
    # print('upper_thresholds', regular_results['upper_thresholds'])

    np.savetxt(dir_t + '.txt', threshold_data)
    # print('start time', last_day_begin, 'threshold final', len(threshold_data))

    # print(regular_results['upper_thresholds'])
    # threshold.extend(regular_results['upper_thresholds'])
    # lock.release()
    
def processFile(tmp_result_dir,day_num,timestamp):
    """
    将所得数据汇总并返回数组
    """
    anomaly_timestamp = []
    threshold = []
    an_ti = "anomaly_"
    thre = "time_"
    for i in range(0,day_num,hour):
        if os.path.exists(os.path.join(tmp_result_dir,an_ti + str(i) + '.txt')):
            if os.path.getsize(os.path.join(tmp_result_dir,an_ti + str(i) + '.txt')):
                m=np.loadtxt(os.path.join(tmp_result_dir,an_ti + str(i) + '.txt'))
                if m.shape==():
                    num=[float(m)]
                    anomaly_timestamp.extend(num)
                else:
                    anomaly_timestamp.extend(m.tolist())
                #anomaly_timestamp.append(af.read())
                
        if os.path.exists(os.path.join(tmp_result_dir,thre + str(i) + '.txt')):

            if os.path.getsize(os.path.join(tmp_result_dir,thre + str(i) + '.txt')):

                n=np.loadtxt(os.path.join(tmp_result_dir,thre + str(i) + '.txt'))
                if n.shape==():
                    num=[float(n)]
                    threshold.extend(num)
                else:
                    threshold.extend(n.tolist())
                    #threshold.extend(tf.read())
    threshold_df = pd.DataFrame({'timestamp': timestamp[hour:len(threshold)+hour], 'threshold': threshold})
    for i in os.listdir(tmp_result_dir):
        if i[0]=='.':
            continue
        os.remove(os.path.join(tmp_result_dir,i))
    os.rmdir(tmp_result_dir)
    return anomaly_timestamp, threshold_df

def spot_anomaly_metric(tmp_result_dir, column_values, timestamp,success_indicator):
    # anomaly_timestamp = []
    # threshold = []

    cores = multiprocessing.cpu_count()
    pool = ProcessPoolExecutor(max_workers=cores) 
    #pool = ProcessPoolExecutor(max_workers=16)

    # lock = multiprocessing.Manager().Lock()
    for last_day_begin in range(0, len(column_values), hour):
        # pool.submit(processData,i,column_values,df_sequence,fw_str)
        # pool.submit(processData,tmp_result_dir,lock,last_day_begin,column_values,timestamp)
        pool.submit(processData, tmp_result_dir, last_day_begin, column_values, timestamp,success_indicator)

        # for last_day_begin in range(0,len(column_values),hour):
        # threshold,anomaly_timestamp=processData(last_day_begin,column_values,timestamp,threshold,anomaly_timestamp)
    # pool.close()
    pool.shutdown(wait=True)

if __name__ == '__main__':

    #dates = ['0327','0513','0912','1015']
    dates =[sys.argv[1]]
    kpis = ['avg(web.rt)','avg(middleware.hsf.provider.rt)','avg(middleware.metaq.receive.rt)', 'avg(middleware.notify.receive.rt)']
    data_dir = '/home/v-yuan15/alibaba/data'
    
    #result_dir='/home/v-yuan15/alibaba/result/spot240' 
    result_dir='/home/v-yuan15/alibaba/result/spot' 
    
    result_metric_dir ='/home/v-yuan15/alibaba/result/spot240'  
    for date in dates:
        data_path = os.path.join(data_dir,date)
        date_result_dir =  os.path.join(result_dir,date)
        date_result_metric_dir =  os.path.join(result_metric_dir,date)
        for file in os.listdir(data_path):
            if os.path.splitext(file)[1] != '.csv' or os.path.splitext(file)[0] == 'label':
                continue
                
            # if file!='304899689973763048910.csv':
            #     continue
            anomaly_timestamp_all = {}
            threshold_all = {}
            if not os.path.exists(os.path.join(date_result_dir,os.path.splitext(file)[0],'kpi','anomaly_figure')):
                continue
            if len(os.listdir(os.path.join(date_result_dir,os.path.splitext(file)[0],'kpi','anomaly_figure'))) == 0:
                continue
            print(file)
            app_result_dir = os.path.join(date_result_metric_dir, os.path.splitext(file)[0], 'metric')
            if not os.path.exists(app_result_dir):
                os.makedirs(app_result_dir)
            # figure_result_dir = os.path.join(result_dir, 'anomaly_figure')
            # if not os.path.exists(figure_result_dir):
            #     os.makedirs(figure_result_dir)

            #               # read data
            df = pd.read_csv(os.path.join(data_path,file)).drop(columns=['app_name']).dropna(axis='columns',how='all')
            columns = df.drop(columns=['timestamp']).columns.values

            for column in columns:

                if column in kpis:
                    continue
                try:
                    df_sequence = df[['timestamp', column]]
                except:
                    continue

                
                # print(column)
                timestamp = df_sequence['timestamp'].values
                column_values = df_sequence[column].fillna(df_sequence[column].mean()).values
                
                if len(np.unique(column_values.reshape((-1,)))) == 1:
                    print('constant column')
                    continue
                if df_sequence[column].mean()<=0:
                    print('negative or zero clummn')
                    continue
                tmp_result_dir = os.path.join(app_result_dir,column)
                if not os.path.exists(tmp_result_dir):
                    os.makedirs(tmp_result_dir)
                
                     
                if column.find('success')+1:
                    spot_anomaly_metric(tmp_result_dir,column_values, timestamp, success_indicator=True)
                    anomaly_timestamp, threshold_df = processFile(tmp_result_dir,len(column_values),timestamp)
                    
                    # print('data length',len(column_values))
                    # # print(len(timestamp))
                    # print('threshold length',len(threshold_df))
                    # print('gap',len(column_values)-len(threshold_df))
                    
                    anomaly_timestamp_all[column] = anomaly_timestamp
                    threshold_all[column] = threshold_df
                else:
                    spot_anomaly_metric(tmp_result_dir,column_values, timestamp,success_indicator=False)
                    anomaly_timestamp, threshold_df = processFile(tmp_result_dir,len(column_values),timestamp)

                    # print('data length', len(column_values))
                    # # print(len(timestamp))
                    # print('threshold length', len(threshold_df))
                    # print('gap', len(column_values) - len(threshold_df))
                    anomaly_timestamp_all[column] = anomaly_timestamp
                    threshold_all[column] = threshold_df
            
            with open(os.path.join(app_result_dir, 'threshold.pickle'), 'wb') as fw:
                pickle.dump(threshold_all, fw)
            with open(os.path.join(app_result_dir, 'anomaly_time.pickle'), 'wb') as fw:
                pickle.dump(anomaly_timestamp_all, fw)
    print("yes")            
            
          
                