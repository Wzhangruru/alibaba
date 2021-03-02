#!usr/bin/python
import os
os.environ["CVDA_VISIBLE_DEVICES"]="1"
import numpy as np
import sys
sys.path.append(".")
sys.path.append("..")
import matplotlib.pyplot as plt
import time
from datetime import datetime
import pandas as pd
from anomaly_detection.spotrt import biSPOTR
from anomaly_detection.spotSuccess import biSPOTS
import pickle
import pytz
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

# from root_cause.root_cause.spot_root_cause import timestamp_to_date
# data_path = '/home/v-yuan15/tmp/zrr-EVT/anamoly_detection/0327A'
# result_path='/home/v-yuan15/tmp/zrr-EVT/anamoly_detection/root_result/0327'
# save_path='/home/v-yuan15/tmp/zrr-EVT/anamoly_detection/root_result/0327/anamoly_time'
deep_saffron = '#FF9933'
air_force_blue = '#5D8AA8'
tz=pytz.timezone('Asia/Shanghai')
def timestamp_to_date(timestamp):
    try:
        return datetime.fromtimestamp(timestamp,tz).strftime('%m-%d_%H:%M')
    except:
        return datetime.fromtimestamp((timestamp//1000),tz).strftime('%m-%d_%H:%M')

def spot_anomaly_kpi(column_values, timestamp):
    no_regular_anomaly = []
    no_regular_dates = []
    threshold = []
    last_day_begin = 0
    while last_day_begin + 1440 < len(column_values):
        last_day_end = last_day_begin + 1440
        today_begin = last_day_end
        today_end = today_begin + 1440

        today_data = column_values[today_begin:today_end]
        previous_data = column_values[:today_end]
        q = 1e-5
        d = 10
        s = biSPOTR(q) 
        daynum = int(last_day_begin//1440)+1
        timestamp_array = np.array(timestamp)
        anomaly_index = np.where(np.isin(timestamp_array,no_regular_anomaly)==True)[0]
        
        if daynum<=6:
            last_day_data = column_values[0:daynum*1440]
            last_day_data = np.delete(np.array(last_day_data),anomaly_index).tolist()
        else:
            last_day_data = column_values[(daynum-7)*1440:daynum*1440]
            arr=np.array(anomaly_index)-(daynum-7)*1440
            last_day_data = np.delete(np.array(last_day_data),arr[np.where(arr>=0)].tolist()).tolist()
        try:
            s.fit(last_day_data, today_data)
            s.initialize(verbose=False)
            result1 = s.run()
            # print(result1)
            # regular_results = s.regular(result1, previous_data, today_begin)
            # print(results)


            for alarm in result1['alarms']:
                no_regular_anomaly.append(timestamp[today_begin + alarm])
                no_regular_dates.append(column_values[today_begin+alarm])
                # print('upper_thresholds', regular_results['upper_thresholds'])
                # print('call')
            threshold.extend(result1['upper_thresholds'])
        except:
            threshold.extend(list(np.zeros(len(today_data))))
        last_day_begin = last_day_begin + 1440
    threshold_df = pd.DataFrame({'timestamp': timestamp[1440:1440+len(threshold)], 'threshold': threshold})
    no_regular_anomal_df=pd.DataFrame({'timestamp':no_regular_anomaly ,'data':no_regular_dates})
    return no_regular_anomal_df, threshold_df

def processByKpi(columns,kpis,df,app_result_dir,count,figure_result_dir):
    kpis = ['avg(web.rt)','avg(middleware.hsf.provider.rt)','avg(middleware.metaq.receive.rt)', 'avg(middleware.notify.receive.rt)']
    cores = multiprocessing.cpu_count()
    pool = ProcessPoolExecutor(max_workers=cores) 
    for kpi in kpis:
        try:
            df_sequence = df[['timestamp', kpi]]
        except:
            continue
        timestamp = df_sequence['timestamp'].values
        column_values = df_sequence[kpi].fillna(df_sequence[kpi].mean()).values
        if len(np.unique(column_values.reshape((-1,)))) == 1:
            print('constant column')
            continue
        if df_sequence[kpi].fillna(df_sequence[kpi].mean()).mean()<=0:
            print('negative or zero column')
            continue
        job = pool.submit(spot_anomaly_kpi,column_values, timestamp)
        no_regular_anomal_df,threshold_df = job.result()
        if no_regular_anomal_df.empty:
            continue
        else:
            with open(os.path.join(app_result_dir, '%s_no_regular_anomaly_time.pickle' % (kpi)), 'wb') as fw:
                pickle.dump(no_regular_anomal_df, fw)
               #anomaly_case = generate_anomaly_case(anomaly_timestamp)
            with open(os.path.join(app_result_dir, '%s_threshold.pickle' % (kpi)), 'wb') as fw:
                pickle.dump(threshold_df, fw)

            anomaly_cases,anomaly_one_time_case =regular(no_regular_anomal_df,column_values,timestamp)
            if len(anomaly_cases)!=0:
                with open(os.path.join(app_result_dir,'%s_anomaly_case.pickle'%(kpi)),'wb')as fw:
                    pickle.dump(anomaly_cases,fw)
                with open(os.path.join(app_result_dir,'%s_anomaly_one_time_case.pickle'%(kpi)),'wb')as fw:
                    pickle.dump(anomaly_one_time_case,fw)
                for i in range(0,len(anomaly_cases)):
                    generate_anomaly_figure(anomaly_cases[i][0],anomaly_cases[i][1], df_sequence, figure_result_dir, kpi, threshold_df, anomaly_one_time_case[i])
                count=count+len(anomaly_cases)
    pool.shutdown(wait = True)
    return count
def processData(tmp_result_dir, last_day_begin, column_values, timestamp):
    """
    为了方便并行计算，按天处理数据并将数据写入文件中
    """


    previous_data = []
    no_regular_anomaly_timestamp=[]
    no_regular_dates=[]
    
    last_day_end = last_day_begin + 1440
    today_begin = last_day_end
    today_end = today_begin + 1440
    last_day_data = column_values[last_day_begin:last_day_end]
    today_data = column_values[today_begin:today_end]
    # print(last_day_begin)
    # print(last_day_data,today_data)

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
    s = biSPOTR(q)
    # print(type(last_day_data))
    # print(last_day_data)
    # print(type(today_data))
    # print(today_data)
    # last_day_data=last_day_data.swapaxes(0,1)[0]
    dir_t = os.path.join(tmp_result_dir, 'threshold_' + str(last_day_begin))
    dir_a = os.path.join(tmp_result_dir, 'no_regular_anomaly_' + str(last_day_begin))
    dir_d = os.path.join(tmp_result_dir, 'no_regular_dates_' + str(last_day_begin))
    try:
        s.fit(last_day_data, today_data)
        s.initialize(verbose=False)
        result1 = s.run()
        for init_alarm in result1['alarms']:
            no_regular_anomaly_timestamp.append(timestamp[today_begin+init_alarm])
            no_regular_dates.append(column_values[today_begin+init_alarm])
        #regular_results = s.regular(result1, previous_data, today_begin)
        # print(results)

        #if len(regular_results['alarms']) == 1440:
            #regular_results['alarms'] = []
        # lock.acquire()
        print('threshold_' + str(last_day_begin))
        threshold = result1['upper_thresholds']

    except:
        threshold = list(np.zeros(len(today_data)))

    #tt = []
    #for alarm in regular_results['alarms']:
        #tt.append(timestamp[today_begin + alarm])
        # print(timestamp[today_begin + alarm])
        # anomaly_timestamp.append(timestamp[today_begin + alarm])
    #np.savetxt(dir_a + '.txt', tt)
    np.savetxt(dir_a + '.txt', no_regular_anomaly_timestamp)
    # print('upper_thresholds', regular_results['upper_thresholds'])
    np.savetxt(dir_d + '.txt', no_regular_dates)
    np.savetxt(dir_t + '.txt', threshold)
    # print('in last time begin', )
    # print('in last time begin ', last_day_begin,len(result1['upper_thresholds']))
    # print(regular_results['upper_thresholds'])
    # threshold.extend(regular_results['upper_thresholds'])
    # lock.release()

def processFile(tmp_result_dir, day_num, timestamp):
    """
    将所得数据汇总并返回数组
    """
    no_regular_anomaly = []
    no_regular_date=[]
    threshold = []
    an_ti = "no_regular_anomaly_"
    an_da="no_regular_dates_"
    thre = "threshold_"
    print('day_num:')
    print(day_num)
    for i in range(0, day_num, 1440):
        if os.path.exists(os.path.join(tmp_result_dir, an_ti + str(i) + '.txt')):
            if os.path.getsize(os.path.join(tmp_result_dir, an_ti + str(i) + '.txt')):
                m = np.loadtxt(os.path.join(tmp_result_dir, an_ti + str(i) + '.txt'))
                
                if m.shape==():
                    num=[float(m)]
                    no_regular_anomaly.extend(num)
                else:
                    no_regular_anomaly.extend(m.tolist())
        
                # anomaly_timestamp.append(af.read())
        if os.path.exists(os.path.join(tmp_result_dir, an_da + str(i) + '.txt')):
            if os.path.getsize(os.path.join(tmp_result_dir, an_da + str(i) + '.txt')):
                l = np.loadtxt(os.path.join(tmp_result_dir, an_da + str(i) + '.txt'))
                if l.shape==():
                    da=[float(l)]
                    no_regular_date.extend(da)
                else:
                    no_regular_date.extend(l.tolist())
        if os.path.exists(os.path.join(tmp_result_dir, thre + str(i) + '.txt')):
            # print('success', thre+str(i))
            if os.path.getsize(os.path.join(tmp_result_dir, thre + str(i) + '.txt')):
                # print('success 2', thre + str(i))
                n = np.loadtxt(os.path.join(tmp_result_dir, thre + str(i) + '.txt'))
                if n.shape==():
                    da=[float(n)]
                    threshold.extend.extend(da)
                else:
                    threshold.extend(n.tolist())
            # threshold.extend(tf.read())

    # print(anomaly_timestamp)
    # print('len timestamp',len(timestamp))
    # #print(len(threshold))
    # print('len threshold',len(threshold))
    if len(threshold)!= 0:
        threshold_df = pd.DataFrame({'timestamp': timestamp[1440:1440+len(threshold):], 'threshold': threshold})
    else:
        threshold_df = pd.DataFrame({'timestamp': [], 'threshold': threshold})
    no_regular_anomal_df=pd.DataFrame({'timestamp':no_regular_anomaly ,'data':no_regular_date})
    for i in os.listdir(tmp_result_dir):

        if os.path.splitext(i)[0]=='.ipynb_checkpoints':
            continue
        else:
            os.remove(os.path.join(tmp_result_dir, i))
    os.rmdir(tmp_result_dir)
    return no_regular_anomal_df, threshold_df


def spot_anomaly_kpi_multiprocess(tmp_result_dir, column_values, timestamp):
    # anomaly_timestamp = []
    # threshold = []

    cores = multiprocessing.cpu_count()
    pool = ProcessPoolExecutor(max_workers=16)
    #pool = ProcessPoolExecutor(max_workers=16)暂时只开16个进程

    # lock = multiprocessing.Manager().Lock()
    #print(len(column_values))
    for last_day_begin in range(0, len(column_values), 1440):
        # print('last_day_begin:')
        # print(last_day_begin)
        # pool.submit(processData,i,column_values,df_sequence,fw_str)
        # pool.submit(processData,tmp_result_dir,lock,last_day_begin,column_values,timestamp)
        pool.submit(processData, tmp_result_dir, last_day_begin, column_values, timestamp)

        # for last_day_begin in range(0,len(column_values),1440):
        # threshold,anomaly_timestamp=processData(last_day_begin,column_values,timestamp,threshold,anomaly_timestamp)
    # pool.close()
    pool.shutdown(wait=True)
    
#def spot_anomaly_kpi(column_values, timestamp):
    #anomaly_timestamp = []
    #threshold = []

    #last_day_begin = 0
    #while last_day_begin + 1440 < len(column_values):
        #last_day_end = last_day_begin + 1440
        #today_begin = last_day_end
        #today_end = today_begin + 1440

        #last_day_data = column_values[last_day_begin:last_day_end]
        #today_data = column_values[today_begin:today_end]
        #previous_data = column_values[:today_end]
        #q = 1e-5
        #d = 10
        #s = biSPOTR(q)
        #s.fit(last_day_data, today_data)
        #s.initialize(verbose=False)
        #result1 = s.run()
        #regular_results = s.regular(result1, previous_data, today_begin)
        # print(results)

        #if len(regular_results['alarms']) == 1440:
            #regular_results['alarms'] = []

        #for alarm in regular_results['alarms']:
            #anomaly_timestamp.append(timestamp[today_begin + alarm])
        # print('upper_thresholds', regular_results['upper_thresholds'])
        #threshold.extend(regular_results['upper_thresholds'])
        #last_day_begin = last_day_begin + 1440
    #threshold_df = pd.DataFrame({'timestamp': timestamp[-len(threshold):], 'threshold': threshold})
   # return anomaly_timestamp, threshold_df

def generate_anomaly_case(anomaly_timestamp):
    # todo:
    i=0
    anomaly_cases=[]
    
    # with open(os.path.join(result_dir, '%s_anomaly_time.pickle' % (kpi)), 'rb') as fr:
    #     anomaly_timestamp=pickle.load(fr

    anomaly_timestamp.sort()

    case_begin = 0
    case_end = 0
    for anomaly_time in anomaly_timestamp:
        if case_begin==0:
            case_begin = anomaly_time
            continue
        elif case_end==0:
            case_end = anomaly_time
            continue
        elif anomaly_time-case_end <= 3600000:
            case_end = anomaly_time
        elif anomaly_time-case_end > 3600000:
            anomaly_cases.append([case_begin,case_end])
            case_end = 0
            case_begin = anomaly_time

    anomaly_cases.append([case_begin, case_end])

    
    return anomaly_cases


def transfer_time(timestamp_list):
    anomaly_data_time = []
    for anomalytime in timestamp_list:
        dt = datetime.fromtimestamp((anomalytime // 1000),tz).strftime('%m-%d %H:%M')
        anomaly_data_time.append(dt)
    return anomaly_data_time


def generate_anomaly_figure(start_time,end_time, df_sequence, figure_result_dir, kpi, threshold_df, anomaly_timestamp):
    histroy_days = 1
    begintimestamp = start_time
    endtimestamp = end_time
    star_timestamp = begintimestamp - histroy_days * 24 * 60 * 60000
    if star_timestamp < df_sequence['timestamp'][0]:
        star_timestamp = df_sequence['timestamp'][0] / 1000
    else:
        star_timestamp = star_timestamp / 1000

    endtimestamp = (endtimestamp + 3600000) / 1000

    lis = transfer_time(range(int(star_timestamp * 1000), int(endtimestamp * 1000), 60000))
    threshold = threshold_df[(threshold_df['timestamp'] >= (star_timestamp * 1000))
                             & (threshold_df['timestamp'] < (endtimestamp * 1000))]
    anomaly_time = [int(timestamp) for timestamp in anomaly_timestamp if
                    (int(timestamp) >= (int(star_timestamp * 1000)) & int(timestamp) < (int(endtimestamp * 1000)))]

    normal_data = df_sequence[(df_sequence['timestamp'] >= (star_timestamp * 1000))
                              & (df_sequence['timestamp'] < (endtimestamp * 1000))]
    anomaly_data_time = transfer_time(anomaly_time)

    plt.figure(figsize=(60, 20))
    plt.plot(transfer_time(normal_data['timestamp']), normal_data[kpi], color=air_force_blue)

    plt.plot(transfer_time(threshold['timestamp']), threshold['threshold'], color=deep_saffron, lw=2, ls='dashed')
    plt.scatter(anomaly_data_time, df_sequence.loc[df_sequence['timestamp'].isin(anomaly_time)][kpi].values, color='red')
    plt.subplots_adjust(left=0.05, bottom=0.2)
    plt.xticks(range(0, len(lis), 60), rotation=60)
    plt.savefig(os.path.join(figure_result_dir, '%s_%s_%s_%s'
                             % (kpi, timestamp_to_date(start_time), timestamp_to_date(end_time), '.pdf')))
    plt.show()
    plt.close()
    
def generate_anomaly_figure_double(start_time,end_time, df_sequence, figure_result_dir, kpi, threshold_df, anomaly_timestamp):
    histroy_days = 1
    begintimestamp = start_time
    endtimestamp = end_time
    star_timestamp = begintimestamp - histroy_days * 24 * 60 * 60000
    if star_timestamp < df_sequence['timestamp'][0]:
        star_timestamp = df_sequence['timestamp'][0] / 1000
    else:
        star_timestamp = star_timestamp / 1000

    endtimestamp = (endtimestamp + 3600000) / 1000
    threshold_df_up = threshold_df['upper']
    threshold_df_low = threshold_df['lower']
    lis = transfer_time(range(int(star_timestamp * 1000), int(endtimestamp * 1000), 60000))
    threshold_up = threshold_df_up[(threshold_df_up['timestamp'] >= (star_timestamp * 1000))
                             & (threshold_df_up['timestamp'] < (endtimestamp * 1000))]
    threshold_low = threshold_df_low[(threshold_df_low['timestamp'] >= (star_timestamp * 1000))
                             & (threshold_df_low['timestamp'] < (endtimestamp * 1000))]
    anomaly_time = [int(timestamp) for timestamp in anomaly_timestamp if
                    (int(timestamp) >= (int(star_timestamp * 1000)) & int(timestamp) < (int(endtimestamp * 1000)))]

    normal_data = df_sequence[(df_sequence['timestamp'] >= (star_timestamp * 1000))
                              & (df_sequence['timestamp'] < (endtimestamp * 1000))]
    anomaly_data_time = transfer_time(anomaly_time)

    plt.figure(figsize=(60, 20))
    plt.plot(transfer_time(normal_data['timestamp']), normal_data[kpi], color=air_force_blue)

    plt.plot(transfer_time(threshold_up['timestamp']), threshold_up['threshold'], color=deep_saffron, lw=2, ls='dashed')
    plt.plot(transfer_time(threshold_low['timestamp']), threshold_low['threshold'], color=deep_saffron, lw=2, ls='dashed')
    plt.scatter(anomaly_data_time, df_sequence.loc[df_sequence['timestamp'].isin(anomaly_time)][kpi].values, color='red')
    plt.subplots_adjust(left=0.05, bottom=0.2)
    plt.xticks(range(0, len(lis), 60), rotation=60)
    plt.savefig(os.path.join(figure_result_dir, '%s_%s_%s_%s'
                             % (kpi, timestamp_to_date(start_time), timestamp_to_date(end_time), '.pdf')))
    plt.show()
    plt.close()
    
def regular(no_regular_anamoly,column_values,timestamp_df):
    timestamps=[]
    for tim in timestamp_df:
        timestamps.append(tim)
    i=0
    l=0
    for data in no_regular_anamoly['data'][i:]:
        
        if abs(float(data))>=200:
            l=l+1
        else:
            remove_timestamp_df=no_regular_anamoly.loc[no_regular_anamoly['data']==data]
            remove_timestamp=remove_timestamp_df['timestamp']
            no_regular_anamoly=no_regular_anamoly[~no_regular_anamoly['timestamp'].isin(list(remove_timestamp))]
            i=l
            continue
    
    m=0
    n=0
    for date in no_regular_anamoly['data'][n:]:
               
        m=n
        max=-1
        find_timestamp=no_regular_anamoly.loc[no_regular_anamoly['data']==date]
        
        find_index=timestamps.index(list(find_timestamp['timestamp'])[0])
        
        if find_index<1440:
            for da in column_values[:find_index]:
                if da>max:
                    max=da  
        else:
            for da in column_values[find_index-10080:find_index]:
                if da>max:
                    max=da
        
        if date<max:
            no_regular_anamoly=no_regular_anamoly[~no_regular_anamoly['timestamp'].isin([list(find_timestamp['timestamp'])[0]])]
            n=m
            continue
    print(no_regular_anamoly)
    anomaly_timestamp=[]
    for timestamp in no_regular_anamoly['timestamp']:
        anomaly_timestamp.append(timestamp)
    anomaly_cases=[]
    anomaly_one_case=[]
    anomaly_one_time_case=[]
    case_begin = 0
    case_end = 0
    for anomaly_time in anomaly_timestamp:
            
        if (case_begin==0) and (case_end==0):
            case_begin = anomaly_time
            case_end=anomaly_time
            anomaly_one_case.append(case_begin)
            continue
        elif anomaly_time-case_end <= 3600000:
            case_end = anomaly_time
            anomaly_one_case.append(case_end)
            
        elif anomaly_time-case_end > 3600000:
            
            if len(anomaly_one_case)>=3 and case_begin!=case_end:
                print('one case:')
                print(anomaly_one_case)
                anomaly_cases.append([case_begin,case_end])
                anomaly_one_time_case.append(anomaly_one_case)
            anomaly_one_case=[]
            case_end = anomaly_time
            case_begin = anomaly_time
            anomaly_one_case.append(case_begin)
    if len(anomaly_one_case)>=3 and case_begin!=case_end:
        anomaly_cases.append([case_begin,case_end])
        anomaly_one_time_case.append(anomaly_one_case)
    
    return anomaly_cases,anomaly_one_time_case
if __name__ == '__main__':
    #dates = ['1015'] 
    kpis=['avg(middleware.metaq.receive.rt)','avg(web.rt)',
                'avg(middleware.hsf.provider.rt)','avg(middleware.notify.receive.rt)']
    
    
    
    data_dir = '/home/v-yuan15/alibaba/data'
    result_dir='/home/v-yuan15/alibaba/result/spotMore'
    count=0

    dates = sys.argv[1:];
    for date in dates:
        print(date)
        data_path = os.path.join(data_dir,date)
        date_result_dir =  os.path.join(result_dir,date)
        
        for file in os.listdir(data_path):
            if os.path.splitext(file)[1] == '.csv' and os.path.splitext(file)[0] != 'label'  :
                # file = '89908911891437689168992891989103768911891689968992.csv'
                print(file)
                #data_file = os.path.join(file)
                app_result_dir = os.path.join(date_result_dir, os.path.splitext(file)[0], 'kpi')
                
                if not os.path.exists(app_result_dir):
                    os.makedirs(app_result_dir)
                
                figure_result_dir = os.path.join(app_result_dir, 'anomaly_figure')
                #figure_result_dir = os.path.join(app_result_save_dir, 'anomaly_figure')
                if not os.path.exists(figure_result_dir):
                    os.makedirs(figure_result_dir)

                #               # read data
                
                
                
                
                #multiprocess by curve
                df = pd.read_csv(os.path.join(data_path,file)).drop(columns=['app_name']).dropna(axis='columns',how='all')
                #count = processByKpi(columns,kpis,df,app_result_dir,count,figure_result_dir)
                
                for kpi in kpis:
                   
                    try:
                        df_sequence = df[['timestamp', kpi]]
                    except:
                        continue
                    print(kpi)

                    timestamp = df_sequence['timestamp'].values
                    column_values = df_sequence[kpi].fillna(df_sequence[kpi].mean()).values
                    #print(column_values)
                    if len(np.unique(column_values.reshape((-1,)))) == 1:
                        print('constant column')
                        continue
                    # anomaly_timestamp, threshold_df = spot_anomaly_kpi(column_values, timestamp)
                    #tmp_result_dir = os.path.join(app_result_dir,kpi)
                    #if not os.path.exists(tmp_result_dir):
                        #os.makedirs(tmp_result_dir)
                    #spot_anomaly_kpi_multiprocess(tmp_result_dir,column_values, timestamp)
                    
                    #no_regular_anomal_df, threshold_df = processFile(tmp_result_dir,len(column_values),timestamp)
                    
                    no_regular_anomal_df, threshold_df = spot_anomaly_kpi(column_values, timestamp)
                    #print(anomaly_timestamp)
                    if no_regular_anomal_df.empty:
                        continue
                    else:
                        with open(os.path.join(app_result_dir, '%s_no_regular_anomaly_time.pickle' % (kpi)), 'wb') as fw:
                            pickle.dump(no_regular_anomal_df, fw)
                        #anomaly_case = generate_anomaly_case(anomaly_timestamp)
                        with open(os.path.join(app_result_dir, '%s_threshold.pickle' % (kpi)), 'wb') as fw:
                            pickle.dump(threshold_df, fw)
                            
                        anomaly_cases,anomaly_one_time_case =regular(no_regular_anomal_df,column_values,timestamp)
                        if len(anomaly_cases)!=0:
                            with open(os.path.join(app_result_dir,'%s_anomaly_case.pickle'%(kpi)),'wb')as fw:
                                pickle.dump(anomaly_cases,fw)
                            with open(os.path.join(app_result_dir,'%s_anomaly_one_time_case.pickle'%(kpi)),'wb')as fw:
                                pickle.dump(anomaly_one_time_case,fw)
                            for i in range(0,len(anomaly_cases)):
                                generate_anomaly_figure(anomaly_cases[i][0],anomaly_cases[i][1], df_sequence, figure_result_dir, kpi, threshold_df, anomaly_one_time_case[i])
                            count=count+len(anomaly_cases)
                            
                        
                        
                        
    print('the count is:')
    print(count)
                        #generate anomaly graph
                        #for case in anomaly_case:
                            #print(case)
                           # generate_anomaly_figure(case[0],case[1], df_sequence, figure_result_dir, kpi, threshold_df,
                                                #anomaly_timestamp)
                            
# ['avg(web.rt)','avg(middleware.hsf.provider.rt)','avg(middleware.metaq.receive.rt)', #'avg(middleware.notify.receive.rt)']


