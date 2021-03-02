import sys
import os
import pickle
import pandas as pd
from datetime import datetime
import pytz
tz=pytz.timezone('Asia/Shanghai')
result_rate_dir='/home/v-zrr19/alibaba/result/evalution'
result_dir = "/home/v-zrr19/alibaba/result/newEvalution"
summary_dir = '/home/v-zrr19/alibaba/result/online/new'
anomaly_detection_metric_dir = '/home/v-zrr19/alibaba/result/spotCurve'
data_path = '/home/v-zrr19/alibaba/data'
time_range_before=5
time_range_after=1
sum_case=0
r_s=[0,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]
e_s=[0.1]
#r_s=[0.1]
#e_s=[0.2]
#e_s=[0.8,0.9]
dates=['1016','1021','1023','1218','0119']
#dates=['1016',]
#pcorr=0
#s
cut=0
beta=1
loop=1000
rmkpi=1

def one_order(r,prune,pcoor,beta,loop,rmkpi,cut):
    #return 'random_cutgraph_prune_%r_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r' % (bool(int(prune)),
                                       #r, beta, loop, bool(int(pcorr)), bool(int(rmkpi)))
    return 'random_cutgraph_cut_%r_prune_%r_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r' % (bool(int(cut)),bool(int(prune)),r, beta, loop, bool(int(pcorr)), bool(int(rmkpi)))
def second_order(r,beta,pcoor,cut,prune,loop,rmkpi):
    return 'random_second_cutgraph_cut_%r_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r_prune_%r' % (
                bool(int(cut)),r, beta, loop, bool(int(pcorr)), bool(int(rmkpi)),bool(int(prune)))

for pcorr in [1]:
    print('pcoor:')
    print(pcorr)
   #if beta==1:
   #    lis=range(2)
   #else:
    lis=range(1)
    for prune in [0]:
        print('prune')
        print(prune)
        for e in e_s:
            for r in r_s:
                if beta==1:
                    parameter=one_order(r,prune,pcorr,beta,loop,rmkpi,cut)
                else:
                    
                    parameter=second_order(r,beta,pcorr,cut,prune,loop,rmkpi)
                #parameter='random_cutgraph_prune_%r_r_%g_beta_%g_loop_%d_pcorr_%r_rmkpi_%r' % (bool(int(prune)),
                                               #r, beta, loop, bool(int(pcorr)), bool(rmkpi))
                
                print(parameter)
                for date in dates:
                    
                    for file in os.listdir(os.path.join(summary_dir,date)):
                        
                        for kpi in os.listdir(os.path.join(summary_dir,date,file)):
                            print(date) 
                            print(file)
                            print(kpi)
                            s1='Random walk rank result\n'
                            #s2='Graph:'+date+'_'+file+'_'+kpi+'_pc0.1_PC0.1_tau_10_pearson_0.01.dmp\n'
                            print(s1)
                            #print(s2)
                            
                            if not os.path.exists(os.path.join(summary_dir,date,file,kpi,parameter)):
                                print('llllllllll')
                                print(os.path.join(summary_dir,date,file,kpi,parameter))
                                continue
                            sum_case=sum_case+1
                            with open (os.path.join(summary_dir,date,file,kpi,parameter),'r') as selfg:
                                m=selfg.readlines()
                            with open(os.path.join(result_rate_dir,'rate',date,file,'4'+kpi+'.pickle'),'rb') as frate:
                                rates=pickle.load(frate)
                            sum_rate=rates['max_rate'].sum()
                            sum_visit_time=1000
                            try:
                                n=m.count('Random walk rank result\n')
                               # s2_index=m.index(s2)
                                if n==0:
                                    continue
                            except:
                                continue
                            index=[]
                            index_one=m.index('Random walk rank result\n')
                            index.append(index_one)
                            for i in range(n-1):
                                index_one=m.index("Random walk rank result\n", index_one + 1)
                                index.append(index_one)
                            print('index:')
                            print(index)
                            #metrics=[]
                            #rates=[]
                            #visit_times=[]
                            for j in range(n):
                                roots={}
                                #sum_rate=0
                                #sum_visit_time=0
                                content=[]
                                if j==n-1:
                                    #print('mmm')
                                    para=m[index[j]-1].split('_')[-8:]
                                   # print('mm')
                                    if para[-1]=='0.05.dmp\n':
                                        para=m[index[j]-1].split('_')[-5:]
                                    elif ':' in para[0]:
                                        para=m[index[j]-1].split('_')[-5:]
                                    elif para[0]=='tau':
                                        para=m[index[j]-1].split('_')[-6:]
                                    #elif para[0]=='4' or para[0]=='before':
                                      #  para=m[index[j]-1].split('_')[-5:]
                                    content=m[index[j]+1:]
                                else:
                                    #print('mmmmmm')
                                    para=m[index[j]-1].split('_')[-8:]
                                   # print('mmmm')
                                    if ':' in para[0]:
                                        if para[-1]=='0.05.dmp\n':
                                            para=m[index[j]-1].split('_')[-5:]
                                        else:
                                            para=m[index[j]-1].split('_')[-5:]
                                    elif para[0]=='tau':
                                        para=m[index[j]-1].split('_')[-6:]
                                    #elif para[0]=='4' or para[0]=='before':
                                       # para=m[index[j]-1].split('_')[-5:]
                                        
                                    content=m[index[j]+1:index[j+1]-1]
                                #print(content)
                                
                                for line in content:
                                    if line.split(':')[0]=='Graph' or line=='Kpi has no neighbors\n' or line=='KPI not in Graph\n' or line=='The graph is empty\n':
                                        continue
                                    #if line=='Kpi has no neighbors\n':
                                    #    break
                                    elif line=='Random walk rank result\n':
                                        continue
                                    elif line=='isit time:0\n' or line=='ime:0\n' or line=='\n' or line=='e:0\n':
                                        continue
                                    elif line.split(':')[0]!='metric':
                                        continue
                    
                                    else:
                                        metric=line.split(',')[0].split(':')[1]
                                        rate=line.split(',')[2].split(':')[1]
                                        #p_rate=rates[rates['metrics']==metric]['max_rate']
                                        ##print(p_rate.values)
                                        #if p_rate.values==[]:
                                        #    rate=line.split(',')[2].split(':')[1]
                                        #else:
                                        #    try:
                                        #        rate=p_rate.values[0]
                                        #    except:
                                        #        continue
                                        visit_time=line.split(',')[3].split(':')[1]  
                                    
                                    
                                    if float(rate)<=0.0:
                                        continue
                                    roots[metric]=[float(rate),float(visit_time)]
                                        #sum_rate=sum_rate+float(rate)
                                        #sum_visit_time=sum_visit_time+float(visit_time)
                                
                                        
                                       # metrics.append(metric)
                                       # rates.append(float(rate))
                                       # visit_times.append(float(visit_time))
                                if roots:
                                    #if sum_visit_time==0:
                                        #sum_visit_time=1
                                    for key in roots.keys():
                                        s_visit=roots[key][1]/sum_visit_time
                                        s_rate=roots[key][0]/sum_rate
                                        score=e*s_visit+(1-e)*s_rate
                                        roots[key]=[score,roots[key][0],roots[key][1],s_rate,s_visit]
                                    sort_roots=sorted(roots.items(), key=lambda item:item[1],reverse=True)
                                    #print(sort_roots)
                                    save_root=[]
                                    save_rate=[]
                                    save_visitime=[]
                                    save_score=[]
                                    save_s_rate=[]
                                    save_s_visit=[]
                                    for l in sort_roots:
                                        save_root.append(l[0])
                                        save_rate.append(l[1][1])
                                        save_visitime.append(l[1][2])
                                        save_score.append(l[1][0])
                                        save_s_rate.append(l[1][3])
                                        save_s_visit.append(l[1][4])
                                    
                                    root_length=len(save_root)
                                    root_rank=[index for index in range(1,root_length+1)]
                                    root_Ra=[0]*root_length
                                    root_causes=pd.DataFrame({'rootCause':save_root,'rank':root_rank,'Ra':root_Ra,'score':save_score,
                                                              'rate':save_rate,
                                                              's_rate':save_s_rate,'visittime':save_visitime,'s_visit':save_s_visit})
                                    #print(root_causes)
                                    if len(para)<=1:
                                        #save_path=os.path.join(result_dir,parameter,para[0].split('.')[0])
                                        save_path=os.path.join(result_dir,'score',str(e),parameter,
                                                               para[0].split('.')[0]+'.'+para[0].split('.')[1])
                                    elif len(para)==3:
                                        #save_path=os.path.join(result_dir,'score',str(e),parameter,'%s_%s_%s'%(para[0],
                                        #para[1],para[2].split('.')[0]))
                                        save_path=os.path.join(result_dir,'score',str(e),parameter
                                                               ,'%s_%s_%s'%(para[0],para[1],para[2].split('.')[0]+'.'+para[2].split('.')[1]))
                                    elif len(para)==2:
                                        #save_path=os.path.join(result_dir,'score',str(e),parameter,'%s_%s'%(para[0],para[1].split('.')[0]))
                                        save_path=os.path.join(result_dir,'score',str(e),parameter,
                                                               '%s_%s_%s'%(para[0],para[1].split('.')[0]+'.'+para[1].split('.')[1]))
                                    elif len(para)==5:
                                        #save_path=os.path.join(result_dir,'score',str(e),parameter,'%s_%s'%(para[0],para[1].split('.')[0]))
                                        save_path=os.path.join(result_dir,'score',str(e),parameter,
                                                               '%s_%s_%s_%s_%s'%(para[0],para[1],
                                                                                 para[2],para[3],
                                                                                 para[4].split('.')[0]+'.'+para[4].split('.')[1]))
                                    elif len(para)==6:
                                        #save_path=os.path.join(result_dir,'score',str(e),parameter,'%s_%s_%s_%s_%s_%s'%(para[0],para[1],para[2],para[3],para[4],para[5].split('.')[0]))                     
                                        save_path=os.path.join(result_dir,'score',str(e),parameter,'%s_%s_%s_%s_%s_%s'%(para[0],para[1],
                                                                                                para[2],para[3],para[4],
                                                                                    para[5].split('.')[0]+'.'+para[5].split('.')[1]))
                                    elif len(para)==8:
                                        save_path=os.path.join(result_dir,'score',str(e),parameter,'%s_%s_%s_%s_%s_%s_%s_%s'%
                                                               (para[0],para[1],para[2],para[3],para[4],para[5],para[6],
                                                                                    para[7].split('.')[0]))
                                    else:
                                        #save_path=os.path.join(result_dir,'score',str(e),parameter,'%s_%s_%s_%s'%(para[0],para[1],para[2],para[3].split('.')[0]))
                                        save_path=os.path.join(result_dir,'score',str(e),parameter,'%s_%s_%s_%s'%(para[0],para[1],
                                                               para[2],para[3].split('.')[0]+'.'+para[3].split('.')[1]))
                                    if not os.path.exists(save_path):
                                        os.makedirs(save_path)
            
                                    with open(os.path.join(save_path,'%s_%s_%s%s'%(date,file,kpi,'.pickle')),'wb') as fw:
                                        pickle.dump(root_causes,fw)
        print(sum_case)