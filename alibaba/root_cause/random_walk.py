import networkx as nx
import numpy as np
import pandas as pd
import random
import sys
import os
import pingouin as pg

    
def remove_self(list_a,a):
    list_a  = list(list_a)
    if a in list_a:
        list_a.remove(a)
        return list_a
    else:
        return list_a

def C(G, v_j, v_fe):
    '''
    计算两曲线相似度
    G nx.DiGraph() 根因定位图,每个节点具有一个属性字典 字典中有一个属性 键为timelist值为一个列表 记录曲线
    v_j v_fe 节点名称
    返回相似度
    '''
    Tj = G.nodes[v_j]['timelist']
    Tfe = G.nodes[v_fe]['timelist']



    if len(Tj) != len(Tfe):
        print("length not match")
        return 0

    n = len(Tj)
    sum_x = sum(Tj)
    sum_y = sum(Tfe)
    sum_x2 = sum([x * x for x in Tj])
    sum_y2 = sum([y * y for y in Tfe])
    sum_xy = sum([Tj[i] * Tfe[i] for i in range(n)])
    if ((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2))==0:
        val = 0
    else:
        val = (n * sum_xy - sum_x * sum_y) / ((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2)) ** 0.5
    
    return abs(val)

def generate_df(G):
    dict_tmp = {}
    for i in G.nodes():
        dict_tmp[i] = G.nodes[i]['timelist']
    return pd.DataFrame(dict_tmp)

#def partial_corr_c(G,v_j, v_fe,df,corr_type):
#    # print(G)
#    if v_j == v_fe:
#        return 1.
#    # print(v_j, v_fe)
#
#    # Tj = G.nodes[v_j]['timelist']
#    # Tfe = G.nodes[v_fe]['timelist']
#
#    Pa_Tj = remove_self(G.pred[v_j],v_j)
#    Pa_Tfe = remove_self(G.pred[v_fe],v_fe)
#
#    confounder = list(set(Pa_Tj) | set(Pa_Tfe))
#    if v_fe in confounder:
#        confounder.remove(v_fe)
#    if v_j in confounder:
#        confounder.remove(v_j)
#
#    # print(confounder)
#
#
#    # df = df.astype('float')
#    # print(df[confounder].dtypes)
#    # # # print(df)
#    # print(df[v_j].dtypes)
#    # print(df[v_fe])
#    # print(pg.partial_corr(data=df, x=v_j, y=v_fe, covar=confounder))
#    # print('df_v_j',df[v_j])
#    # print('df_v_fe',df[v_fe])
#    # print('df_con',df[confounder])
#    
#    return abs(pg.partial_corr(data=df, x=v_j, y=v_fe, covar=confounder, method=corr_type)['r'].values[0])   
def partial_corr_c(G,v_j, v_fe,df,corr_type):
    # print(G)
    if v_j == v_fe:
        return 1.
    # print(v_j, v_fe)

    # Tj = G.nodes[v_j]['timelist']
    # Tfe = G.nodes[v_fe]['timelist']

    Pa_Tj = remove_self(G.pred[v_j],v_j)
    Pa_Tfe = remove_self(G.pred[v_fe],v_fe)

    confounder = list(set(Pa_Tj) | set(Pa_Tfe))
    if v_fe in confounder:
        confounder.remove(v_fe)
    if v_j in confounder:
        confounder.remove(v_j)

    # print(confounder)
    
    if len(df[v_fe].unique())==1:
        return 0.
    if len(df[v_j].unique())==1:
        return 0.
    for item in confounder:
        if len(df[item].unique())==1:
            confounder.remove(item)
    
    
    
    return abs(pg.partial_corr(data=df, x=v_j, y=v_fe, covar=confounder, method=corr_type)['r'].values[0])

def probablity_matrix(G, v_fe,r,p_corr=False,remove_kpi=False,corr_type='pearson'):
    '''
    计算概率矩阵
    v_fe 前端节点
    G nx.DiGraph() 根因定位图
    return
    P pd.DataFrame n*n大小 标准化的概率矩阵 由相似性度量仅有P[i][i.succ]非0
    '''
    n = len(G.nodes)
    df = generate_df(G)
    P = pd.DataFrame(np.zeros((n, n), dtype=np.float64), index=G.nodes(), columns=G.nodes())
    for i in G.nodes():
        successor = remove_self(G.succ[i], i)
        for j in successor:
            if p_corr:
                P[i][j] = r * partial_corr_c(G, j, v_fe, df,corr_type)
            else:
                P[i][j] = r * C(G, j, v_fe)
        if remove_kpi:
            parents_i = remove_self(remove_self(G.pred[i],i),v_fe)
        else:
            parents_i = remove_self(G.pred[i],i)
        for j in parents_i:
            if p_corr:
                P[i][j] = partial_corr_c(G, j, v_fe,df,corr_type)
            else:
                P[i][j] = C(G, j, v_fe)

        if p_corr:
            c_self = partial_corr_c(G, i, v_fe, df,corr_type)
        else:
            c_self = C(G, i, v_fe)
        # print('max',P[i].max())
        # print('c self',c_self)
        if c_self> P[i].max():
            P[i][i] = c_self - P[i].max()

        s = P[i].sum()
        if s==0:
            continue
        for j in set(parents_i+successor):
            P[i][j] = P[i][j] / s
        P[i][i] = P[i][i] / s
        # print(P[i].sum())
    return P


def random_pick(some_list, probabilities):
    '''
    somelist 项列表
    probablities 概率列表
    return
    返回所选的项
    '''
    x = random.uniform(0, 1)
    cumulative_probability = 0.0
    for item, item_probability in zip(some_list, probabilities):
        cumulative_probability += item_probability
        if x < cumulative_probability: break
    return item
def random_walk_test(G, P, v_fe, r, beta, num_loop,remove_kpi):
    '''
    G nx.DiGraph() 根因定位图
    P pd.DataFrame n*n大小 标准化的概率矩阵 由相似性度量仅有 P[i][i.succ]非0
    v_fe 前端节点
    r   参数rou
    beta 参数beta
    num_loop 循环次数
    n  节点数
    m  边数
    v_s 当前节点
    v_p 前一个节点
    M pd.DataFrame n*n大小 转移矩阵
    R 字典 以节点名为键 出现次数为值
    return
    返回一个排序后的列表
    '''
    # 初始化
    v_s = v_fe
    # v_p = v_fe
    # n = len(G.nodes())
    R = dict.fromkeys(G.nodes(), 0)
    # M = pd.DataFrame(np.zeros((n, n), dtype=np.float64), index=G.nodes(), columns=G.nodes())

    # 循环随机游走
    for i in range(num_loop):
        # v_p = v_pre
        #
        # if remove_kpi:
        #     parents_v_s = remove_self(remove_self(G.pred[v_s], v_s), v_fe)
        # else:
        #     parents_v_s = remove_self(G.pred[v_s], v_s)
        # for v_next in parents_v_s:
        #     M[v_s][v_next] += (1 - beta) * P[v_p][v_s] + beta * P[v_s][v_next]
        # for v_before in remove_self(G.succ[v_s],v_s):
        #     M[v_s][v_before] += r * (1 - beta) * P[v_p][v_s] + r * beta * P[v_before][v_s]
        # if M[v_s].max() < P[v_p][v_s] * (1 - beta):
        #     M[v_s][v_s] = P[v_p][v_s] * (1 - beta) - M[v_s].max()
        # s = M[v_s].sum()
        # for l in M.index:
        #     M[v_s][l] /= s
        # print(M[v_s])
        # print('start v_s',i,v_s)
        # print(P.index.tolist())
        # print(P[v_s])
        v_s = random_pick(P.index.tolist(), P[v_s].values)
        R[v_s] += 1

        #print('end v_s',i,v_s)


    R_order = sorted(R.items(), key=lambda x: x[1], reverse=True)
    return R_order

def random_walk(G, P, v_fe, r, beta, num_loop,remove_kpi):
    '''
    G nx.DiGraph() 根因定位图
    P pd.DataFrame n*n大小 标准化的概率矩阵 由相似性度量仅有 P[i][i.succ]非0
    v_fe 前端节点
    r   参数rou
    beta 参数beta
    num_loop 循环次数
    n  节点数
    m  边数
    v_s 当前节点
    v_p 前一个节点
    M pd.DataFrame n*n大小 转移矩阵
    R 字典 以节点名为键 出现次数为值
    return
    返回一个排序后的列表
    '''
    # 初始化
    v_s = v_fe
    # v_p = v_fe
    # n = len(G.nodes())
    R = dict.fromkeys(G.nodes(), 0)
    # M = pd.DataFrame(np.zeros((n, n), dtype=np.float64), index=G.nodes(), columns=G.nodes())

    # 循环随机游走
    for i in range(num_loop):
        # v_p = v_pre
        #
        # if remove_kpi:
        #     parents_v_s = remove_self(remove_self(G.pred[v_s], v_s), v_fe)
        # else:
        #     parents_v_s = remove_self(G.pred[v_s], v_s)
        # for v_next in parents_v_s:
        #     M[v_s][v_next] += (1 - beta) * P[v_p][v_s] + beta * P[v_s][v_next]
        # for v_before in remove_self(G.succ[v_s],v_s):
        #     M[v_s][v_before] += r * (1 - beta) * P[v_p][v_s] + r * beta * P[v_before][v_s]
        # if M[v_s].max() < P[v_p][v_s] * (1 - beta):
        #     M[v_s][v_s] = P[v_p][v_s] * (1 - beta) - M[v_s].max()
        # s = M[v_s].sum()
        # for l in M.index:
        #     M[v_s][l] /= s
        # print(M[v_s])
        # print('start v_s',i,v_s)
        # print(P.index.tolist())
        # print(P[v_s])
        v_s = random_pick(P.index.tolist(), P[v_s].values)
        R[v_s] += 1

        #print('end v_s',i,v_s)


    R_order = sorted(R.items(), key=lambda x: x[1], reverse=True)
    return R_order

def random_walk_second_order(G, P, v_fe, r, beta, num_loop):
    '''
    G nx.DiGraph() 根因定位图
    P one-order probability matrix
    v_fe 前端节点
    r   参数rou
    beta 参数beta
    num_loop 循环次数
    n  节点数
    m  边数
    v_s 当前节点
    v_p 前一个节点
    M pd.DataFrame n*n大小 转移矩阵
    R 字典 以节点名为键 出现次数为值
    return
    返回一个排序后的列表
    '''
    # 初始化
    v_s = v_fe
    v_p = v_fe
    n = len(G.nodes())
    R = dict.fromkeys(G.nodes(), 0)
    M = dict.fromkeys(G.nodes(), 0)

    # 循环随机游走
    # print(P)
    for i in range(num_loop):

        # v_p = v_s
        # print(v_s,v_p)
        p_k_s = P[v_p][v_s]
        sum = 0
        for key,value in P[v_s].iteritems():
            if value > 0:
                M[key] = (1-beta) * p_k_s + beta * P[v_s][key]
                sum = sum + (1-beta) * p_k_s + beta * P[v_s][key]
        if sum==0:
            for key,value in M.items():
                M[key] = 1 /len(M.items())
        else:   
            for key,value in M.items():
                M[key] = value / sum



        v_n = random_pick(M.keys(), M.values())
        R[v_n] += 1

        v_p = v_s
        v_s = v_n
    R_order = sorted(R.items(), key=lambda x: x[1], reverse=True)
    return R_order


