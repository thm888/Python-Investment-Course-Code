'''
1.导入所需要的库，指定文件读取路径
'''
import numpy as np
import pandas as pd
import os 
import re
path ='/Users/hmt/Documents/ PHBS/courses/investment/project/data/wind底层库数据'

'''
2.财务、股价行情，估值数据读取与预处理
'''
#财务数据数据读取
financial_data = pd.read_csv(f'{path}/financial_Data.csv',dtype={'ANN_DT':'str'})
financial_data =financial_data.drop(['OBJECT_ID','S_INFO_WINDCODE','S_INFO_COMPCODE','CRNCY_CODE'],axis=1)

#财务数据预处理
financial_data_2 = financial_data.rename(columns={'WIND_CODE':'WINDCODE','ANN_DT':'公告日期',
                                                  'REPORT_PERIOD':'报告日期','S_FA_ROE_DEDUCTED':'净资产收益率(扣除非经常损益)',
                                                  'S_FA_YOYASSETS':'总资产增长率', 'S_FA_ROE_YEARLY':'roeyoy', 
                                                  'S_FA_ROE':'roe', 'S_FA_ROE_AVG':'roeavg',})#批量重命名列名，使其更直观

financial_data_2 = financial_data_2.sort_values(by=['WINDCODE','报告日期'])
financial_data_2['公告日期'] = pd.to_datetime(financial_data_2['公告日期'], format='%Y%m%d')
financial_data_2['报告日期'] = pd.to_datetime(financial_data_2['报告日期'], format='%Y%m%d')
financial_data_2 = financial_data_2.rename(columns={'公告日期':'date'})
financial_data_2['公告日期'] = financial_data_2['date']
#可以发现A股2002年之前的财务数据有缺失情况

#股价行情数据读取
price_data = pd.read_csv(f'{path}/price_data.csv',encoding='gbk',dtype={'UP_DOWN_LIMIT_STATUS':str,})#日估值信息
windAshare = pd.read_csv(f'{path}/windAshare.csv',encoding='gbk')#WIND全A指数
all_ashare = pd.read_csv(f'{path}/all_stock_wind.csv',encoding='gbk')#日行情量价信息

#股价行情数据预处理
price_data = price_data.drop(['OBJECT_ID','CRNCY_CODE'],axis=1)
all_ashare = all_ashare.drop(['OBJECT_ID','S_DQ_CHANGE','S_DQ_PCTCHANGE','S_DQ_LIMIT', 'S_DQ_STOPPING'],axis=1)
windAshare = windAshare.drop(['OBJECT_ID','CRNCY_CODE','OPDATE'],axis=1)
price_data['TRADE_DT'] = pd.to_datetime(price_data['TRADE_DT'],format='%Y%m%d')
all_ashare['TRADE_DT'] = pd.to_datetime(all_ashare['TRADE_DT'],format='%Y%m%d')
price_data.rename(columns={'TRADE_DT':'date'},inplace=True)
all_ashare.rename(columns={'TRADE_DT':'date'},inplace=True)
price_data[['stkcd', 'exchange_site']] = price_data['S_INFO_WINDCODE'].str.split('.', expand=True)
all_ashare[['stkcd', 'exchange_site']] = all_ashare['S_INFO_WINDCODE'].str.split('.', expand=True)
price_data.rename(columns={'S_INFO_WINDCODE':'WINDCODE'},inplace=True)
all_ashare.rename(columns={'S_INFO_WINDCODE':'WINDCODE'},inplace=True)

'''
3.合并数据成一张表
'''
#合并每日估值数据与财务数据，注意是与报告的公告日连接
all_ashare_dataset = pd.merge(price_data,financial_data_2,on=['WINDCODE','date'],how='outer')
all_ashare_dataset = all_ashare_dataset.sort_values(by=['WINDCODE','date'])

#把每日行情的财务数据都向前填充最近一个公告日报告的财务数据
def ffill_series(group):
    return group.fillna(method='ffill')
columns_to_fill = ['报告日期', '净资产收益率(扣除非经常损益)', '总资产增长率', 'roeyoy','roe', 'roeavg','公告日期']
grouped = all_ashare_dataset.groupby('WINDCODE')
filled_data = grouped[columns_to_fill].apply(ffill_series).reset_index(level=0, drop=True)
for column in columns_to_fill:
    all_ashare_dataset[column] = filled_data[column]

'''清洗非交易日数据'''
#导入A股交易日数据
tr_date = pd.read_csv(f'{path}/trading_date.csv',encoding='gbk')
tr_date['TRADE_DAYS'] = pd.to_datetime(tr_date['TRADE_DAYS'],format='%Y%m%d')
tr_date['S_INFO_EXCHMARKET']  = tr_date['S_INFO_EXCHMARKET'].replace({'BSE':'BJ','SZSE':'SZ','SSE':'SH'})
tr_date.rename(columns={'TRADE_DAYS':'date','S_INFO_EXCHMARKET':'exchange_site'},inplace=True)

#清洗合并数据与日行情数据的非交易日数据
all_ashare_dataset_2 = pd.merge(all_ashare_dataset,tr_date,on=['date','exchange_site'],how='inner')
all_ashare = pd.merge(all_ashare,tr_date,on=['date','exchange_site'],how='inner')
all_ashare_dataset_2 = all_ashare_dataset_2.drop(['exchange_site','stkcd'],axis=1)
all_ashare = all_ashare.drop(['exchange_site','stkcd'],axis=1)

#合并总数据集，将合并数据与日行情量价数据合并
merge_stock1 = pd.merge(all_ashare,all_ashare_dataset_2,on=['WINDCODE','date'],how='outer')#合并
merge_stock1.sort_values(by=['WINDCODE','date'],inplace=True)
merge_stock1.rename(columns={'S_DQ_PRECLOSE':'preclose', 'S_DQ_OPEN':'open',
       'S_DQ_HIGH':'high', 'S_DQ_LOW':'low', 'S_DQ_CLOSE':'close',
       'S_DQ_VOLUME':'volume', 'S_DQ_AMOUNT':'amount', 'S_DQ_ADJPRECLOSE':'复权preclose', 'S_DQ_ADJOPEN':'复权open',
       'S_DQ_ADJHIGH':'复权high', 'S_DQ_ADJLOW':'复权low', 'S_DQ_ADJCLOSE':'复权close', 'S_DQ_ADJFACTOR':'复权因子',
       'S_DQ_AVGPRICE':'avgprice', 'S_DQ_TRADESTATUS':'交易状态', 'S_DQ_TRADESTATUSCODE':'交易状态码','S_VAL_MV':'总市值',
        'S_DQ_MV':'流通市值', 'S_VAL_PB_NEW':'PB',
       'S_DQ_TURN':'换手率', 'S_DQ_FREETURNOVER':'流通换手率', 'S_DQ_CLOSE_TODAY':'今日收盘价',
       'UP_DOWN_LIMIT_STATUS':'涨跌停状态'},inplace=True)#重命名列名

''''
4.清洗 ST 股票，给股票打上行业信息标签
'''
#导入ST股票数据
st_stock = pd.read_csv(f'{path}/ST_data.csv',encoding='gbk',dtype={'ENTRY_DT':str,'REMOVE_DT':str})
st_stock.rename(columns={'S_INFO_WINDCODE':'WINDCODE','S_TYPE_ST':'ST状态'},inplace=True)
st_stock = st_stock.drop(['OBJECT_ID','REASON'],axis=1)
st_stock['REMOVE_DT'] = st_stock['REMOVE_DT'].fillna('20240521')
st_stock['ENTRY_DT'] = pd.to_datetime(st_stock['ENTRY_DT'],format='%Y%m%d')
st_stock['REMOVE_DT'] = pd.to_datetime(st_stock['REMOVE_DT'],format='%Y%m%d')

#导入行业分类信息数据
industry = pd.read_csv(f'{path}/行业分类.csv',encoding='gbk',dtype={'REMOVE_DT':str,'ENTRY_DT':str,'SW_IND_CODE':str})
industry.rename(columns={'S_INFO_WINDCODE':'WINDCODE', 'SW_IND_CODE':'申万行业代码','CUR_SIGN':'最新标志'},inplace=True)
industry['REMOVE_DT'] = industry['REMOVE_DT'].fillna('20240521')
industry['ENTRY_DT'] = pd.to_datetime(industry['ENTRY_DT'],format='%Y%m%d')
industry['REMOVE_DT'] = pd.to_datetime(industry['REMOVE_DT'],format='%Y%m%d')

#更新标签函数
def update_info_label(df_daily, df_info,label_columns,ENTRY_DT='ENTRY_DT',REMOVE_DT='REMOVE_DT',date='date',WINDCODE='WINDCODE'):
    merge_data_test = pd.merge(df_daily,df_info,on=WINDCODE,how='left')
    condition = (merge_data_test[date]>=merge_data_test[ENTRY_DT])&(merge_data_test[date]<=merge_data_test[REMOVE_DT])
    merge_data_test = merge_data_test[condition]
    merge_data_test = merge_data_test[[WINDCODE,date]+label_columns]
    merge_data_test = merge_data_test.drop_duplicates([WINDCODE,date],keep='first')
    df_daily = pd.merge(df_daily,merge_data_test,on=[WINDCODE,date],how='left')
    df_daily.sort_values(by=[WINDCODE,date])
    return df_daily

#更新ST股票标签
merge_stock2 = update_info_label(df_daily=merge_stock1,df_info=st_stock,label_columns=['ST状态'])
#更新行业信息标签
merge_stock3 = update_info_label(df_daily=merge_stock2,df_info=industry,label_columns=['申万行业代码'])
#删除ST股票
merge_stock4 = merge_stock3[merge_stock3['ST状态'].isna()]
'''
5.给股票信息打上公司信息标签
'''
#导入A股公司信息数据，并合并
ashare_basic_info = pd.read_csv(f'{path}/A股基本信息.csv',encoding='gbk',dtype=str)

ashare_basic_info.rename(columns={'S_INFO_WINDCODE':'WINDCODE', 'S_INFO_CODE':'stkcd', 'S_INFO_NAME':'公司名称', 
                                        'S_INFO_EXCHMARKET':'交易所','S_INFO_LISTBOARD':'上市板块代码', 'S_INFO_LISTDATE':'上市时间',
                                        'S_INFO_DELISTDATE':'退市时间','S_INFO_LISTBOARDNAME':'上市板块', 
                                        'IS_SHSC':'是否沪股通'},inplace=True)
merge_stock5 = pd.merge(merge_stock4,ashare_basic_info,on='WINDCODE',how='left')

#剔除上市时间，申万行业代码空值的股票
merge_stock6 = merge_stock5[merge_stock5['上市时间'].notna()]
merge_stock6 = merge_stock6[merge_stock6['申万行业代码'].notna()]


'''
6.股价行情时间裁剪，导出数据
'''
#股价行情时间裁剪，选取 20000101 到 20240101
cutoff_date_up = pd.to_datetime('20240101')
cutoff_date_down = pd.to_datetime('20000101')
# 使用布尔索引来选择date小于或等于20240101的行
merge_stock6 = merge_stock6[merge_stock6['date'] <= cutoff_date_up]
merge_stock6 = merge_stock6[merge_stock6['date'] >= cutoff_date_down]

#导出数据
merge_stock6.to_csv(f'{path}/all_ashares.csv', index=False)
'''
读取示例：all_ashare = pd.read_csv(f'{path}/all_ashares.csv',dtype={'交易状态':str, '交易状态码':str,'涨跌停状态':str, 
                                                          'ST状态':str, '申万行业代码':str, 'stkcd':str, '公司名称':str,
                                                          '交易所':str, '上市板块代码':str, '上市时间':str, '退市时间':str,
                                                          '上市板块':str, '是否沪股通':str})
'''