import requests
import urllib
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import csv
import os
import re
from retry import retry
from langdetect import detect
from langdetect import DetectorFactory


@retry(Exception, tries=4, delay=3, backoff=2)
def get_html(filename):
    with urllib.request.urlopen(filename) as url:
        #print(url.read())
        b_soup = BeautifulSoup(url,'lxml')
    while b_soup==None:
        with urllib.request.urlopen(filename) as url:
            b_soup = BeautifulSoup(url,'lxml')
    return b_soup

#Preparing English Data
# if taking too long then remove statement which create "Header_2" column
def prep_en_data(parse_url_en,ministry_list):
    df1_en = pd.DataFrame(columns = ['Title','Ministry','Posting_Date','Link'])
    parse_main_en = parse_url_en.find('div',{'class':'content-area'})
    #print((parse_main.attrs))
    no_index=0
    for i in range(len(parse_main_en.contents)-2):
        ministry_name = (' '.join(str((parse_main_en.contents[i+1].li.h3.string)).strip().split())).strip('\"')
        if ministry_name not in ministry_list['English_Ministry_Name'].unique():print((ministry_name))
        for k in parse_main_en.contents[i+1].li.ul.findAll('li'):
            n_title= ' '.join(str(k.a.string).strip().replace('\n',' ').replace('\r',' ').split())
            df1_en.loc[no_index]=[n_title,ministry_name,str(k.span.string).split(':').pop().strip(),\
                                 str(k.a['href']).strip()]
            no_index=no_index+1
    df1_en = pd.merge(df1_en,ministry_list,left_on=['Ministry'],right_on=['English_Ministry_Name'],how='inner')
    df1_en.drop(['Ministry'],inplace=True,axis=1)
    df1_en['Posting_Date'] = pd.to_datetime(df1_en['Posting_Date'])
    df1_en['B_Soup']=df1_en['Link'].apply(lambda x:get_html(x))
    df1_en["Header_2"]=df1_en['B_Soup'].apply(lambda x:(str(x.findAll('h2')[0]\
                                                        .get_text()).strip().replace('\n',' ').replace('\r',' ')))
    df1_en["Header_2"].replace(r'^\s*$',np.nan,regex=True,inplace=True)
    df1_en["Header_2"].fillna(df1_en["Title"][df1_en["Header_2"].isnull()],inplace=True)
    df1_en["Posting_Datetime"]=df1_en['B_Soup'].apply(lambda x:\
                                                     [re.search('[0-9]{2}?.+(AM|PM)',str(div.get_text())).group(0) for div in \
                                                      x.findAll('div',attrs = {'class' : True}) \
                                                      if 'ReleaseDateSubHead'in div['class'][0]][0])
    df1_en['Posting_Datetime'] = pd.to_datetime(df1_en['Posting_Datetime'])
    df1_en.drop(['B_Soup'],axis=1,inplace=True)
    df1_en.reset_index(drop=True,inplace=True)
    df1_en.reset_index(inplace=True)
    return df1_en

# Preparing Hindi Data
# if taking too long then remove statement which create "Header_2" column
def prep_hi_data(parse_url_hi,ministry_list):
    df1_hi = pd.DataFrame(columns = ['Title','Ministry','Posting_Date','Link'])
    parse_main_hi = parse_url_hi.find('div',{'class':'content-area'})
    #print((parse_main.attrs))
    no_index=0
    for i in range(len(parse_main_hi.contents)-2):
        ministry_name = (' '.join(str((parse_main_hi.contents[i+1].li.h3.string)).strip().split())).strip('\"')
        if ministry_name not in ministry_list['Hindi_Ministry_Name'].unique():print((ministry_name))
        #print((ministry_name))
        for k in parse_main_hi.contents[i+1].li.ul.findAll('li'):
            n_title= ' '.join(str(k.a.string).strip().replace('\n',' ').replace('\r',' ').split())
            df1_hi.loc[no_index]=[n_title,ministry_name,str(k.span.string).split(':').pop().strip(),\
                                 str(k.a['href']).strip()]
            no_index=no_index+1
    df1_hi = pd.merge(df1_hi,ministry_list,left_on=['Ministry'],right_on=['Hindi_Ministry_Name'],how='inner')
    df1_hi.drop(['Ministry'],inplace=True,axis=1)
    df1_hi['Posting_Date'] = pd.to_datetime(df1_hi['Posting_Date'])
    df1_hi['B_Soup']=df1_hi['Link'].apply(lambda x:get_html(x))
    df1_hi["Header_2"]=df1_hi['B_Soup'].apply(lambda x:(str(x.findAll('h2')[0]\
                                                        .get_text()).strip().replace('\n',' ').replace('\r',' ')))
    df1_hi["Header_2"].replace(r'^\s*$',np.nan,regex=True,inplace=True)
    df1_hi["Header_2"].fillna(df1_hi["Title"][df1_hi["Header_2"].isnull()],inplace=True)
    df1_hi["Posting_Datetime"]=df1_hi['B_Soup'].apply(lambda x:\
                                                     [re.search('[0-9]{2}?.+(AM|PM)',str(div.get_text())).group(0) for div in \
                                                      x.findAll('div',attrs = {'class' : True}) \
                                                      if 'ReleaseDateSubHead'in div['class'][0]][0])
    df1_hi['Posting_Datetime'] = pd.to_datetime(df1_hi['Posting_Datetime'])
    df1_hi.drop(['B_Soup'],axis=1,inplace=True)
    df1_hi.reset_index(drop=True,inplace=True)  
    df1_hi.reset_index(inplace=True)
    return df1_hi

# create dataframe which contain paralle url link for Hindi english
def prep_parallel_data(df1_en,df1_hi):
    df1_en['Posting_Date'] = pd.to_datetime(df1_en['Posting_Date'])
    df1_en['Posting_Datetime'] = pd.to_datetime(df1_en['Posting_Datetime'])
    df1_hi['Posting_Date'] = pd.to_datetime(df1_hi['Posting_Date'])
    df1_hi['Posting_Datetime'] = pd.to_datetime(df1_hi['Posting_Datetime'])
    pd_en_stat=pd.DataFrame(df1_en.groupby(['English_Ministry_Name','Posting_Datetime']).count()['Title'])
    print('English data : No of categoreies grouped by - ministry and date-time:',pd_en_stat.shape[0])
    print('English data :No of entries in categories grouped by - ministry and date-time:',pd_en_stat.Title.sum())
    pd_hi_stat=pd.DataFrame(df1_hi.groupby(['English_Ministry_Name','Posting_Datetime']).count()['Title'])
    print('Hindi data : No of categoreies grouped by - ministry and date-time:',pd_hi_stat.shape[0])
    print('Hindi data :No of entries in categories grouped by - ministry and date-time:',pd_hi_stat.Title.sum())
    pd_stat_merg=pd.merge(pd_en_stat,pd_hi_stat,left_index=True,right_index=True,how='inner',suffixes=('_en','_hi'))
    pd_stat_merg=pd_stat_merg.query("Title_en==Title_hi")
    #print(pd_stat_merg.info())
    pd_stat_merg=pd_stat_merg.query("Title_en==1")
    #print(pd_stat_merg.info())
    print('No of parallel search result:',pd_stat_merg.Title_en.sum())
    df1_en=pd.merge(df1_en,pd_stat_merg,left_on=['English_Ministry_Name','Posting_Datetime'],right_index=True,how='inner')
    df1_hi=pd.merge(df1_hi,pd_stat_merg,left_on=['English_Ministry_Name','Posting_Datetime'],right_index=True,how='inner')
    df1_en.drop(['Title_en','Title_hi'],axis=1,inplace=True)
    df1_hi.drop(['Title_en','Title_hi'],axis=1,inplace=True)
    df1_en.reset_index(drop=True,inplace=True)
    df1_hi.reset_index(drop=True,inplace=True)
    df1_en.drop(['index'],axis=1,inplace=True)
    df1_hi.drop(['index'],axis=1,inplace=True)
    data1_final=pd.merge(df1_en,df1_hi,left_index=True,right_index=True,how='inner',suffixes=['','_hi'])
    data1_final.drop(['English_Ministry_Name_hi','Hindi_Ministry_Name_hi','Posting_Date_hi','Posting_Datetime_hi'],\
                     axis=1,inplace=True)
    data1_final.rename(columns={'Header_2':'Header_2_en'},inplace=True)
    return data1_final

def get_en_data(n_month,n_year,filename_url_en,ministry_pa_list,import_data=False,import_data_dir=''):
    if import_data:
        df_en=pd.read_csv(import_data_dir+'//'+'English_data_'+n_month+'_'+n_year+'.csv',encoding='utf-16')
    else:
        df_en=prep_en_data(get_html(filename_url_en),ministry_pa_list)
    df_en['Posting_Date'] = pd.to_datetime(df_en['Posting_Date'])
    df_en['Posting_Datetime'] = pd.to_datetime(df_en['Posting_Datetime'])
    return df_en

def get_hi_data(n_month,n_year,filename_url_hi,ministry_pa_list,import_data=False,import_data_dir=''):
    if import_data:
        df_hi=pd.read_csv(import_data_dir+'//'+'Hindi_data_'+n_month+'_'+n_year+'.csv',encoding='utf-16')
    else:
        df_hi=prep_hi_data(get_html(filename_url_hi),ministry_pa_list)
    df_hi['Posting_Date'] = pd.to_datetime(df_hi['Posting_Date'])
    df_hi['Posting_Datetime'] = pd.to_datetime(df_hi['Posting_Datetime'])
    return df_hi

def get_parallel_data (df_en,df_hi,n_month,n_year,import_data=False,import_data_dir=''):
    if import_data:
        data_final = pd.read_csv(import_data_dir+'//'+'final_list_'+n_month+'_'+n_year+'.csv',encoding='utf-16')
    else:
        data_final= prep_parallel_data(df_en,df_hi)
    data_final['Posting_Date'] = pd.to_datetime(data_final['Posting_Date'])
    data_final['Posting_Datetime'] = pd.to_datetime(data_final['Posting_Datetime'])
    return data_final
def create_or_load_parallel_csv_and_stat_csv():
    pass