# -*- coding: utf-8 -*-
"""
Created on Fri Jun 14 18:15:01 2019

@author: devanshm
"""

import pandas as pd
from bs4 import BeautifulSoup
import requests,re,os,time,redis,logging,datetime,sys
from lxml import html
from cassandra.cluster import Cluster
from dateutil.parser import parse
from selenium import webdriver
import numpy as np
#import wget,urllib2
cassandra_host = "172.17.9.51"
redis_host = 'localhost'
server = '172.17.9.149'; port = 25

#cassandra_host = "localhost"
#redis_host = 'localhost'
#r = redis.Redis(host=redis_host, port=6379) 
#server = '172.17.9.149'; port = 25

logging.basicConfig(filename='test.log',
                        level=logging.DEBUG,
                        format="%(asctime)s:%(levelname)s:%(message)s")

master_dir = "D:\\Data_dumpers\\Master\\"


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def cassandra_configs_cluster():
    f = open(master_dir+"config.txt",'r').readlines()
    f = [ str.strip(config.split("cassandra,")[-1].split("=")[-1]) for config in f if config.startswith("cassandra")]  
          
    from cassandra.auth import PlainTextAuthProvider

    auth_provider= PlainTextAuthProvider(username=f[1],password=f[2])
    cluster = Cluster([f[0]], auth_provider=auth_provider)
    
    return cluster

cluster = cassandra_configs_cluster()

#cluster = Cluster([cassandra_host])
logging.info('Cassandra Cluster connected...')
# connect to your keyspace and create a session using which u can execute cql commands 
session = cluster.connect('rohit')
#session = cluster.connect('test_df')
logging.info('Using rohit keyspace')


def dateparse(date):
    '''Func to parse dates'''    
    date = pd.to_datetime(date, dayfirst=True)    
    return date

# read holiday master
# holiday_master = pd.read_csv('D:\\NSENotis\\Holidays_2019.txt', delimiter=',',
#                                  date_parser=dateparse, parse_dates={'date':[0]})    

# read holiday master
holiday_master = pd.read_csv('Holidays_2019.txt', delimiter=',',date_parser=dateparse, parse_dates={'date':[0]})    
holiday_master['date'] = holiday_master.apply(lambda row: row['date'].date(), axis=1) 
        
def process_run_check(d):
    '''Func to check if the process should run on current day or not'''
   
    # check if working day or not 
    if len(holiday_master[holiday_master['date']==d])==0:
        print "working day wait file is getting downloaded"
        return 1
    
    elif len(holiday_master[holiday_master['date']==d])==1:
        logging.info('Holiday: skip for current date :{} '.format(d))
        print ('Holiday: skip for current date :{} '.format(d))        
        sys.exit(1)

def previous_working_day(d):
    d = d - datetime.datetime.timedelta(days=1)
    while  True:
            if d in holiday_master["date"].values:
                d = d - datetime.timedelta(days=1)   
            else:
                return d


def fetch_participant_oi_vol(value,d):
    headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
               'Accept':'application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
               'Accept-Encoding':'gzip,deflate,sdch',
               'Referer':'https://www.bseindia.com'}
    filedata = requests.get(value,headers=headers)  
    output=open("{}.csv","wb".format(value))
    output.write(filedata.content)
    output.close()
    if filedata.status_code!=200:
        print ("wait for few minutes")
        print 'Sleep for 5 min'
        time.sleep(300)
        participantwise()
    else:        
        df=pd.read_csv('{}.csv'.format(value))
        #df=pd.read_csv("fao_participant_oi_14062019")
        df.columns = df.iloc[0]
        oi=df[1:5]
        #print (oi["Future Index Long"][1])
        oi.columns=oi.columns.str.replace(" ","_")
        oi.drop(["Total_Long_Contracts","Total_Short_Contracts"],axis=1,inplace=True)
        session.execute("CREATE TABLE IF NOT EXISTS participant_oi(Participant_Date DATE,Future_Index_Long_Client FLOAT,Future_Index_Short_Client FLOAT,Future_Stock_Long_Client FLOAT,Future_Stock_Short_Client FLOAT,Option_Index_Call_Long_Client FLOAT,Option_Index_Put_Long_Client FLOAT,Option_Index_Call_Short_Client FLOAT,Option_Index_Put_Short_Client FLOAT,Option_Stock_Call_Long_Client FLOAT,Option_Stock_Put_Long_Client FLOAT,Option_Stock_Call_Short_Client FLOAT,Option_Stock_Put_Short_Client FLOAT,Future_Index_Long_DII FLOAT,Future_Index_Short_DII FLOAT,Future_Stock_Long_DII FLOAT,Future_Stock_Short_DII FLOAT,Option_Index_Call_Long_DII FLOAT,Option_Index_Put_Long_DII FLOAT,Option_Index_Call_Short_DII FLOAT,Option_Index_Put_Short_DII FLOAT,Option_Stock_Call_Long_DII FLOAT,Option_Stock_Put_Long_DII FLOAT,Option_Stock_Call_Short_DII FLOAT,Option_Stock_Put_Short_DII FLOAT,Future_Index_Long_FII FLOAT,Future_Index_Short_FII FLOAT,Future_Stock_Long_FII FLOAT,Future_Stock_Short_FII FLOAT,Option_Index_Call_Long_FII FLOAT,Option_Index_Put_Long_FII FLOAT,Option_Index_Call_Short_FII FLOAT,Option_Index_Put_Short_FII FLOAT,Option_Stock_Call_Long_FII FLOAT,Option_Stock_Put_Long_FII FLOAT,Option_Stock_Call_Short_FII FLOAT,Option_Stock_Put_Short_FII FLOAT,Future_Index_Long_Pro FLOAT,Future_Index_Short_Pro FLOAT,Future_Stock_Long_Pro FLOAT,Future_Stock_Short_Pro FLOAT,Option_Index_Call_Long_Pro FLOAT,Option_Index_Put_Long_Pro FLOAT,Option_Index_Call_Short_Pro FLOAT,Option_Index_Put_Short_Pro FLOAT,Option_Stock_Call_Long_Pro FLOAT,Option_Stock_Put_Long_Pro FLOAT,Option_Stock_Call_Short_Pro FLOAT,Option_Stock_Put_Short_Pro FLOAT, PRIMARY KEY (Participant_Date))")
        session.execute("CREATE TABLE IF NOT EXISTS participant_vol(Participant_Date DATE,Future_Index_Long_Client FLOAT,Future_Index_Short_Client FLOAT,Future_Stock_Long_Client FLOAT,Future_Stock_Short_Client FLOAT,Option_Index_Call_Long_Client FLOAT,Option_Index_Put_Long_Client FLOAT,Option_Index_Call_Short_Client FLOAT,Option_Index_Put_Short_Client FLOAT,Option_Stock_Call_Long_Client FLOAT,Option_Stock_Put_Long_Client FLOAT,Option_Stock_Call_Short_Client FLOAT,Option_Stock_Put_Short_Client FLOAT,Future_Index_Long_DII FLOAT,Future_Index_Short_DII FLOAT,Future_Stock_Long_DII FLOAT,Future_Stock_Short_DII FLOAT,Option_Index_Call_Long_DII FLOAT,Option_Index_Put_Long_DII FLOAT,Option_Index_Call_Short_DII FLOAT,Option_Index_Put_Short_DII FLOAT,Option_Stock_Call_Long_DII FLOAT,Option_Stock_Put_Long_DII FLOAT,Option_Stock_Call_Short_DII FLOAT,Option_Stock_Put_Short_DII FLOAT,Future_Index_Long_FII FLOAT,Future_Index_Short_FII FLOAT,Future_Stock_Long_FII FLOAT,Future_Stock_Short_FII FLOAT,Option_Index_Call_Long_FII FLOAT,Option_Index_Put_Long_FII FLOAT,Option_Index_Call_Short_FII FLOAT,Option_Index_Put_Short_FII FLOAT,Option_Stock_Call_Long_FII FLOAT,Option_Stock_Put_Long_FII FLOAT,Option_Stock_Call_Short_FII FLOAT,Option_Stock_Put_Short_FII FLOAT,Future_Index_Long_Pro FLOAT,Future_Index_Short_Pro FLOAT,Future_Stock_Long_Pro FLOAT,Future_Stock_Short_Pro FLOAT,Option_Index_Call_Long_Pro FLOAT,Option_Index_Put_Long_Pro FLOAT,Option_Index_Call_Short_Pro FLOAT,Option_Index_Put_Short_Pro FLOAT,Option_Stock_Call_Long_Pro FLOAT,Option_Stock_Put_Long_Pro FLOAT,Option_Stock_Call_Short_Pro FLOAT,Option_Stock_Put_Short_Pro FLOAT, PRIMARY KEY (Participant_Date))")

        client1=[]
        for i in range(len(oi.columns)):
            exp1=oi.columns[i]+"_"+oi["Client_Type"][1]
            client1.append(exp1)
            #print (client1)
            
        client2=[]
        for i in range(len(oi.columns)):
            exp2=oi.columns[i]+"_"+oi["Client_Type"][2]
            client2.append(exp2)
            #print (client2)

        client3=[]
        for i in range(len(oi.columns)):
            exp3=oi.columns[i]+"_"+oi["Client_Type"][3]
            client3.append(exp3)
            #print (client3)

        client4=[]
        for i in range(len(oi.columns)):
            exp4=oi.columns[i]+"_"+oi["Client_Type"][4]
            client4.append(exp4)
            #print (client4)
    
        Client_Type_Client=oi[:1]
        Client_Type_DII=oi[1:2]
        Client_Type_FII=oi[2:3]
        Client_Type_Pro=oi[3:4]
    
        Client_Type_Client.columns=client1
        Client_Type_Client.drop(["Client_Type_Client"],axis=1,inplace=True)
        Client_Type_Client.insert(0,"Participant_Date",d)

        Client_Type_DII.columns=client2
        Client_Type_DII.drop(["Client_Type_DII"],axis=1,inplace=True)
        Client_Type_DII.insert(0,"Participant_Date",d)
        
        Client_Type_FII.columns=client3
        Client_Type_FII.drop(["Client_Type_FII"],axis=1,inplace=True)
        Client_Type_FII.insert(0,"Participant_Date",d)

        Client_Type_Pro.columns=client4
        Client_Type_Pro.drop(["Client_Type_Pro"],axis=1,inplace=True)
        Client_Type_Pro.insert(0,"Participant_Date",d)     
    
        final=pd.merge(Client_Type_Client,Client_Type_DII,how='left',on='Participant_Date')
        final=final.merge(Client_Type_FII,how='left',on='Participant_Date')
        final=final.merge(Client_Type_Pro,how='left',on='Participant_Date')
        final['Participant_Date']=pd.to_datetime(final['Participant_Date'])
        final["Participant_Date"]=final["Participant_Date"].dt.date
        
        return final
       
def participantwise():
    d=datetime.datetime.now().date()
    valtoday=d.strftime('%d%m%Y')
    print("getting file for today_{}".format(d))
    value=("https://www.nseindia.com/content/nsccl/fao_participant_oi_{}.csv".format(valtoday))
    oi=fetch_participant_oi_vol(value,d)
    oi.to_csv("oi.csv",index=False)
    os.system("oi.bat")
    os.remove("oi.csv")
    value_vol=("https://www.nseindia.com/content/nsccl/fao_participant_vol_{}.csv".format(valtoday))
    vol=fetch_participant_oi_vol(value_vol,d)
    vol.to_csv("vol.csv",index=False)
    os.system("vol.bat")
    os.remove("vol.csv")

    for i in range(0,31):
        previousday=previous_working_day(d)
        valprev=previousday.strftime('%d%m%Y')
        print("file not found_{}".format(previousday))
        value=("https://www.nseindia.com/content/nsccl/fao_participant_oi_{}.csv".format(valprev))
        oi=fetch_participant_oi_vol(value,previousday)
        oi.to_csv("oi.csv",index=False)
        os.system("oi.bat")
        os.remove("oi.csv")
        print("file not found_{}".format(previousday))
        value_vol=("https://www.nseindia.com/content/nsccl/fao_participant_vol_{}.csv".format(valprev))
        vol=fetch_participant_oi_vol(value_vol,previousday)
        vol.to_csv("vol.csv",index=False)
        os.system("vol.bat")
        os.remove("vol.csv")
        previousday= previousday - datetime.timedelta(days=1)
    #logging.info('Parameters read from redis for FDerivativeActivity_flag')
    #return r.set('participant_oi_flag',1),r.set('participant_vol_flag',1)

participantwise()
    