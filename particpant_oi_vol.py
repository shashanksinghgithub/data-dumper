import pandas as pd
from bs4 import BeautifulSoup
import requests,re,os,time,logging,datetime,wget,urllib2,redis,sys,shutil
from lxml import html
from cassandra.cluster import Cluster
from dateutil.parser import parse
from selenium import webdriver
import numpy as np
import redis

os.chdir("D:\\Data_dumpers\\FII_PI\\participantwise\\")
cassandra_host = "172.17.9.51"
redis_host = "localhost"

processed_dir = "D:\\Data_dumpers\\FII_PI\\participantwise\\Processed\\"
master_dir = "D:\\Data_dumpers\\Master\\"
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}


logging.basicConfig(filename='test.log',
                        level=logging.DEBUG,
                        format="%(asctime)s:%(levelname)s:%(message)s")


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

def cassandra_configs_cluster():
    f = open(master_dir+"config.txt",'r').readlines()
    f = [ str.strip(config.split("cassandra,")[-1].split("=")[-1]) for config in f if config.startswith("cassandra")]  
          
    from cassandra.auth import PlainTextAuthProvider

    auth_provider= PlainTextAuthProvider(username=f[1],password=f[2])
    cluster = Cluster([f[0]], auth_provider=auth_provider)
    
    return cluster


#cluster = Cluster([cassandra_host])
cluster = cassandra_configs_cluster()
logging.info('Cassandra Cluster connected...')
# connect to your keyspace and create a session using which u can execute cql commands 
session = cluster.connect('rohit')
logging.info('Using rohit keyspace')
session.execute("CREATE TABLE IF NOT EXISTS participant_oi(Participant_Date DATE,Future_Index_Long_Client FLOAT,Future_Index_Short_Client FLOAT,Future_Stock_Long_Client FLOAT,Future_Stock_Short_Client FLOAT,Option_Index_Call_Long_Client FLOAT,Option_Index_Put_Long_Client FLOAT,Option_Index_Call_Short_Client FLOAT,Option_Index_Put_Short_Client FLOAT,Option_Stock_Call_Long_Client FLOAT,Option_Stock_Put_Long_Client FLOAT,Option_Stock_Call_Short_Client FLOAT,Option_Stock_Put_Short_Client FLOAT,Future_Index_Long_DII FLOAT,Future_Index_Short_DII FLOAT,Future_Stock_Long_DII FLOAT,Future_Stock_Short_DII FLOAT,Option_Index_Call_Long_DII FLOAT,Option_Index_Put_Long_DII FLOAT,Option_Index_Call_Short_DII FLOAT,Option_Index_Put_Short_DII FLOAT,Option_Stock_Call_Long_DII FLOAT,Option_Stock_Put_Long_DII FLOAT,Option_Stock_Call_Short_DII FLOAT,Option_Stock_Put_Short_DII FLOAT,Future_Index_Long_FII FLOAT,Future_Index_Short_FII FLOAT,Future_Stock_Long_FII FLOAT,Future_Stock_Short_FII FLOAT,Option_Index_Call_Long_FII FLOAT,Option_Index_Put_Long_FII FLOAT,Option_Index_Call_Short_FII FLOAT,Option_Index_Put_Short_FII FLOAT,Option_Stock_Call_Long_FII FLOAT,Option_Stock_Put_Long_FII FLOAT,Option_Stock_Call_Short_FII FLOAT,Option_Stock_Put_Short_FII FLOAT,Future_Index_Long_Pro FLOAT,Future_Index_Short_Pro FLOAT,Future_Stock_Long_Pro FLOAT,Future_Stock_Short_Pro FLOAT,Option_Index_Call_Long_Pro FLOAT,Option_Index_Put_Long_Pro FLOAT,Option_Index_Call_Short_Pro FLOAT,Option_Index_Put_Short_Pro FLOAT,Option_Stock_Call_Long_Pro FLOAT,Option_Stock_Put_Long_Pro FLOAT,Option_Stock_Call_Short_Pro FLOAT,Option_Stock_Put_Short_Pro FLOAT, PRIMARY KEY (Participant_Date))")
session.execute("CREATE TABLE IF NOT EXISTS participant_vol(Participant_Date DATE,Future_Index_Long_Client FLOAT,Future_Index_Short_Client FLOAT,Future_Stock_Long_Client FLOAT,Future_Stock_Short_Client FLOAT,Option_Index_Call_Long_Client FLOAT,Option_Index_Put_Long_Client FLOAT,Option_Index_Call_Short_Client FLOAT,Option_Index_Put_Short_Client FLOAT,Option_Stock_Call_Long_Client FLOAT,Option_Stock_Put_Long_Client FLOAT,Option_Stock_Call_Short_Client FLOAT,Option_Stock_Put_Short_Client FLOAT,Future_Index_Long_DII FLOAT,Future_Index_Short_DII FLOAT,Future_Stock_Long_DII FLOAT,Future_Stock_Short_DII FLOAT,Option_Index_Call_Long_DII FLOAT,Option_Index_Put_Long_DII FLOAT,Option_Index_Call_Short_DII FLOAT,Option_Index_Put_Short_DII FLOAT,Option_Stock_Call_Long_DII FLOAT,Option_Stock_Put_Long_DII FLOAT,Option_Stock_Call_Short_DII FLOAT,Option_Stock_Put_Short_DII FLOAT,Future_Index_Long_FII FLOAT,Future_Index_Short_FII FLOAT,Future_Stock_Long_FII FLOAT,Future_Stock_Short_FII FLOAT,Option_Index_Call_Long_FII FLOAT,Option_Index_Put_Long_FII FLOAT,Option_Index_Call_Short_FII FLOAT,Option_Index_Put_Short_FII FLOAT,Option_Stock_Call_Long_FII FLOAT,Option_Stock_Put_Long_FII FLOAT,Option_Stock_Call_Short_FII FLOAT,Option_Stock_Put_Short_FII FLOAT,Future_Index_Long_Pro FLOAT,Future_Index_Short_Pro FLOAT,Future_Stock_Long_Pro FLOAT,Future_Stock_Short_Pro FLOAT,Option_Index_Call_Long_Pro FLOAT,Option_Index_Put_Long_Pro FLOAT,Option_Index_Call_Short_Pro FLOAT,Option_Index_Put_Short_Pro FLOAT,Option_Stock_Call_Long_Pro FLOAT,Option_Stock_Put_Long_Pro FLOAT,Option_Stock_Call_Short_Pro FLOAT,Option_Stock_Put_Short_Pro FLOAT, PRIMARY KEY (Participant_Date))")

def dateparse(date):
    '''Func to parse dates'''    
    date = pd.to_datetime(date, dayfirst=True)    
    return date

# read holiday master
holiday_master = pd.read_csv(master_dir+'Holidays_2019.txt', delimiter=',',date_parser=dateparse, parse_dates={'date':[0]})    
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
    d = d - datetime.timedelta(days=1)
    while  True:
            if d in holiday_master["date"].values:
                d = d - datetime.timedelta(days=1)   
            else:
                return d

def fetch_participant_oi_vol(df,e):
    
    d=datetime.datetime.now().date()
    df = df.replace('\t','', regex=True)
    df.dropna(axis = 1, how ='all', inplace = True) 

    df.columns = df.iloc[0]
    df.drop(["Total Long Contracts","Total Short Contracts"],axis=1,inplace=True)
    oi=df[1:5]

    oi.columns=oi.columns.str.replace(" ","_")
    for i in range(0,len(oi.columns)):
        oi['{}'.format(oi.columns[i])] =  [re.sub(r'[^A-Za-z0-9]+','', str(x)) for x in oi['{}'.format(oi.columns[i])]]
    
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
    Client_Type_Client.insert(0,"Participant_Date",e)

    Client_Type_DII.columns=client2
    Client_Type_DII.drop(["Client_Type_DII"],axis=1,inplace=True)
    Client_Type_DII.insert(0,"Participant_Date",e)

    Client_Type_FII.columns=client3
    Client_Type_FII.drop(["Client_Type_FII"],axis=1,inplace=True)
    Client_Type_FII.insert(0,"Participant_Date",e)

    Client_Type_Pro.columns=client4
    Client_Type_Pro.drop(["Client_Type_Pro"],axis=1,inplace=True)
    Client_Type_Pro.insert(0,"Participant_Date",e)     
    
    final=pd.merge(Client_Type_Client,Client_Type_DII,how='left',on='Participant_Date')
    final=final.merge(Client_Type_FII,how='left',on='Participant_Date')
    final=final.merge(Client_Type_Pro,how='left',on='Participant_Date')
    final['Participant_Date']=pd.to_datetime(final['Participant_Date'])
    final["Participant_Date"]=final["Participant_Date"].dt.date
 
    return final


def main(d):
    
    
    if process_run_check(d)== -1:
        return -1
    print 'https://archives.nseindia.com/content/nsccl/fao_participant_oi_{}.csv'.format(d.strftime('%d%m%Y'))
    
    
    while datetime.datetime.now().time()>=datetime.time(17,30) :
        print "sleep for 2 min"
        time.sleep(120)
        try:
            filedata = requests.get('https://archives.nseindia.com/content/nsccl/fao_participant_oi_{}.csv'.format(
                d.strftime('%d%m%Y')), headers=headers ) 
            if filedata.status_code!=200:
                continue
            elif filedata.status_code==200:
                break
        except Exception as e:
            print e
            continue
        
    
    
    #print 'https://www.nseindia.com/content/nsccl/fao_participant_oi_{}.csv'.format(d)  
    print "Processing {}".format(d)
    output=open("fao_participant_oi_{}.csv".format(d),"w")
    output.write(filedata.content)
    output.close()
    print "status",filedata.status_code
    print 'https://archives.nseindia.com/content/nsccl/fao_participant_vol_{}.csv'.format(d.strftime('%d%m%Y'))
    filedata = requests.get('https://archives.nseindia.com/content/nsccl/fao_participant_vol_{}.csv'.format(
            d.strftime('%d%m%Y')) , headers=headers ) 
    
    #print 'https://www.nseindia.com/content/nsccl/fao_participant_vol_{}.csv'.format(d)  
    print "Processing {}".format(d)
    output=open("fao_participant_vol_{}.csv".format(d),"w")
    output.write(filedata.content)
    output.close()
    print "status",filedata.status_code

    df=pd.read_csv("fao_participant_oi_{}.csv".format(d))
    oi=fetch_participant_oi_vol(df,d)
    oi.to_csv("oi.csv",index=False)
    os.system("oi.bat")
    os.remove("oi.csv")
    
    df1=pd.read_csv("fao_participant_vol_{}.csv".format(d))
    vol=fetch_participant_oi_vol(df1,d)
    vol.to_csv("vol.csv",index=False)
    os.system("vol.bat")
    os.remove("vol.csv")
    
    shutil.move("fao_participant_oi_{}.csv".format(d),processed_dir)
    shutil.move("fao_participant_vol_{}.csv".format(d),processed_dir)
    
    r = redis.Redis(host=redis_host, port=6379) 
    r.set('participant_wise_flag',1)
    r.set("participant_wise_remarks",
          'Participant wise OI and Volume dumped in tables participant_oi and participant_vol respectively for {} '.format(d))
    r.set("participant_wise_report",1)
    r.set("market_snap_participant_oi",1)    
    r.set("market_positioning",1)   
    r.set("market_positioning_chg",1)     
          
       
            
d = datetime.datetime.now().date()- datetime.timedelta(days=0)   # set date here

for i in range(365):        
    if os.path.exists(processed_dir+'fao_participant_oi_{}.csv'.format(d)) or os.path.exists(processed_dir+'fao_participant_vol_{}.csv'.format(d)):
        print "file exists"
        break
    else:
        print "file not present"
        main(d)
        
    d = previous_working_day(d)
