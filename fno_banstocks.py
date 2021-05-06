# -*- coding: utf-8 -*-
"""
Created on Mon Dec 30 11:14:41 2020

@author: krishna
"""
import datetime
import requests
import StringIO, zipfile
import os
import sys
import shutil
import pandas as pd
import time 
import logging 
import redis
import re
from cassandra.cluster import Cluster

os.chdir("D:\\Data_dumpers\\NSE_FNO_ban_stocks\\")

redis_host = 'localhost'
cassandra_host = '172.17.9.51'


download_dir = "D:\\Data_dumpers\\NSE_FNO_ban_stocks\\Download\\"
processed_dir = "D:\\Data_dumpers\\NSE_FNO_ban_stocks\\Processed_folder\\"
master_dir = "D:\\Data_dumpers\\Master\\"
log_path = "D:\\Data_dumpers\\NSE_FNO_ban_stocks\\"

redis_host = 'localhost'
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}


# log events in debug mode 
logging.basicConfig(filename=log_path+"test.log",
                        level=logging.DEBUG,
                        format="%(asctime)s:%(levelname)s:%(message)s")

def dateparse(date):
    '''Func to parse dates'''    
    date = pd.to_datetime(date, dayfirst=True)    
    return date
  
# read holiday master
holiday_master = pd.read_csv(master_dir+ 'Holidays_2019.txt', delimiter=',',
                             date_parser=dateparse, parse_dates={'date':[0]})    
holiday_master['date'] = holiday_master.apply(lambda row: row['date'].date(), axis=1)
       
    
def convertDate(date):
    y = date.strftime("%Y")
    m = date.strftime("%m")
    d = date.strftime("%d")
    return [y, m, d]


def getFilename(date):
    [y, m, d] = convertDate(date)
    return "fo_secban_%s%s%s.csv" % (d, m, y)

    
def getReqStr(date):
    [y, m, d] = convertDate(date)
    return "/archives/fo/sec_ban/%s" % (getFilename(date))

        
def cassandra_configs_cluster():
    f = open(master_dir+"config.txt",'r').readlines()
    f = [ str.strip(config.split("cassandra,")[-1].split("=")[-1]) for config in f if config.startswith("cassandra")]  
          
    from cassandra.auth import PlainTextAuthProvider

    auth_provider= PlainTextAuthProvider(username=f[1],password=f[2])
    cluster = Cluster([f[0]], auth_provider=auth_provider)
    
    return cluster

def T1_working_day(d):
    '''Get previous wokring day'''
    
    d = d + datetime.timedelta(days=1)
    while  True:
            if d in holiday_master["date"].values:
                #print "Holiday : ",d
                d = d + datetime.timedelta(days=1)                
            else:
                return d    


def cassandra_dumper():
    
    
    # create python cluster object to connect to your cassandra cluster (specify ip address of nodes to connect within your cluster)
    #cluster = Cluster([cassandra_host])
    cluster = cassandra_configs_cluster()

    logging.info('Cassandra Cluster connected...')
    # connect to your keyspace and create a session using which u can execute cql commands 
    session = cluster.connect('rohit')
    logging.info('Using rohit keyspace')
    
    # CREATE A TABLE; dump bhavcopies to this table
    session.execute('CREATE TABLE IF NOT EXISTS FNO_banstocks (symbol VARCHAR,date DATE, PRIMARY KEY (symbol, date))')
    
    # walk through all the downlaoded bhavcopies in downlaods dir
    for r,d,f in os.walk(download_dir):
        
        # process every bhavcopy in expiry encodings format
        logging.info('Traverse through each bhavcopy')
        for csv_file in f:
            #read each csv file 
            print csv_file
            file_name = csv_file
            logging.info('Processing fno ban stocks {0}...'.format(file_name))

            fno_ban = pd.read_csv(download_dir + file_name)
            og_len = len(fno_ban)
            
            shutil.move(download_dir + file_name, log_path+"temp.csv")
            
            # write csv file to cassandra db
            os.system(log_path+"dump.bat ")               
            # move to processed folder
            shutil.move(log_path+"temp.csv", processed_dir + file_name)            
            r = redis.Redis(host=redis_host, port=6379) 
            r.set('fno_banstocks', 1)            
            #get number of rows dumped into cassandra and original rows in file
            d = fno_ban['date'].values[0]           
            c_len = session.execute('select count(*) from rohit.fno_banstocks where date = \'{}\' allow filtering;'.format(d), timeout=None).one()[0]
            
            print 'Number of rows in orginal file {},number of rows dumped in cassandra {}'.format(og_len,c_len)
            logging.info('{}: Number of rows in orginal file {}, number of rows dumped in cassandra {}'.format(d,og_len,c_len))
                       
            
            r.set('fno_banstocks_len',
                  '{}: {} rows dumped in cassandra , whereas {} rows were present in original file'.format(d, c_len, og_len))


def downloadCSV(d):
    
    filename = getFilename(d)
    reqstr = getReqStr(d)
    reqstr = "https://www1.nseindia.com"+reqstr
    print reqstr
    responsecode = ''
    print "Downloading %s ..." % (filename)
        
    if len(holiday_master[holiday_master['date']==d])==0:
        # working day so run until bhavcopy is downloaded
        try:
            responsecode = requests.get(reqstr, headers=headers )   
            print responsecode.status_code
        except Exception as e:
            print e
            responsecode=404
            
        while datetime.datetime.now().time() > datetime.time(19,0):
            # sleep for 2 min
            logging.info('{} is Working day : sleep for 2 minutes '.format(d))
            
            print 'Sleep for 2 min...'
            
            time.sleep(120)
            try:
                responsecode = requests.get(reqstr, headers=headers, timeout=45 )  
                if responsecode.status_code==200:
                    break
            except Exception as e:
                print "Response Exception / {}".format(e)
        
        
           
    elif len(holiday_master[holiday_master['date']==d])==1:
        logging.info('Holiday: skip for current date :{} '.format(d))
        return -1
    
    
    try:
        # write a file 
        with open("temp.csv","wb") as f:
            f.write(responsecode.content)
        df = pd.read_csv("temp.csv")
        process_date = pd.to_datetime(re.search("([0-9]{2}\-[A-Z]{3}\-[0-9]{4})",df.columns.values[0]).group()).date()
        df['date'] = process_date
        df.columns= ['Symbol','date']
        if df.empty==False:
            df.to_csv(download_dir+"fo_secban_{}.csv".format(process_date.strftime("%d%m%Y")), index=False)
        
        
        return 1
    
    except:
        print "File processing error "
        return -1
        

def getUpdate():
    errContinous = 0
    d = T1_working_day(datetime.date.today())
    decr = datetime.timedelta(days=1)
    while errContinous > -30 and (not os.path.exists(os.path.join("Processed_folder",getFilename(d)))):
        if downloadCSV(d) > -1:
            errContinous = 0
        else:
            errContinous -= 1
        d -= decr

def main(args):
    if args:
        if args[0] == "-update":
            getUpdate()
            # dump to cassandra
            cassandra_dumper()
        

if __name__ == "__main__":
    main(sys.argv[1:])