#import libraries
import logging,os,datetime 
import pandas as pd
from cassandra.cluster import Cluster
import time, redis


os.chdir("D:\Data_dumpers\FNO_factor")
master_dir='D:\\Data_dumpers\\Master\\'
curr_dir = os.getcwd()
redis_host = 'localhost'
cassandra_host = "172.17.9.51"
process_dir = "D:\\Data_dumpers\\NSE_FNO_bhavcopy\\Processed_folder\\"

def dateparse_d(date):
    '''Func to parse dates'''    
    date = pd.to_datetime(date, dayfirst=True)    
    return date


def dateparse(row):
    '''Func to parse dates while reading ticker files'''
    d = row.split("+")[0]
    d = pd.to_datetime(d, format='%Y-%m-%d %H:%M:%S')
    return d
    
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


def convertDate(date):
    y = date.strftime("%Y")
    m = date.strftime("%b").upper()
    d = date.strftime("%d")
    return [y, m, d]

def getFilename(date):
    [y, m, d] = convertDate(date)
    print "fo%s%s%sbhav.csv" % (d, m, y)
    return "fo%s%s%sbhav.csv" % (d, m, y)


# read holiday master
holiday_master = pd.read_csv(master_dir+'Holidays_2019.txt', delimiter=',',date_parser=dateparse_d, parse_dates={'date':[0]})    
holiday_master['date'] = holiday_master.apply(lambda row: row['date'].date(), axis=1)

# create python cluster object to connect to your cassandra cluster (specify ip address of nodes to connect within your cluster)
#cluster = Cluster([cassandra_host])
session = cluster.connect('rohit')
logging.info('Using rohit keyspace')
session.row_factory = pandas_factory
session.default_fetch_size = None


       
def process_run_check(d):
    '''Func to check if the process should run on current day or not'''  
    
    r = redis.Redis(host=redis_host, port=6379) 
    flag_check = int(r.get('fno_factors_dump'))       
    
    
    if len(holiday_master[holiday_master['date']==d])==0:
        while datetime.datetime.now().time() >= datetime.time(17,30) and flag_check!=1:
            print 'working day, sleep for 2 min'
            time.sleep(120)
            flag_check = int(r.get('fno_factors_dump')) 
        else:
            return 1
    elif len(holiday_master[holiday_master['date']==d])==1:
        logging.info('Holiday: skip for current date :{} '.format(d))
        return -1
                
    

  
'''get data from bhavcopy and perform calculations'''
def get_cassandra_df(value):
    
    keys = pd.read_excel(master_dir+'MasterData.xlsx')
    keys = keys[keys['IsActiveFNO']==True]
    keys.loc[keys['Type']=='SSF','instrument'] = 'FUTSTK'
    keys.loc[keys['Type']=='Index','instrument'] = 'FUTIDX'
    keys = keys['SYMBOL']+"_"+keys['instrument']+"_0.0_XX_1"
    
    result = pd.DataFrame()
  
    for key in keys:
        query=session.execute("SELECT * FROM FNO_bhavcopy where price_date='{}' and key='{}' ALLOW FILTERING".format(value, key),
                      timeout = None)
        result = result.append(query._current_rows, ignore_index = True)
            
    #result=result.loc[~result["symbol"].isin(["NIFTYIT"])]
    result[["contracts","val_inlakh"]]=result[["contracts","val_inlakh"]].astype(float)
    
    result["valuebycontracts"]=(result["val_inlakh"]*100000)/result["contracts"]
       
    result["valuebycontracts"]=result["valuebycontracts"].round(2)
    return result


'''get dollar rate'''
def get_dollar_rate(value):
    
    
    logging.info("Reading data from cassandra")
    query=session.execute("SELECT * FROM bloom_usd_inr where date='{}' ALLOW FILTERING".format(value),
                          timeout = None)
    
    result = query._current_rows
    
    result["usd_inr"]= result["usd_inr"].astype(float)*1000000
    result["usd_inr"]= result["usd_inr"].round(2)
    result = result['usd_inr'].values[0]

    return result

'''calculate factor for futstk and futidx and store in cassandra'''
def calcultae_futstk_futidx(df,dollar,d):
    
    futstk=df.loc[df["instrument"].isin(["FUTSTK"])]
    length_futstk=len(futstk); sum_futstk=futstk["valuebycontracts"].sum()
    print "length_futstk , total_value_contracts",length_futstk,sum_futstk
    
    futidx=df.loc[df["instrument"].isin(["FUTIDX"])]
    futidx=futidx.loc[futidx['symbol'].isin(['NIFTY','BANKNIFTY'])]
    length_futidx=5
    sum_futidx= 4*futidx[futidx['symbol']=='NIFTY']["valuebycontracts"].values[0] + futidx[futidx['symbol']=='BANKNIFTY']["valuebycontracts"].values[0]
    print "length_futidx , total_value_contracts",length_futidx,sum_futidx
    
    futstk=round((dollar*length_futstk)/sum_futstk,2)
    futidx=round((dollar*length_futidx)/sum_futidx,2)
    
    final = pd.DataFrame([['FUTSTK',d,futstk],['FUTIDX',d,futidx]], columns=['instrument','price_date','factor'])
    
    final.to_csv("fno_factor.csv",index=False)    
    
    session.execute("CREATE TABLE IF NOT EXISTS fno_factors(price_date DATE,instrument TEXT,factor FLOAT,PRIMARY KEY(price_date,instrument))")
    logging.info("store data into cassandra")
    os.system("dump.bat")
  
    return 1

def main(nd):
    d=datetime.datetime.now().date()-datetime.timedelta(nd)
    print d
    if process_run_check(d) == -1:
        print "holiday; skipping for current day"
        return -1
    df=get_cassandra_df(d)
    dollar=get_dollar_rate(d)
    if calcultae_futstk_futidx(df,dollar,d)==1:
        r = redis.Redis(host=redis_host, port=6379) 
        res = session.execute("select price_date from fno_factors where price_date>='{}' allow filtering".format(
                datetime.datetime.now().date()-datetime.timedelta(days=15)))
        res = res._current_rows
        res.sort_values(by='price_date', inplace=True)
        res = res.iloc[-1]['price_date']
        r.set("fno_factors",1)
        r.set("fno_factors_remarks", "Fno_factors dumped in cassandra for {}".format(res))
        
        # market poisitoning fno factors flag
        r.set("market_pos_fno_factors",1)
        r.set("fno_factors_dump",2)

main(0)   # set daterange here 

