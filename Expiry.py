#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
#from collections import OrderedDict
import time
#from dateutil import parser
import os
import Utility_functions
import logging
import datetime
import redis
from cassandra.cluster import Cluster
from cassandra.util import datetime_from_timestamp



import warnings
warnings.filterwarnings("ignore")

master_dir = 'X:\\Data_dumpers\\ETL\\Master\\'
download_dir = 'X:\\ETL\\Download\\'
output_dir = 'X:\\ETL\\Output\\'
log_path = "X:\\ETL\\"
email_dir = "X:\\Output\\"
redis_host = 'localhost'
cassandra_host = "172.17.9.51"

logging.basicConfig(filename=log_path+"test.log",
                        level=logging.DEBUG,
                        format="%(asctime)s:%(levelname)s:%(message)s")

def dateparse(date):
    '''Func to parse dates'''    
    date = pd.to_datetime(date, dayfirst=True)    
    return date

# Define a pandas factory to read the cassandra data into pandas dataframe:
def pandas_factory(colnames, rows):
    
    '''Pandas factory to determine cassandra data into python df'''
    return pd.DataFrame(rows, columns=colnames)



def fetch_dollar():
    
    # create python cluster object to connect to your cassandra cluster (specify ip address of nodes to connect within your cluster)
    cluster = Cluster([cassandra_host])
    
    session = cluster.connect('rohit')
    #logging.info('Using test_df keyspace')
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    
    query=session.execute("SELECT * from bloom_usd_inr where date>='{}' allow filtering".format(
            datetime.datetime.now().date()-datetime.timedelta(days=10 )))
    result=query._current_rows

    result['date'] = result['date'].apply(lambda row: row.date())
    result.sort_values(by='date', inplace=True)

   
    return round(result.tail(1)['usd_inr'].values[0],2)




def main_processor(expiry, month, year, file_name,name, dollar_value):
    # if valid Expiry date process and generate reports
    # Remove duplicate rows if any in master files
    
    Utility_functions.drop_duplicate_rows('RollData.xlsx')
    Utility_functions.drop_duplicate_rows('Indices_dump.xlsx')
    Utility_functions.drop_duplicate_rows('Unrolled_dumped.xlsx')

    # Dataframe with product far, near and rollover values
    logging.info('Calling bhavcopy processor function...')
    data, master = Utility_functions.bhavcopy_processor(file_name)
    logging.info('Calling Sector rollovers...')
    result = Utility_functions.sector_rollovers(data)

    # Dump data
    logging.info('Calling rollover_data_dumper function for dumping to master files...')
    dumper, indices_rollover, unrolled_dumper = Utility_functions.rollover_data_dumper(data, master, result, expiry,
                                                                                       month, year, name, dollar_value)

    # get top and low stocks from grt than avg sheet
    logging.info('Calling greater than average function...')
    grt_avg_top_stocks, grt_avg_low_stocks = Utility_functions.greaterthanAverage(expiry, month, year)
    # get top unrolled OI stocks
    unrolled_OI_top_stocks = {}; average_unrolled ='';
    if expiry == 'E':
        logging.info('Unrolled_OI is not calculated for expiry day...')
        average_unrolled = Utility_functions.unrolled_OI_Calculator(expiry, month, year,0)
        pass
    else:
        # calculate unrolled OI
        logging.info('Calculating unrolled_OI for {0}'.format(expiry))
        unrolled_OI_top_stocks, average_unrolled = Utility_functions.unrolled_OI_Calculator(expiry, month, year,1)
    # increased activity
    increased_activity = []
    if expiry in ['E-5', 'E-4', 'E-3']:
        logging.info('Skip: Increased activity.')
        pass
    else:
        # calculate increased activity
        logging.info('Calculating increased activity for {0}'.format(expiry))
        increased_activity = Utility_functions.increased_activity_calc(expiry, month, year)

    # get top 2 leading and lagging sectors
    logging.info('Get leading and lagging sectors...')
    leading_sectors, lagging_sectors, avg_indices = Utility_functions.leading_lagging_sectors(expiry, month, year)

    # call final print
    logging.info('Final report generation...')
    Utility_functions.print_final_report(expiry, avg_indices, increased_activity, leading_sectors, lagging_sectors,
                                         grt_avg_top_stocks, grt_avg_low_stocks, unrolled_OI_top_stocks, name, month, year, average_unrolled)

    # Update master files
    logging.info('Dump results to master dump files...')
    Utility_functions.append_df_to_excel('RollData.xlsx', df=dumper, header=None, index=False)
    Utility_functions.append_df_to_excel('Indices_dump.xlsx', df=indices_rollover, header=None, index=False)
    Utility_functions.append_df_to_excel('Unrolled_dumped.xlsx', df=unrolled_dumper, header=None, index=False)





def main(nd, dollar_value):
    
    # read holiday master
    print "Dollar value: ", dollar_value
    holiday_master = pd.read_csv(master_dir+'Holidays_2019.txt', delimiter=',',
                                 date_parser=dateparse, parse_dates={'date':[0]})    
    holiday_master['date'] = holiday_master.apply(lambda row: row['date'].date(), axis=1)
    
    
    df = pd.read_excel(master_dir+"Expiry_dates_master.xlsx")
    df.dropna(inplace=True)
    df['d'] = df.apply(lambda row: datetime.datetime.strptime("{}{}{}".format(row['Date'],
                                                          row['Month'], row['Year']),"%d%b%Y").date() , axis=1)
        
    date = datetime.datetime.today().date() - datetime.timedelta(days=nd)
    print "Processing for date {}".format(date)
    
    
    r = redis.Redis(host=redis_host, port=6379) 
    expiry_flag = int(r.get('expiry_report_flag'))
    
    # run only if an expiry week 
    if len(df[df['d']==date]['Expiry']) == 0 :
        print 'Not an Expiry week '
        logging.info('Not an expiry week; logging off')
        return -1
    else:
        logging.info('{} is expiry date; process initiating...'.format(date))
    
    if len(holiday_master[holiday_master['date']==date])==0 :
        # working day so run until bhavcopy is downloaded       
        while expiry_flag == 0 and datetime.datetime.now().time() > datetime.time(17,28):
            # sleep for 2 min
            print 'Sleep for 2 min...'
            time.sleep(120)
            logging.info('{} is Working day : sleep for 2 minutes '.format(date))
           
            
            expiry_flag = int(r.get('expiry_report_flag'))
        
    elif len(holiday_master[holiday_master['date']==date])==1:
        print 'Holiday: skip for current date : ',date
        logging.info('Holiday: skip for current date : ',date)
        return -1 
    
 
    
    # traverse through all bhavcopies    
    for root, d, f in os.walk(download_dir):
        if len(f) == 0:
            logging.error('Download folder is empty, add bhavcopy to downlaod folder')
        
        else:            
            logging.info('Traverese through all the bhavcopies from Download folder...')
            for csv_file in f:
                
                file_name = csv_file
                # extract date from input bhavcopy name
                logging.info('Processing {0} bhavcopy...'.format(file_name))
                
                name = file_name[2:11]
                date, month, year = name[:2], name[2:5], name[5:]
        
                # read expiry from master dump to get expiry code
                # if not an expiry throw exception
                try:
                    dates = pd.read_excel(master_dir + 'Expiry_dates_master.xlsx')
                    expiry = str(dates[(dates['Date'] == int(date)) &
                                       (dates['Month'] == month) & 
                                       (dates['Year'] == int(year))]['Expiry'].values[0])
                    logging.info('{0}/{1}/{2} is a valid expiry, process initiated...'.format(date,month,year))
                    print file_name
                    main_processor(expiry, month, year, file_name,name, dollar_value)
        
                except IndexError:
                    
                    logging.info('Exception: {0}/{1}/{2} - Not a valid expiry date...'.format(date, month, year))
    
        
    
    r.set('expiry_report_flag',0) # reset expiry flag
	


start_time = time.time()

if __name__ == '__main__':
    
    main(nd=0, dollar_value=fetch_dollar())   # set today date range and dollar value here  

end_time = time.time()

logging.info('Time taken to process :'.format(end_time - start_time))
print "Execution time: {0} Seconds.... ".format(end_time - start_time)
