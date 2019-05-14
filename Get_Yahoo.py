#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 20 19:46:33 2019

@author: rajubingi
"""

#import urllib2
#import pytz
from pyspark.sql import SparkSession
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from urllib.request import urlopen   
from scipy.signal import savgol_filter as smooth  
#from lxml.html import parse
import json
import os
import datetime
from datetime import timedelta
#import pandas as pd

from bs4 import BeautifulSoup
from lxml import html  
import requests
from time import sleep
#import argparse
from collections import OrderedDict
#from time import sleep
from pandas_datareader import DataReader
#import csv
import numpy as np
#import datetime
import pandas as pd
#import matplotlib.pyplot as plt
from yahoo_fin import stock_info as si

#######################################

def ticker_names(psite, plocation, pticker, pname ):
    #hdr = {'User-Agent': 'Mozilla/5.0'}
    #req = urllib2.Request(site, headers=hdr)
    
    
    page = urlopen(psite)
    soup = BeautifulSoup(page)

    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            #sector = str(col[psector].string.strip()).lower().replace(' ', '_')
            name = str(col[pname].string.strip()).lower().replace(' ', '_')
            if plocation == 'LSE':
              ticker = str(col[pticker].string.strip()).replace('.', '')+'.L'
            else :
              ticker = str(col[pticker].string.strip())
            
            if ticker not in tickers:
                #sector_tickers[sector] = list()
                 tickers.append((ticker,name))
    return tickers

#####################################
    
def stats(ticker):
        url = "http://finance.yahoo.com/quote/%s?p=%s"%(ticker,ticker)
        #print(url)
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        response = requests.get(url, verify=False)
    	#print ("Parsing %s"%(url))
        sleep(2)
        parser = html.fromstring(response.text)
        summary_table = parser.xpath('//div[contains(@data-test,"summary-table")]//tr')
        summary_data = OrderedDict()
        other_details_json_link = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{0}?formatted=true&lang=en-US&region=US&modules=summaryProfile%2CfinancialData%2CrecommendationTrend%2CupgradeDowngradeHistory%2Cearnings%2CdefaultKeyStatistics%2CcalendarEvents&corsDomain=finance.yahoo.com".format(ticker)
        
        summary_json_response = requests.get(other_details_json_link)
    
    #try:
        json_loaded_summary =  json.loads(summary_json_response.text)
        #print('')
        #print(json_loaded_summary)
        try: 
         sector = json_loaded_summary["quoteSummary"]["result"][0]["summaryProfile"]["sector"]
        except:
         sector = 'N/A'  
        try: 
         industry = json_loaded_summary["quoteSummary"]["result"][0]["summaryProfile"]["industry"]
        except:
         industry = 'N/A'
        try:
         desc = json_loaded_summary["quoteSummary"]["result"][0]["summaryProfile"]["longBusinessSummary"] 
        except:
         desc = ticker      		
        try: 
         y_Target_Est =  json_loaded_summary["quoteSummary"]["result"][0]["financialData"]["targetMeanPrice"]['raw']
        except:
         y_Target_Est = 0
        try: 
         earnings_list = json_loaded_summary["quoteSummary"]["result"][0]["calendarEvents"]['earnings']
         #print(earnings_list)
        except: 
         earnings_list = 0
        try:
         eps = json_loaded_summary["quoteSummary"]["result"][0]["defaultKeyStatistics"]["trailingEps"]['raw']
        except: 
         eps = 0
        #print(eps)
        
        
        
  
        '''datelist = []
        for i in earnings_list['earningsDate']:
            datelist.append(i['fmt'])
            earnings_date = ' to '.join(datelist)'''
        #print('test')
        for table_data in summary_table:
            raw_table_key = table_data.xpath('.//td[contains(@class,"C(black)")]//text()')
            raw_table_value = table_data.xpath('.//td[contains(@class,"Ta(end)")]//text()')
            table_key = ''.join(raw_table_key).strip()
            table_value = ''.join(raw_table_value).strip()
            summary_data.update({table_key:table_value})
        
        summary_data.update({'1y Target Est':y_Target_Est,'EPS (TTM)':eps,'Earnings Date':'','ticker':ticker,'industry':industry, 'sector': sector, 'desc':desc })
        return summary_data
    #except:
        #print ("Failed to parse json response")		
        return ''     
      
      
      

#####################################3

def history(ticker, start, end):
   

   data = DataReader(ticker, 'yahoo', start, end)
   data['Ticker'] = ticker
        
   return data



##########################################
   
def Get_RSI(ticker, location):
   
    past100Days  = []
    if (location == 'LSE'):
      filename = ticker.replace('.L', '_'+location)
    elif (location == 'NSE'):
      filename = ticker.replace('.NS', '_'+location)
    elif (location == 'BSE'):
      filename = ticker.replace('.BO', '_'+location)
      #print(filename)
    
    df = pd.read_csv("/Users/rajubingi/Equity_Data/Data/" +filename +".csv", sep=',')
    past100Days = list(df['close'].tail(100)) 
    #print(past100Days)
    if (len(past100Days) == 100):
        prices = np.array(past100Days)
        n = 14
        deltas = np.diff(prices)
        seed = deltas[:n+1]
        up = seed[seed >= 0].sum()/n
        down = -seed[seed < 0].sum()/n
        rs = up/down
        rsi = np.zeros_like(prices)
        rsi[:n] = 100. - 100./(1. + rs)
    
        for i in range(n, len(prices)):
            delta = deltas[i - 1]  # cause the diff is 1 shorter
    
            if delta > 0:
                upval = delta
                downval = 0.
            else:
                upval = 0.
                downval = -delta
    
            up = (up*(n - 1) + upval)/n
            down = (down*(n - 1) + downval)/n
    
            rs = up/down
            rsi[i] = 100. - 100./(1. + rs)
        
        
          #print(ticker, rsi)
        return  rsi[98],rsi[99]
    else:
        return  0
##########################################
   
def Get_RSI_Latest(ticker,df):
   
    #START_DATE = datetime.date.today() - timedelta(days=160)
    #df = si.get_data(ticker, start_date = START_DATE)
    past100Days = list(df['close'].tail(100)) 
    #print(past100Days)
    
    if (len(past100Days)== 100):
        
        prices = np.array(past100Days)
        n = 14
        deltas = np.diff(prices)
        seed = deltas[:n+1]
        up = seed[seed >= 0].sum()/n
        down = -seed[seed < 0].sum()/n
        rs = up/down
        rsi = np.zeros_like(prices)
        rsi[:n] = 100. - 100./(1. + rs)
    
        for i in range(n, len(prices)):
            delta = deltas[i - 1]  # cause the diff is 1 shorter
    
            if delta > 0:
                upval = delta
                downval = 0.
            else:
                upval = 0.
                downval = -delta
    
            up = (up*(n - 1) + upval)/n
            down = (down*(n - 1) + downval)/n
    
            rs = up/down
            rsi[i] = 100. - 100./(1. + rs)
    
    
    #print(ticker, rsi)
        #if (rsi[99] <= 40):
        
        if (rsi[99] >= 0):
         return  ticker, rsi[99]
        else:
         return  ''
       

#############################################
def Get_Stats_current(ticker):
    
    #print(ticker[0])
    df = si.get_stats(ticker[0])
    #print(df)
    df['ticker'] = ticker[0]
    
    #9,26,31,34,35,36,37,38,39,40,56,55
    df = df.iloc[[0,2,9,26,31,34,35,36,37,38,39,40,56,55], [0,1,2]]
    df = df.pivot(index='ticker', columns='Attribute', values='Value')
    df['RSI'] = ticker[1]
    df['Transdate'] = datetime.date.today()
 
    return df

#############################################
def Get_Stats(ticker,pindex):
    
    #print(ticker[0])
    df = si.get_stats(ticker)
    #print(df)
    df['ticker'] = ticker
    
    df = df.pivot(index='ticker', columns='Attribute', values='Value')
    #print(df)
    if not os.path.exists("/Users/rajubingi/Equity_Data/"+pindex + "_stats.csv") : 
       df.to_csv("/Users/rajubingi/Equity_Data/"+pindex + "_stats.csv",  mode='a')
    else:
       df.to_csv("/Users/rajubingi/Equity_Data/"+pindex + "_stats.csv",header=False,  mode='a')
 
    return 'success'
###############################################

def cal_avg_pe(pindex):      
    
    
    df = pd.read_csv("/Users/rajubingi/Equity_Data/" + pindex + ".csv", sep=',')
    #print(df)
    #df.pe_ratio = df.pe_ratio.astype(float).fillna(0.0)
    #df.pe_ratio = float(df.pe_ratio.replace(',', ''))
    df['pe_ratio'] = df['pe_ratio'].replace(',','', regex=True).astype(float)
    avg = df.groupby('Industry')['pe_ratio'].mean()
    
    df_pe = pd.DataFrame(round(avg), columns=['pe_ratio'])
    
    if os.path.exists("/Users/rajubingi/Equity_Data/" + pindex + "_AvgPE.csv") :
     df_pe.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_AvgPE.csv", header=False, mode='a')
    else:
     ftse_avg = []
     df_file = pd.DataFrame(ftse_avg, columns=['Industry','AvgPE'])
     df_file.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_AvgPE.csv",index=False)
     
     df_pe.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_AvgPE.csv", header=False, mode='a')
    
    return 'Success'
     
############################# ########

def last_five_days(ticker, location, pindex,date):      
    
    if (location == 'LSE'):
      filename = ticker.replace('.L', '_'+location)
    elif (location == 'NSE'):
      filename = ticker.replace('.NS', '_'+location)
    elif (location == 'BSE'):
      filename = ticker.replace('.BO', '_'+location)
      
    df = pd.read_csv("/Users/rajubingi/Equity_Data/Data/" + filename + ".csv", sep=',')
    df = df.tail(7)    
    
    
    df = df.pivot(index='ticker', columns='date', values='close')
    df['change'] = round(((df[df.columns[6]] - df[df.columns[5]])/df[df.columns[3]]) * 100,2)
    df['Trans_Date'] = date
    #change = round(((df[df.columns[4]] - df[df.columns[3]])/df[df.columns[3]]) * 100,2)
    
    df_days = pd.DataFrame(df, columns=df.columns)
    if not os.path.exists("/Users/rajubingi/Equity_Data/" + pindex + "_Last5days.csv") : 
       df_days.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_Last5days.csv",  mode='a')
    else:
       df_days.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_Last5days.csv",header=False,  mode='a')
    






    #print (df)
    
    return df

############################## Merging Files
def merge_files(pindex):
 df1 = pd.read_csv("/Users/rajubingi/Equity_Data/" + pindex + ".csv", sep=',')
 df2 = pd.read_csv("/Users/rajubingi/Equity_Data/" + pindex + "_tech.csv", sep=',')
 df3 = pd.read_csv("/Users/rajubingi/Equity_Data/" + pindex + "_AvgPE.csv", sep=',')
 df4 = pd.read_csv("/Users/rajubingi/Equity_Data/" + pindex + "_Last5days.csv", sep=',')

 #df5 =  pd.merge(df1, df2, on='Ticker')
 df6 =  pd.merge(df1, df3, on='Industry')
 df7 =  pd.merge(df6, df2,  on='ticker')
 df8 =  pd.merge(df7, df4,  on='ticker')
 #df8 = df8.sort_values('change')
               
 #print (df7)
 #df_filtered = df7.query('change < 0')
 #df_filtered = df_filtered.sort_values('change')
 #df_filtered.sort_values('change')
 
 df_details = pd.DataFrame(df6, columns=df6.columns)
 df_details.to_csv("/Users/rajubingi/Equity_Data/"+ pindex + "_details.csv",index = False )
 
 
 df_fundamentals = pd.DataFrame(df8, columns=df8.columns)
 df_fundamentals.to_csv("/Users/rajubingi/Desktop/"+ pindex + "_Fundamentals.csv",index = False )
 print ("/Users/rajubingi/Desktop/" + pindex + "_Fundamentals.csv" + " has been created")
 
 #df_selected = pd.DataFrame(df_filtered, columns=df_filtered.columns)
 #df_selected.to_csv("/Users/rajubingi/Desktop/"+ pindex + "_Fundamentals_selected.csv",index = False )
 #print ("/Users/rajubingi/Desktop/" + pindex + "_Fundamentals_selected.csv" + " has been created") 
 
 return 'Success'

##########################  Fibanocci
 
def Get_Fiba_latest(ticker):
    
    START_DATE = datetime.date.today() - timedelta(days=150)
    
    df = si.get_data(ticker, start_date = START_DATE)
    df = df.tail(100)
    
    price_min = df.close.min()
    
    price_max = df.close.max()
    
    diff = price_max - price_min
    level1 = price_max - 0.236 * diff
    level2 = price_max - 0.382 * diff
    level3 = price_max - 0.618 * diff
    
    '''print ("Level", "Price")
    print ("0 ", price_max)
    print ("0.236", level1)
    print ("0.382", level2)
    print ("0.618", level3)
    print ("1 ", price_min)'''
    
    df2 = pd.DataFrame(df, columns=['close'])
        
        
    #https://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_average_convergence_divergence_macd
        
    MACD = df2.rolling(window=12).mean() - df2.rolling(window=26).mean()
    MACD_9 = MACD.rolling(window=9).mean()  #Signal Line

        
    MA_20 = df2.rolling(window=20).mean()
    #MA_23 = df2.rolling(window=23).mean()
    #MA_38 = df2.rolling(window=38).mean()
    #MA_61 = df2.rolling(window=61).mean()
    
    return level3, level2, MACD, MACD_9, MA_20


##################################

def Get_Fiba(ticker,location):
    
    past100Days  = []
    if (location == 'LSE'):
      filename = ticker.replace('.L', '_'+location)
    elif (location == 'NSE'):
      filename = ticker.replace('.NS', '_'+location)
    elif (location == 'BSE'):
      filename = ticker.replace('.BO', '_'+location)
      #print(filename)
    
    df = pd.read_csv("/Users/rajubingi/Equity_Data/Data/" +filename +".csv", sep=',')
    #print(df)
    past100Days = df['close'].tail(150)
    #past100Days_vol = df['volume'].tail(75)
   
    #print(past100Days)
    
    MA_20_df = past100Days.rolling(window=20).mean()
    #Avg_vol_df =  past100Days_vol.rolling(window=20).mean()
    
    
    price_min = past100Days.min()
    
    price_max = past100Days.max()
    
    diff = price_max - price_min
    level1 = price_max - 0.236 * diff
    level2 = price_max - 0.382 * diff
    level3 = price_max - 0.618 * diff
    
    '''print ("Level", "Price")
    print ("0 ", price_max)
    print ("0.236", level1)
    print ("0.382", level2)
    print ("0.618", level3)
    print ("1 ", price_min)'''
    
    #df2 = pd.DataFrame(df, columns=['close'])
    n_fast = 12
    n_slow = 26
    
    EMAfast = pd.Series(past100Days.ewm(span=n_fast, min_periods=n_slow).mean())
    EMAslow = pd.Series(past100Days.ewm(span=n_slow, min_periods=n_slow).mean())
    MACD = pd.Series(EMAfast - EMAslow, name='MACD_' + str(n_fast) + '_' + str(n_slow))
    MACDsign = pd.Series(MACD.ewm(span=9, min_periods=9).mean(), name='MACDsign_' + str(n_fast) + '_' + str(n_slow))
    MACDdiff = pd.Series(MACD - MACDsign, name='MACDdiff_' + str(n_fast) + '_' + str(n_slow))
    df = df.join(MACD)
    df = df.join(MACDsign)
    df = df.join(MACDdiff)
        
    
    ''' old code    
    #https://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_average_convergence_divergence_macd
        
    MACD_df = df2.rolling(window=12).mean() - df2.rolling(window=26).mean()
    MACD_9_df = MACD_df.rolling(window=9).mean()  #Signal Line

        
    MA_20_df = df2.rolling(window=20).mean()
    #MA_23 = df2.rolling(window=23).mean()
    #MA_38 = df2.rolling(window=38).mean()
    #MA_61 = df2.rolling(window=61).mean()
    
    if (pd.isnull(list(MACD_df.tail(2)['close']))[0]):
         MACD = 0 
         MACD_Yesterday = 0
    else:
         #print ((list(MACD_df.tail(2)['close']))[0])
         MACD = (list(MACD_df.tail(2)['close']))[1]
         MACD_Yesterday = (list(MACD_df.tail(2)['close']))[0]
         
        #print (MACD,MACD_Yesterday )
        
        
         
    if (pd.isnull(list(MACD_9_df.tail(1)['close']))[0]):
          MACD_9 = 0 
    else:
          MACD_9 = (list(MACD_9_df.tail(1)['close']))[0]
         
        
        
    if (pd.isnull(list(MA_20_df.tail(2)['close']))[0]):
         MA_20 = 0 
         MA20_Yesterday = 0
    else:
         MA_20 = (list(MA_20_df.tail(2)['close']))[1]
         MA20_Yesterday = (list(MA_20_df.tail(2)['close']))[0]
    '''
    
    if (pd.isnull(list(df.tail(2)['MACD_12_26']))[0]):
         MACD = 0 
         MACD_Yesterday = 0
    else:
         #print ((list(MACD_df.tail(2)['close']))[0])
         MACD = (list(df.tail(2)['MACD_12_26']))[1]
         MACD_Yesterday = (list(df.tail(2)['MACD_12_26']))[0]
         
    if (pd.isnull(list(df.tail(1)['MACDsign_12_26']))[0]):
          MACD_9 = 0 
    else:
          MACD_9 = (list(df.tail(1)['MACDsign_12_26']))[0]
        
    if (pd.isnull(list(MA_20_df.tail(2)))[0]):
         MA_20 = 0 
         MA20_Yesterday = 0
    else:
         MA_20 = (list(MA_20_df.tail(2)))[1]
         MA20_Yesterday = (list(MA_20_df.tail(2)))[0]
    
    '''if (pd.isnull(list(Avg_vol_df.tail(1)))[0]):
          Avg_Vol = 0 
    else:
          Avg_Vol = (list(Avg_vol_df.tail(1)))[0]'''
    
    #print(list(MA_20_df.tail(2))[0])    
    
    
    return level3,level2, MACD, MACD_Yesterday, MACD_9, MA_20, MA20_Yesterday

def Call_PPSR_latest(ticker, df):
    
    #print (datetime.datetime.now())
        #print(ticker)
 #if (ticker != 'III.L'):
        #START_DATE = datetime.date.today() - timedelta(days=2)
        data = df.tail(100)
        #print(data)
        #data = df.set_index('date')
        
        DATE = data.index.tolist()
        #print (DATE[0])
       
        START_DATE = DATE[0]
        #print(START_DATE)
        if(START_DATE.weekday() == 5):
            NEXT_DATE = (START_DATE + timedelta(days=3))
        elif(START_DATE.weekday() == 6):
            NEXT_DATE = (START_DATE + timedelta(days=2))
        else:
            NEXT_DATE = (START_DATE + timedelta(days=1))
        
        #def PPSR(data):
            
        #data = si.get_data(ticker, start_date = START_DATE)
        
        
        pindex=pd.bdate_range(NEXT_DATE, datetime.date.today() +  timedelta(days=0), freq='B')
        
        Holiday = []
        for each in pindex:
            if(
                (each.month==12 and (each.day==25 or each.day==26)) 
                or (each.month==1 and  each.day==1)
                or (each.month==4 and (each.day==19 or each.day==22)) 
                or (each.month==5 and (each.day==6 or each.day==27)) 
                or (each.month==8 and  each.day==26)
                )  :
                
                Holiday.append(each)
        
        pindex = pindex.drop(Holiday)
        
        #omit_dates(df, years, holiday_dates, omit_days_near=2, omit_weekends=True)
        
        #print (pindex )
        #print(pindex)
        
        #print(pindex)
        #print (len(pindex))
        
        data = (data.tail(len(pindex)))
        #print(data)
        #print (len(data))
        
        if (len(pindex)==len(data)):
            
            pdata = {'P_high':list(data['high']),'P_low':list(data['low']),'P_close':list(data['close'])}
            pdf = pd.DataFrame(pdata,index=pindex)
            #print(pdf)
            
            data=data.join(pdf)
            
            PP = pd.Series((data['P_high'] + data['P_low'] + data['P_close'] + data['open']) / 4)  
            R1 = pd.Series(2 * PP - data['P_low'])  
            S1 = pd.Series(2 * PP - data['P_high'])  
            R2 = pd.Series(PP + data['P_high'] - data['P_low'])  
            S2 = pd.Series(PP - data['P_high'] + data['P_low'])  
            R3 = pd.Series(data['P_high'] + 2 * (PP - data['P_low']))  
            S3 = pd.Series(data['P_low'] - 2 * (data['P_high'] - PP))
            
            #Current High less the current Low
            ATR1 = abs(pd.Series(data['high'] - data ['low'])  )
            
            #Current High less the previous Close (absolute value)
            ATR2 = abs(pd.Series(data['high'] - data ['P_close']) )
            
            #Current Low less the previous Close (absolute value)
            ATR3 = abs (pd.Series(data['low'] - data ['P_close']) )
            
            ATR_DF = pd.DataFrame({'ATR1': ATR1,'ATR2': ATR2, 'ATR3': ATR3})
            #print(ATR_DF)
            ATR_DF_MAX = pd.DataFrame ({'ATR':ATR_DF.max(axis = 1)})
            ATR_AVG = ATR_DF_MAX.rolling(window=14).mean()
            #print(ATR_AVG)
            #pd.max(axis=None, skipna=None, level=None, numeric_only=None, **kwargs)[source]  ,'ATR1':ATR1, 'ATR2':ATR2, 'ATR3':ATR3
            
            psr = {'PP':PP, 'R1':R1, 'S1':S1, 'R2':R2, 'S2':S2, 'R3':R3, 'S3':S3} 
            
            PSR = pd.DataFrame(psr) 
               
            #print (PSR)
            data= data.join(PSR) 
            data = data.join(ATR_AVG)
            #print(data)
            #del data.index.name
            
            return (data.tail(2))
        else:
            print(ticker + "  Index length does not match")
            return ''

def Call_PPSR(ticker, location):
    
          if (location == 'LSE'):
            filename = ticker.replace('.L', '_'+location)
          elif (location == 'NSE'):
            filename = ticker.replace('.NS', '_'+location)
          elif (location == 'BSE'):
            filename = ticker.replace('.BO', '_'+location)
          #print(filename)
        
          df = pd.read_csv("/Users/rajubingi/Equity_Data/Data/" +filename +".csv", sep=',')
          df = df.tail(100)
          data = df.set_index('date')
          
          DATE = list(df['date'])
          START_DATE = datetime.datetime.strptime(DATE[0], '%Y-%m-%d').date() 
          #START_DATE = datetime.date.today() - timedelta(days=150)
          #print(START_DATE)
          
          if(START_DATE.weekday() == 5):
            NEXT_DATE = (START_DATE + timedelta(days=3))
          elif(START_DATE.weekday() == 6):
            NEXT_DATE = (START_DATE + timedelta(days=2))
          else:
            NEXT_DATE = (START_DATE + timedelta(days=1))
        
          pindex=pd.bdate_range(NEXT_DATE, datetime.date.today() +  timedelta(days=0), freq='B')
          Holiday = []
          for each in pindex:
            if(
                (each.month==12 and (each.day==25 or each.day==26)) 
                or (each.month==1 and  each.day==1)
                or (each.month==4 and (each.day==19 or each.day==22)) 
                or (each.month==5 and (each.day==6 or each.day==27)) 
                or (each.month==8 and  each.day==26)
                )  :
                
                Holiday.append(each)
        
          pindex = pindex.drop(Holiday) 
          #print(pindex)
        
    
          data = (data.tail(len(pindex)))
        #print(data)
        #print (len(data))
        
          if (len(pindex)==len(data)):
            
            pdata = {'P_high':list(data['high']),'P_low':list(data['low']),'P_close':list(data['close'])}
            pdf = pd.DataFrame(pdata,index=pindex)
            #print(pdf)
            
            data=data.join(pdf)
            
            PP = pd.Series((data['P_high'] + data['P_low'] + data['P_close'] +  data['open']) / 4)  
            
            
            
                       #Current High less the current Low
            ATR1 = abs(pd.Series(data['high'] - data ['low'])  )
            
            #Current High less the previous Close (absolute value)
            ATR2 = abs(pd.Series(data['high'] - data ['P_close']) )
            
            #Current Low less the previous Close (absolute value)
            ATR3 = abs (pd.Series(data['low'] - data ['P_close']) )
            
            ATR_DF = pd.DataFrame({'ATR1': ATR1,'ATR2': ATR2, 'ATR3': ATR3})
            #print(ATR_DF)
            ATR_DF_MAX = pd.DataFrame ({'ATR':ATR_DF.max(axis = 1)})
            ATR_AVG = ATR_DF_MAX.rolling(window=14).mean()
            #print(ATR_DF_MAX)
            #pd.max(axis=None, skipna=None, level=None, numeric_only=None, **kwargs)[source]  ,'ATR1':ATR1, 'ATR2':ATR2, 'ATR3':ATR3
            
            psr = {'PP':PP} #, 'open_val':open_val, 'low_val':low_val, 'high_val':high_val, 'volume':volume
            
            PSR = pd.DataFrame(psr) 
               
            #print (PSR)
            #print(data)
            data= data.join(PSR) 
            data = data.join(ATR_AVG)
            #print(data)
            #del data.index.name
            #print(data.tail(2))
            return (data.tail(1))
          else:
            print(ticker + "  Index length does not match")
            return ''


def cal_SL_RL(ticker, location):

    
    #START_DATE = datetime.date.today() - timedelta(days=180)
    #df = si.get_data(ticker, start_date = START_DATE)
    if (location == 'LSE'):
        filename = ticker.replace('.L', '_'+location)
    elif (location == 'NSE'):
        filename = ticker.replace('.NS', '_'+location)
    elif (location == 'BSE'):
        filename = ticker.replace('.BO', '_'+location)
          #print(filename)
        
    df = pd.read_csv("/Users/rajubingi/Equity_Data/Data/" +filename +".csv", sep=',')
     
     
    past100Days = list(df['close'].tail(150)) 
    if ( len(past100Days) == 150):
        ltp = np.array(past100Days)
        #if ticker == 'FCIT.L' : 
         # print(ltp)
        current_price = ltp[149]
        #print(current_rpice)
        #ltp = np.ravel(ltp)
        #print(ltp)
        n = 4
        """
        This function takes a numpy array of last traded price
        and returns a list of support and resistance levels 
        respectively. n is the number of entries to be scanned.
        """
    
    
        #converting n to a nearest even number
        if n%2 != 0:
            n += 1
        
        n_ltp = ltp.shape[0]
        #print(n_ltp)
    
        # smoothening the curve
        ltp_s = smooth(ltp, (n+1), 3)
        #print(ltp_s)
    
        #taking a simple derivative
        ltp_d = np.zeros(n_ltp)
        #print(ltp_d)
        
        ltp_d[1:] = np.subtract(ltp_s[1:], ltp_s[:-1])
        #print(ltp_d[1:])
     
        resistance = []
        support = []
        
        for i in range(n_ltp - n):
            arr_sl = ltp_d[i:(i+n)]
            #print(len(arr_sl))
            #print(arr_sl)
            first = arr_sl[:len(arr_sl)//2] #first half  A[:len(A)//2]
            #print(first)
            last = arr_sl[len(arr_sl)//2:] #second half
            #print(last)
             
             
            r_1 = np.sum(first > 0)
            #print(r_1)
            r_2 = np.sum(last < 0)
            #print(r_2)
            
            
            s_1 = np.sum(first < 0)
            s_2 = np.sum(last > 0)
    
            #local maxima detection
            if (r_1 == (n/2)) and (r_2 == (n/2)):
                res = ltp[i+int((n/2)-1)]
                if (res >= current_price):
                
                  resistance.append(res)
    
            #local minima detection
            if (s_1 == (n/2)) and (s_2 == (n/2)):
                sup = ltp[i+int((n/2)-1)]
                if (sup < current_price):
                  support.append(sup)
        
        support.sort(reverse = True)
        resistance.sort()
        return  {'support': support[0:3], 'resistance': resistance[0:3]}

def cal_SL_RL_latest(ticker, df):

    
    #START_DATE = datetime.date.today() - timedelta(days=180)
    #df = si.get_data(ticker, start_date = START_DATE)
    past100Days = list(df['close'].tail(100)) 
    if ( len(past100Days) >= 99):
        ltp = np.array(past100Days)
        #print(ltp)
        current_price = ltp[99]
        #print(current_rpice)
        #ltp = np.ravel(ltp)
        #print(ltp)
        n = 4
        """
        This function takes a numpy array of last traded price
        and returns a list of support and resistance levels 
        respectively. n is the number of entries to be scanned.
        """
    
    
        #converting n to a nearest even number
        if n%2 != 0:
            n += 1
        
        n_ltp = ltp.shape[0]
        #print(n_ltp)
    
        # smoothening the curve
        ltp_s = smooth(ltp, (n+1), 3)
        #print(ltp_s)
    
        #taking a simple derivative
        ltp_d = np.zeros(n_ltp)
        #print(ltp_d)
        
        ltp_d[1:] = np.subtract(ltp_s[1:], ltp_s[:-1])
        #print(ltp_d[1:])
     
        resistance = []
        support = []
        
        for i in range(n_ltp - n):
            arr_sl = ltp_d[i:(i+n)]
            #print(len(arr_sl))
            #print(arr_sl)
            first = arr_sl[:len(arr_sl)//2] #first half  A[:len(A)//2]
            #print(first)
            last = arr_sl[len(arr_sl)//2:] #second half
            #print(last)
             
             
            r_1 = np.sum(first > 0)
            #print(r_1)
            r_2 = np.sum(last < 0)
            #print(r_2)
            
            
            s_1 = np.sum(first < 0)
            s_2 = np.sum(last > 0)
    
            #local maxima detection
            if (r_1 == (n/2)) and (r_2 == (n/2)):
                res = ltp[i+int((n/2)-1)]
                if (res >= current_price):
                
                  resistance.append(res)
    
            #local minima detection
            if (s_1 == (n/2)) and (s_2 == (n/2)):
                sup = ltp[i+int((n/2)-1)]
                if (sup < current_price):
                  support.append(sup)
        
        support.sort(reverse = True)
        resistance.sort()
        return  {'support': support[0:3], 'resistance': resistance[0:3]}


def Extract_Fib_tickers(psite, pindex):
    
    
    START_DATE = datetime.date.today() - timedelta(days=180)
    Trans_date = datetime.date.today()
    
    list_of_tickers = pd.read_csv(psite, sep=',',index_col=0)
    tickers =list_of_tickers.index
    #tickers = ['GVC.L','TUI.L','ABF.L']
    
    Fib = []
    
    for ticker in tickers:
        #print(ticker)
        df = si.get_data(ticker, start_date = START_DATE)
        df = df.tail(100)
        
        df2 = pd.DataFrame(df, columns=['close'])
        
        
        #https://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_average_convergence_divergence_macd
        
        MACD_df = df2.rolling(window=12).mean() - df2.rolling(window=26).mean()
        
        
        MACD_9 = MACD_df.rolling(window=9).mean()  #Signal Line

        
        MA_20_df = df2.rolling(window=20).mean()
        MA_38 = df2.rolling(window=38).mean()
        MA_61 = df2.rolling(window=61).mean()
        

        
        '''if (pd.isnull(list(MA_12.tail(1)['close']))[0]):
         MA_12 = 0 
        else:
         MA_12 = (list(MA_12.tail(1)['close']))[0]

        if (pd.isnull(list(MA_26.tail(1)['close']))[0]):
         MA_26 = 0 
        else:
         MA_26 = (list(MA_26.tail(1)['close']))[0]'''
        
        
        
        if (pd.isnull(list(MACD_df.tail(2)['close']))[0]):
         MACD = 0 
         MACD_Yesterday = 0
        else:
         #print ((list(MACD_df.tail(2)['close']))[0])
         MACD = (list(MACD_df.tail(2)['close']))[1]
         MACD_Yesterday = (list(MACD_df.tail(2)['close']))[0]
         
        #print (MACD,MACD_Yesterday )
        
        
         
        if (pd.isnull(list(MACD_9.tail(1)['close']))[0]):
          MACD_9 = 0 
        else:
          MACD_9 = (list(MACD_9.tail(1)['close']))[0]
         
        
        
        if (pd.isnull(list(MA_20_df.tail(2)['close']))[0]):
         MA_20 = 0 
         MA20_Yesterday = 0
        else:
         MA_20 = (list(MA_20_df.tail(2)['close']))[1]
         MA20_Yesterday = (list(MA_20_df.tail(2)['close']))[0]
         
         
        if (pd.isnull(list(MA_38.tail(1)['close']))[0]):
         MA_38 = 0 
        else:
         MA_38 = (list(MA_38.tail(1)['close']))[0]
         
        if (pd.isnull(list(MA_61.tail(1)['close']))[0]):
         MA_61 = 0 
        else:
         MA_61 = (list(MA_61.tail(1)['close']))[0]
        
        price_min = df.close.min()
        price_max = df.close.max()
        
        diff = price_max - price_min
        level1 = price_max - 0.236 * diff
        level2 = price_max - 0.382 * diff
        level3 = price_max - 0.618 * diff
        
        #print(level3)
        
        close = (list(df.tail(1)['close']))[0]
        #print (close)
        RSI = Get_RSI_Latest(ticker,df)
        
        if RSI  is not  None:
            RSI_Value= RSI[1]
        else :
            RSI_Value = 0
        
        #print(ticker, RSI_Value, MA_20 )
        
        '''print ("Level", "Price")
        print ("0 ", price_max)
        print ("0.236", level1)
        print ("Sell(0.382):", level2)
        print ("Buy(0.618):", level3)
        print ("1 ", price_min)
        
        
        print (((level3 - close[0])/close[0])*100)
        print (((level3 - close[0])/level3)*100)'''
        
        buying_price  = abs(level3 - close)
        selling_price = abs(level2 - close)
        
        df1 = pd.read_csv("/Users/rajubingi/Desktop/" + pindex + "_Fundamentals.csv"
                               , usecols=["ticker","Comapany_Name","Index","Sector","Industry","Dividend","Ex_Dividend_Date","Beta","pe_ratio","AvgPE","52_week_low","52_week_high"])

        #print (df1)
        df2 = df1.set_index('ticker')
        try:
            df2= df2.loc[ticker]
            
            Company_Name = df2['Comapany_Name']
            Index  = df2['Index']
            Sector = df2['Sector']
            Industry = df2['Industry']
            Beta = df2['Beta']
            week_low = df2['52_week_low']
            week_high = df2['52_week_high']
            pe_ratio = df2['pe_ratio']
            Dividend = df2['Dividend']
            Company_Name = df2['Comapany_Name']
            Ex_Dividend_Date = df2['Ex_Dividend_Date']
            AvgPE = df2['AvgPE']
            
            
            
            
            if ( #((((buying_price)/close)*100) <= 2 or (((buying_price)/level3)*100) <= 0.5 ) and
                 (round(RSI_Value) >= 45 and round(RSI_Value) <= 68 and close >= MA_20 #and  close <= MA20_Yesterday
                  )
                
                #and (MACD_Yesterday < MACD_9)
                and (MACD > MACD_9 )
                and (MACD > 0 and MACD_9 > 0)
                
                and pe_ratio > 1
    
                ):
                
                 print('') 
                 print ('################   BUY ' + ticker + ' ################  ')
                 #print("Ideal Buy Price: " + str(level3)  )
                 #print ("Current Price: " + str(close) )
                 
                 PPSR = Call_PPSR_latest(ticker, df)
                 #print(PPSR)
                 SR = cal_SL_RL_latest(ticker,df)
                 #print(SR)
                 
                 
                 
                 if PPSR is not  None and len(PPSR) >= 1 : 
                    PP = (list(PPSR['PP']))[1]
                    #ATR_List = (list(PPSR['ATR']))
                    ATR = (list(PPSR['ATR']))[1] #((ATR_List[0]* 13) + ATR_List[1]) / 14
                    #print (PPSR,  ATR )
                 else :
                    PP = 0
                    ATR = 0
                 ################### support 
                 
                 
                 
                 if SR  is not  None:    
                    if (len(SR['support']) == 3):
                        
                        S1 = SR['support'][0] 
                        S2 = SR['support'][1] 
                        S3 = SR['support'][2]
                    elif (len(SR['support']) == 2):
                        S1 = SR['support'][0] 
                        S2 = SR['support'][1]
                        S3 = 0
                    elif (len(SR['support']) == 1):
                        S1 = SR['support'][0] 
                        S2 = 0
                        S3 = 0
                    else:
                        S1 = 0
                        S2 = 0
                        S3 = 0
                    
                    if (len(SR['resistance']) == 3):
                        
                        R1 = SR['resistance'][0] 
                        R2 = SR['resistance'][1] 
                        R3 = SR['resistance'][2]
                    elif (len(SR['resistance']) == 2):
                        R1 = SR['resistance'][0] 
                        R2 = SR['resistance'][1]
                        R3 = 0
                    elif (len(SR['resistance']) == 1):
                        R1 = SR['resistance'][0] 
                        R2 = 0
                        R3 = 0
                    else:
                        R1 = 0
                        R2 = 0
                        R3 = 0
                 else:
                   
                    R1 =  0
                    R2 =  0
                    R3 =  0
                    S1 =  0
                    S2 =  0
                    S3 =  0
                    
                #print(round(R2)
                 Fib.append((ticker,Trans_date,Company_Name, Index, Sector,Industry,Beta, pe_ratio, AvgPE, Dividend,Ex_Dividend_Date, round(MACD_Yesterday,2),  round(MACD,2),round(MACD_9,2), round(MA20_Yesterday), round(MA_20), round(MA_38), round(MA_61),  round(close), round(level3), str(round((((level3 - close)/close)*100))) + '%'
                                  ,round(level2),str(round((((level2 - level3)/level3)*100))) + '%', week_low, week_high, round(RSI_Value),round(ATR),round(PP),round(R1),round(S1),round(R2),round(S2),round(R3),round(S3)))
             
                 print('') 
            #else:
            elif (
                   #((((selling_price)/close)*100) <= 2 or (((selling_price)/level2)*100) <= 0.5 ) or
                   round(RSI_Value) >= 70
                   or  close < MA_20
                   or  MACD < MACD_9 
                   or  (MACD < 0 and MACD_9 < 0)
                   
                   ):
                 print("### Sell " + ticker + '  ### ' ,'CurrentPrice:', round(close),'   ' ,' SellPrice:', round(level2))
        except:        
            print(ticker + ' does not exist')                                  
          
    df = pd.DataFrame (Fib,columns = ['ticker','Trans_date','Company_Name','Index','Sector','Industry','Beta','pe_ratio','AvgPE','Dividend','Ex_Dividend_Date' ,'MACD_Yesterday', 'MACD', 'MACD_9','MA20_Yesterday', 'MA_20', 'MA_38', 'MA_61','Current_Price', 'Buying_Price','Price_Diff', 'Selling_Price','Profit%','52_week_low','52_week_high','RSI','ATR','Pivot_Point','R1','S1','R2','S2','R3','S3'])
        
    '''df1 = pd.read_csv("/Users/rajubingi/Desktop/" + pindex + "_Fundamentals.csv", index_col=[1]
                               , usecols=["ticker","Comapany_Name","Index","Sector","Industry","Dividend","Ex_Dividend_Date","Beta","pe_ratio","AvgPE","52_week_low","52_week_high"])
       df2 =  pd.merge(df1, df, on='ticker')'''
    
    df.to_csv("/Users/rajubingi/Desktop/" + pindex + "_fib.csv", index=False)
    
    return 'Success'