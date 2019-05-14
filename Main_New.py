#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 19:43:54 2019

@author: rajubingi
"""

from Get_Data_New import Get_Data_New

from Get_Yahoo import Extract_Fib_tickers

from SQL import SQL

from Load_Data_Database import Load_Data_Database

import datetime


start_time = datetime.datetime.now()

print("START#########  ")


      
################### FTSE100 full run  ###################
      
#FTSE = Get_Data_New("/Users/rajubingi/Equity_Data/List_FTSE.csv",'FTSE', 'LSE', 'Y','Y','Y','Y')


###################  FTSE100 Data load ###################
      
#FTSE = Get_Data_New("/Users/rajubingi/Equity_Data/List_FTSE.csv",'FTSE', 'LSE', 'N','Y','Y','Y')

################### RSI FTSE100 run ###################    

#FTSE = Get_Data_New_Current("/Users/rajubingi/Equity_Data/List_FTSE.csv",'FTSE')

################### FIB FTSE100 run ###################  

#FTSE = Extract_Fib_tickers("/Users/rajubingi/Equity_Data/List_FTSE.csv", 'FTSE')


#FTSESQL = SQL("/Users/rajubingi/Desktop/FTSE_Fundamentals.csv", 'FTSE')

FTSESQL = Load_Data_Database('N','N', 'Y','FTSE')
print (FTSESQL)




################### NIFTY100 full run  ###################
      
#NIFTY100 = Get_Data_New("/Users/rajubingi/Equity_Data/List_NIFTY.csv",'NIFTY', 'NSE', 'Y','Y','Y','Y')


###################  FTSE100 Data load ###################
      
#NIFTY100 = Get_Data_New("/Users/rajubingi/Equity_Data/List_NIFTY.csv",'NIFTY', 'NSE', 'N','Y','Y','Y')

################### RSI FTSE100 run ###################    

#NIFTY100 = Get_Data_New_Current("/Users/rajubingi/Equity_Data/List_NIFTY.csv",'NIFTY')

################### FIB FTSE100 run ###################  

#NIFTY100 = Extract_Fib_tickers("/Users/rajubingi/Equity_Data/List_NIFTY.csv",'NIFTY')

#NFTYSQL = SparkSQL("/Users/rajubingi/Desktop/NIFTY_Fundamentals.csv", 'NIFTY')

#NFTYSQL = Load_Data_Database('Y','N', 'Y','NIFTY')
      
#print (NIFTY100)   

#print (NFTYSQL)



print("END#########")
      

end_time = datetime.datetime.now()



print  ('start time:', start_time,'#####' ,'end time:', end_time)