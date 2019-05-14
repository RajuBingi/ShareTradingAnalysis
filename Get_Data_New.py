#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  1 20:51:33 2019

@author: rajubingi
"""



#from Get_Yahoo import ticker_names
from Get_Yahoo import stats,history,Get_RSI,last_five_days,cal_avg_pe,merge_files,Get_Fiba
from Get_Yahoo import Get_RSI_Latest,Get_Stats,Get_Fiba_latest, Call_PPSR, Call_PPSR_latest,cal_SL_RL
from yahoo_fin import stock_info as si
import datetime
import pandas as pd
import os.path
import csv
from datetime import timedelta
import os
from time import sleep
#import shutil
#import csv
#from numpy import loadtxt

START = '2017-01-01'
END = datetime.date.today()
if(END.weekday() == 5):
    END = (END - timedelta(days=1))
elif(END.weekday() == 6):
    END = (END - timedelta(days=2))
else:
    END = datetime.date.today()   

#print(END)




############################ 

def Get_Data_New(psite, pindex, plocation,  pstatistics, phistory, pcurrentdata, prsi ):
    ftse = []
    ftse_tech = []
    ftse_extra = []
    ftse_error =[]
    

      
    if os.path.exists("/Users/rajubingi/Desktop/" + pindex + "_extra.csv") and (pstatistics == 'Y') :      
      os.remove("/Users/rajubingi/Desktop/" + pindex + "_extra.csv")
      df = pd.DataFrame(ftse_extra, columns=['Transdate','Ticker','Desc'])
      df.to_csv("/Users/rajubingi/Desktop/" + pindex + "_extra.csv", index=False)
      
     

   
    if os.path.exists("/Users/rajubingi/Equity_Data/" + pindex + "data_Error.csv") and (pstatistics == 'Y'):   
      os.remove("/Users/rajubingi/Equity_Data/" + pindex + "data_Error.csv")
   

   
    if os.path.exists("/Users/rajubingi/Equity_Data/" + pindex + "_AvgPE.csv") and (pstatistics == 'Y') : 
      os.remove("/Users/rajubingi/Equity_Data/" + pindex + "_AvgPE.csv")
      
      df_error = pd.DataFrame(ftse_error, columns=['Industry','AvgPE'])
      df_error.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_AvgPE.csv",index=False)

    if os.path.exists("/Users/rajubingi/Equity_Data/" + pindex + "_Last5days.csv") : 
      os.remove("/Users/rajubingi/Equity_Data/" + pindex + "_Last5days.csv")
     
    if os.path.exists("/Users/rajubingi/Equity_Data/"+pindex + "_stats.csv") : 
     os.remove("/Users/rajubingi/Equity_Data/"+pindex + "_stats.csv")

    

    



    df_error = pd.DataFrame(ftse_error, columns=['Transdate','Ticker','Error'])
    df_error.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_data_Error.csv",index=False)



  

################################################################## Extarcting Tickers


    #ticker_name = ticker_names(psite, plocation, pticker_loc, pname_loc)
 #ticker_name = [('GVC.L', 'GVC Holding'),('ABF.L', 'GVC Holding'),('TUI.L', 'GVC Holding')]
#print (ticker_name)

#ticker_name = [('GVC.L', 'GVC Holding')]

#res_list = [ticker.replace('.', '')+'.L' for ticker, name in ticker_name]

#res_list = ['GVC.L']
 
    with open(psite) as inputfile:
            reader = csv.reader(inputfile)
            ticker_name = list(reader)
   
    
    #list_of_tickers = pd.read_csv(psite, sep=',',index_col=0)
    #tickers = list_of_tickers.index
    #print(ticker_name )

    #
    


################################  


    #ticker_name = [ ('ABF','',''), ('GVC','','')]
    
    #print(ticker_name)
    if  (pindex == 'FTSE') :
     ticker_name.remove(['DCG', 'DAIRY CREST', 'FTSE250'])
     ticker_name.remove(['AJB', 'AJ BELL', 'FTSE250'])
     
    elif (pindex == 'NIFTY'):
     print('')
     
    
    for ticker_list in ticker_name :
        
              if   (pindex == 'FTSE') :
                ticker = ticker_list[0].replace('.', '')  + '.L'
              elif (pindex == 'NIFTY') :
                ticker = ticker_list[0].replace('.', '')  + '.NS'
              name = ticker_list[1]
              index = ticker_list[2]
              
              #print(ticker_list)
              print ('###############################################' + 'fetching??' )
              print(ticker_list)
              

######################################### ticker's history data
                  
                  
              if (phistory == 'Y'):      
                  
   
                  if (plocation == 'LSE'):
                    filename = ticker.replace('.L', '_LSE') 
                  elif(plocation == 'NSE'):
                    filename = ticker.replace('.NS', '_NSE')
                  elif(plocation == 'BSE'):
                    filename = ticker.replace('.BO', '_BSE')
                
                  
                  if not os.path.exists("/Users/rajubingi/Equity_Data/Data/" +filename +".csv") :
                      ftse_data_error = []
                      try:
                          history_data = si.get_data(ticker, start_date = START)
                          if len(history_data) >= 1:
                           #print(history_data)
                           df_history_data = pd.DataFrame(history_data.round(2), columns=[ 'open','high','low', 'close', 'adjclose', 'volume','ticker'])
                           df_history_data.to_csv("/Users/rajubingi/Equity_Data/Data/" +filename +".csv")
                           print ( ticker + ' History data has been loaded ')
                    
                          else:
                            print ('Failed to Extract HistoryData: ' + ticker )
                            pcurrentdata = 'N'
                            ftse_data_error.append((END,ticker,'Not abe to extract History Data'))
                            df_error = pd.DataFrame(ftse_data_error, columns=['Transdate','Ticker','Error'])
                            df_error.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_data_Error.csv", index=False, header=False, mode='a')
                      except:
                           print ('#############' + ticker + 'does not exist####################')
                           ftse_data_error.append((END,ticker,'does not exist'))
                           df_error = pd.DataFrame(ftse_data_error, columns=['Transdate','Ticker','Error'])
                           df_error.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_data_Error.csv", index=False, header=False, mode='a')
                           continue           
                  else:
                     print (filename + '  already exist')

 ########################################### ticker's Current data
                     
              if (pcurrentdata == 'Y'):     
 

                
                     with open("/Users/rajubingi/Equity_Data/Data/" +filename +".csv") as inputfile:
                        reader = csv.reader(inputfile)
                        inputm = list(reader)
                        last_DATE = inputm[-1][0]
                        print('LastDate:' + last_DATE)
                      
                    
                                                 
                     print('deleting last  record' )
                     readFile = open("/Users/rajubingi/Equity_Data/Data/" +filename +".csv")
                     lines = readFile.readlines()
                          
                     readFile.close()
                     w = open("/Users/rajubingi/Equity_Data/Data/" +filename +".csv",'w')
                     w.writelines([item for item in lines[:-1]])
                     w.close()
                          
                     
                        
                     START_DATE = datetime.datetime.strptime(last_DATE,'%Y-%m-%d').date()
                     ftse_data_error = []
                     print ("START_DATE: " + START_DATE.strftime('%Y-%m-%d')  )
                     
                     try:
                         
                         current_data = si.get_data(ticker, start_date = START_DATE)
                         current_data =  current_data[~current_data.index.duplicated(keep='first')]
                         
                         #history(ticker, START_DATE, END)
                         #print(current_data)
                         
                         if (len(current_data) >= 1 and START_DATE == current_data.index[0] ) :
                    
                           
                    
                         #print(actual_data)
                           df_current_data = pd.DataFrame(current_data.round(2), columns=['open','high','low', 'close', 'adjclose', 'volume','ticker'])
                           df_current_data.to_csv("/Users/rajubingi/Equity_Data/Data/" +filename +".csv",header=False, mode='a')
                           print ( ticker + ' Daily data has been loaded ')
                         else:
                           
                           prsi == 'N'
                           ftse_data_error.append((END,ticker,'Not able to extract Current Data or Data is not right format'))
                           df_error = pd.DataFrame(ftse_data_error, columns=['Transdate','Ticker','Error'])
                           df_error.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_data_Error.csv", index=False, header=False, mode='a')
                           print ( ticker + ' Daily data failed to load')
                     except:
                           print ('#############' + ticker + '  data not loaded####################')
                           prsi == 'N'
                           ftse_data_error.append((END,ticker,'data not loaded due to exceptions'))
                           df_error = pd.DataFrame(ftse_data_error, columns=['Transdate','Ticker','Error'])
                           df_error.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_data_Error.csv", index=False, header=False, mode='a')
                           continue         
                       
#########################################Statistics
         # ticker's statistics
             
              
             
              if (pstatistics == 'Y'):
                  
                  statistics =  stats(ticker)
                  #print(statistics)
                  stats_new = Get_Stats(ticker,pindex)
                  #try:
                  if len(statistics) > 1:
                        
                           
                          
                           ticker = statistics['ticker']
                           company_name = name
                           #index = pindex
                           sector = statistics['sector']
                           desc = statistics['desc']
                           cname = statistics['desc'][0:50]
                           industry =  statistics['industry']
                           try:
                            beta_val = statistics['Beta (3Y Monthly)']
                           except:
                            beta_val = 'N/A'
                           try:
                            market_cap = statistics['Market Cap']
                           except:
                            market_cap = 'N/A'
                           try:
                            yearly_low = statistics['52 Week Range'].split()[0]
                           except:
                            yearly_low = '0'
                           try:
                            yearly_high = statistics['52 Week Range'].split()[2]
                           except:
                            yearly_high = '0'
                           try:
                            pe_ratio = statistics['PE Ratio (TTM)']
                           except:
                            pe_ratio = 'N/A'
                           try:
                            eps = statistics['EPS (TTM)']
                           except:
                            eps = 'N/A'
                           try:
                            Dividend = statistics['Forward Dividend & Yield']
                           except:
                            Dividend = 'N/A'
                           try:
                            Ex_Dividend_Date_val = statistics['Ex-Dividend Date']
                           except:
                            Ex_Dividend_Date_val = 'N/A'
                           try:
                            Avg_vol= statistics['Avg. Volume']
                           except:
                            Avg_vol = 'N/A'
                           #Earnings_Date= statistics['Earnings Date']
                           ''' if (len(cname.split('plc')[0]) == 50):
                             if(len(cname.split('Plc')[0]) == 50):
                               if(len(cname.split('PLC')[0]) == 50):
                                  if (len(cname.split('p.l.c.')[0]) == 50):
                                     if (len(cname.split('ltd')[0]) == 50):
                                        if (len(cname.split('LTD')[0]) == 50):
                                           if (len(cname.split(',')[0]) == 50):
                                              if(len(cname.split('is')[0]) == 50): 
                                                company_name = cname
                                              
                                              else:
                                                company_name =  cname.split('is')[0] 
                                               
                                           else:
                                              company_name =  cname.split(',')[0] 
                                        else:
                                              company_name =  cname.split('LTD')[0]
                                
                                     else:
                                              company_name = cname.split('ltd')[0]
                                  else:
                                              company_name = cname.split('p.l.c.')[0]
                               else:
                                              company_name = cname.split('PLC')[0]
                             else:
                                              company_name = cname.split('Plc')[0]
                           else:
                               company_name =  cname.split('plc')[0]'''
                          
                           if (pe_ratio == 'N/A'):
                              pe_ratio_num = 0
                           else:
                              pe_ratio_num = float(pe_ratio.replace(",", ""))
                             
                           if (beta_val == 'N/A'):
                              beta = 0
                           else:
                              beta = beta_val
                              
                           if (Ex_Dividend_Date_val == 'N/A'):
                              Ex_Dividend_Date = '1900-01-01'
                           else:
                              Ex_Dividend_Date = Ex_Dividend_Date_val
                           
                           if (Avg_vol == 'N/A'):
                              Avg_volume = 0
                           else:
                              Avg_volume = round(int((Avg_vol.replace(",", "")))/1000000,2)

                           ftse.append((END,ticker,company_name ,index,sector,industry, beta,market_cap , int(float(yearly_low.replace(",", ""))), 
                                              int(float(yearly_high.replace(",", ""))), pe_ratio_num, eps, Dividend,Ex_Dividend_Date,Avg_volume))
                           #print(ftse)
                           ftse_extra.append((END,ticker,desc))
                               
                               
                  else:
                            print ('Failed to Extract statistics: ' + ticker )
                            ftse_error.append((END,ticker,'Not abe to extract statistics'))

                                       
                               
    ################################## Calculate RSI
              if (prsi == 'Y') :   
                         
                             RSI_val = Get_RSI(ticker, plocation)
                             print(RSI_val)
                             if RSI_val is not None and len(RSI_val) > 1 :
                              RSI_Yesterday = round( RSI_val[0],3)
                              RSI = round(RSI_val[1],3)
                             else:
                              RSI = 0
                              RSI_Yesterday = 0
                             
                             buy_sell_MACD_AVG_val = Get_Fiba(ticker, plocation)
                             #if buy_sell_MACD_AVG_val is not  None  : 
                             Buy_Price = round(buy_sell_MACD_AVG_val[0],2)
                             Sell_Price = round(buy_sell_MACD_AVG_val[1],2)
                             MACD = round(buy_sell_MACD_AVG_val[2],3)
                             MACD_Yesterday = round(buy_sell_MACD_AVG_val[3],3)
                             MACD_9 = round(buy_sell_MACD_AVG_val[4],3)
                             MA_20 = round(buy_sell_MACD_AVG_val[5],3)
                             MA20_Yesterday = round(buy_sell_MACD_AVG_val[6],3)
                             #Avg_Vol = round(buy_sell_MACD_AVG_val[7]/1000000,2)
                             
                             
                             open_val = list(current_data.tail(1)['open'])[0]
                             low_val  = list(current_data.tail(1)['low'])[0]
                             high_val = list(current_data.tail(1)['high'])[0]
                             close_val = list(current_data.tail(1)['close'])[0]
                             
                             volume = round(list(current_data.tail(1)['volume'])[0]/1000000, 2)
                             
                             
                             
                             PPSR = Call_PPSR(ticker, plocation)
                             #print(PPSR)
                             
                             
                             if PPSR is not  None and len(PPSR) >= 1 : 
                                 PP = (list(PPSR['PP']))[0]
                                 #ATR_List = (list(PPSR['ATR']))
                                 ATR = (list(PPSR['ATR']))[0] #((ATR_List[0]* 13) + ATR_List[1]) / 14
                                 '''open_val = (list(PPSR['open']))[0]  
                                 low_val = (list(PPSR['low']))[0]
                                 high_val = (list(PPSR['high']))[0]
                                 close_val = (list(PPSR['close']))[0]
                                 volume = round((list(PPSR['volume']))[0]/1000000,2)'''
                                 #print (PPSR, ATR_List, ATR )
                             else :
                                 PP = (list(current_data.tail(2)['high'])[0] + list(current_data.tail(2)['low'])[0] + list(current_data.tail(2)['close'])[0] + open_val)/4
                                 ATR = 0
                             
                             SR = cal_SL_RL(ticker, plocation)
                             
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
                             
                             
                             
                             lastFivedaysdays = last_five_days(ticker, plocation, pindex,END)
                             if (lastFivedaysdays is not  None and len(lastFivedaysdays) >= 1) :
                               #change = list(lastFivedaysdays['change'])[0]
                               print('Last Five `Days have been Processed: '+ ticker )
                               print('')
                             ftse_tech.append((END,ticker,MACD_Yesterday, MACD, MACD_9,MA20_Yesterday, MA_20,RSI_Yesterday, RSI,round(ATR), round(Buy_Price),
                                                 round(Sell_Price),round(PP),round(R1),round(S1),round(R2),round(S2),round(R3),round(S3),
                                                 round(open_val,2),round(low_val,2),round(high_val,2), round(close_val,2), volume  ))
                         
                             
                              
                           #print(float(pe_ratio_num.replace(',', '')))
                           
                               
                             
                            
  
    ##########################################
    if (pstatistics == 'Y'):
        df = pd.DataFrame(ftse, columns=['Transdate','ticker','Company_Name' ,'Index','Sector','Industry', 'Beta','Market_Cap' , '52_week_low', 
                                                      '52_week_high','pe_ratio','eps','Dividend','Ex_Dividend_Date','Avg_volume'])
                    
        df.to_csv("/Users/rajubingi/Equity_Data/" + pindex + ".csv",index=False)
                                       
        df = pd.DataFrame(ftse_extra, columns=['Transdate','ticker','Desc'])
        df.to_csv("/Users/rajubingi/Desktop/" + pindex + "_extra.csv", index=False)
                        
        df_error = pd.DataFrame(ftse_error, columns=['Transdate','Ticker','Error'])
        df_error.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_stats_Error.csv")     
                             
    if (prsi == 'Y') :
        df = pd.DataFrame(ftse_tech, columns=['Transdate','ticker','MACD_Yesterday', 'MACD', 'MACD_9','MA20_Yesterday', 'MA_20','RSI_Yesterday','RSI','ATR','Buy_Value',
                                                  'Sell_Value','PP','R1','S1','R2','S2','R3','S3','open','low','high','close' , 'volume'])                            
        df.to_csv("/Users/rajubingi/Equity_Data/" + pindex + "_tech.csv",index=False)
              
    if(pstatistics == 'Y' ):
        call_avg_pe = cal_avg_pe(pindex)
        if (call_avg_pe == 'Success'):
            print('Average PE Ratio has been Processed: ')
        

    
    if (prsi == 'Y' and pcurrentdata == 'Y') :  
       Merge_Files = merge_files(pindex)
              
    if (Merge_Files == 'Success'):
       print(pindex + ' Files are Merged ')  

  
    
    
    
    return 'Success'

##################################  END  ######################################
    


def Get_Data_New_Current(psite, pindex):
    
    START_DATE = datetime.date.today() - timedelta(days=10)
    
    oversold_fileName = "/Users/rajubingi/Equity_Data/OVERSOLD_" + pindex + ".csv"
    if os.path.exists(oversold_fileName)  :
      os.remove(oversold_fileName)
    
    overbought_fileName = "/Users/rajubingi/Equity_Data/OVERBOUGHT_" + pindex + ".csv"
    if os.path.exists(overbought_fileName)  :
      os.remove(overbought_fileName)
      
    '''df = pd.DataFrame(ftse, columns=['Ticker','Moving_Average_200','Moving_Average_50','week_high_52','week_low_52',
                                       'Avg_vol_10days','Avg_vol_3months','Beta','Dividend_Date', 'Ex_Dividend_Date','Fiscal_Year_Ends',
                                       'Market_Cap','Shares_Outstanding','Total_Debt/Equity','P/E','RSI','Transdate'])
    df.to_csv("/Users/rajubingi/Equity_Data/" + "current_ftse100" + ".csv", index=False)'''
    
    list_of_tickers = pd.read_csv(psite, sep=',',index_col=0)
    tickers = list_of_tickers.index

####################### Retruning oversold tickers
    
    Tickers_RSI = map(Get_RSI_Latest,tickers)
    #print(list(filter(None, (over_sold))))
    
    RSI_Tickers_List = list(filter(None, (Tickers_RSI)))
    
    #print (RSI_Tickers_List)
    #RSI_Tickers_List=[('SMT.L', 36.5084929365015), ('HL.L', 33.98819268299056), ('MRO.L', 60.95269425743856)]
    #print(oversold_tickers)
######################### Returning stats for oversold tickers   
    for ticker in RSI_Tickers_List:
      if ticker[1] <= 50:
        
        stats = Get_Stats_current(ticker)
        stats_df=stats.rename(columns = {'Ticker':'ticker','200-Day Moving Average 3':'Moving_Average_200','50-Day Moving Average 3':'Moving_Average_50'
                                         ,'52 Week High 3':'week_high_52','52 Week Low 3':'week_low_52','Avg Vol (10 day) 3':'Avg_vol_10days'
                                         ,'Avg Vol (3 month) 3':'Avg_vol_3months','Beta (3Y Monthly)':'Beta','Dividend Date 3':'Dividend_Date'
                                         ,'Fiscal Year Ends':'Fiscal_Year_Ends','Market Cap (intraday) 5':'Market_Cap'
                                         ,'Shares Outstanding 5':'Shares_Outstanding','Total Debt/Equity (mrq)':'Total_Debt/Equity'
                                         ,'Trailing P/E':'P/E'})
        #print(stats_df.columns)

####################### Returning  oversold tickers   last 7 days movement     
        last_seven_days = si.get_data(ticker[0], start_date = START_DATE)
        buy_sell_val = Get_Fiba_latest(ticker[0])
        PPSR = Call_PPSR_latest(ticker[0])
        #print(buy_sell_val[0])
        #print(PPSR)
        
        
        buy_price = round(buy_sell_val[0],2)
        sell_price = round(buy_sell_val[1],2)
        
        
        
        #print(PP, R1, S1)
        
        
        last_seven_days_df = (last_seven_days.tail(7).iloc[[0,1,2,3,4,5,6], [3]])
        last_seven_days_df = last_seven_days_df.transpose()
        last_seven_days_df['ticker'] = ticker[0]
        
        last_seven_days_df['change'] = ((last_seven_days_df[last_seven_days_df.columns[6]] 
                                           - last_seven_days_df[last_seven_days_df.columns[5]])
                                             /last_seven_days_df[last_seven_days_df.columns[5]]) * 100
    
        last_seven_days_df['Buy_Price']  = buy_price
        last_seven_days_df['Sell_Price'] = sell_price
        
        if (len(PPSR) >= 1):
            PP = list(PPSR['PP'])
            R1 = list(PPSR['R1'])
            S1 = list(PPSR['S1'])
            R2 = list(PPSR['R2'])
            S2 = list(PPSR['S2'])
            R3 = list(PPSR['R3'])
            S3 = list(PPSR['S3'])
            last_seven_days_df['PP']  = PP[0]
            last_seven_days_df['R1']  = R1[0]
            last_seven_days_df['S1']  = S1[0]
            last_seven_days_df['R2']  = R2[0]
            last_seven_days_df['S2']  = S2[0]
            last_seven_days_df['R3']  = R3[0]
            last_seven_days_df['S3']  = S3[0]
        else:
            last_seven_days_df['PP']  = 0
            last_seven_days_df['R1']  = 0
            last_seven_days_df['S1']  = 0
            last_seven_days_df['R2']  = 0
            last_seven_days_df['S2']  = 0
            last_seven_days_df['R3']  = 0
            last_seven_days_df['S3']  = 0
        
        
        #print(last_seven_days_df.columns)
        
####################### Merging above  Dataframes and files
        
        #df1 =  pd.merge(stats_df, last_seven_days_df, on='Ticker')
        df1 = pd.read_csv("/Users/rajubingi/Equity_Data/" + pindex + ".csv", index_col=[0]
                               , usecols=["ticker","Comapany_Name","Index","Sector","Industry","Dividend"])
        
        df2 = pd.read_csv("/Users/rajubingi/Equity_Data/" + pindex + "_AvgPE.csv", sep=',')
        df3 =  pd.merge(df1, stats_df, on='ticker')
        df4 =  pd.merge(df3, last_seven_days_df, on='ticker')
        df5 =  pd.merge(df4, df2, on='Industry')
        
        
####################### Merging files
        
        
        
        
        df_final = pd.DataFrame(df5, columns=df5.columns)
        if not os.path.exists(oversold_fileName) : 
           df_final.to_csv(oversold_fileName, index=False, mode='a')
        else:
           df_final.to_csv(oversold_fileName,index=False, header=False,  mode='a')
           
############################ over bought
      elif ticker[1] > 60:
          
        

####################### Returning  oversold tickers   last 7 days movement     
        last_seven_days = si.get_data(ticker[0], start_date = START_DATE)
        buy_sell_val = Get_Fiba_latest(ticker[0])
        PPSR = Call_PPSR_latest(ticker[0])
        #print(PPSR)
        buy_price = round(buy_sell_val[0],2)
        sell_price = round(buy_sell_val[1],2)
        
        
        
        last_seven_days_df = (last_seven_days.tail(7).iloc[[0,1,2,3,4,5,6], [3]])
        last_seven_days_df = last_seven_days_df.transpose()
        last_seven_days_df['ticker']=ticker[0]
        last_seven_days_df['change'] = ((last_seven_days_df[last_seven_days_df.columns[6]] 
                                           - last_seven_days_df[last_seven_days_df.columns[5]])
                                             /last_seven_days_df[last_seven_days_df.columns[5]]) * 100
            
        last_seven_days_df['Buy_Price']  = buy_price
        last_seven_days_df['Sell_Price'] = sell_price
        
        if ( len(PPSR) >= 1):
            PP = list(PPSR['PP'])
            R1 = list(PPSR['R1'])
            S1 = list(PPSR['S1'])
            R2 = list(PPSR['R2'])
            S2 = list(PPSR['S2'])
            R3 = list(PPSR['R3'])
            S3 = list(PPSR['S3'])
            last_seven_days_df['PP']  = PP
            last_seven_days_df['R1']  = R1
            last_seven_days_df['S1']  = S1
            last_seven_days_df['R2']  = R2
            last_seven_days_df['S2']  = S2
            last_seven_days_df['R3']  = R3
            last_seven_days_df['S3']  = S3
        else:
            last_seven_days_df['PP']  = 0
            last_seven_days_df['R1']  = 0
            last_seven_days_df['S1']  = 0
            last_seven_days_df['R2']  = 0
            last_seven_days_df['S2']  = 0
            last_seven_days_df['R3']  = 0
            last_seven_days_df['S3']  = 0
        
        last_seven_days_df['RSI']=ticker[1]
        #print(last_seven_days_df)
        
####################### Merging above  Dataframes and files
        
        #df1 =  pd.merge(stats_df, last_seven_days_df, on='Ticker')
        df1 = pd.read_csv("/Users/rajubingi/Equity_Data/" + pindex + ".csv", index_col=[0]
                               , usecols=["ticker","Comapany_Name","Index","Sector","Industry","Dividend"])
        
    
        df3 =  pd.merge(df1, last_seven_days_df, on='ticker')
       
        
        
####################### Merging files
        
        
        
        
        df_final = pd.DataFrame(df3, columns=df3.columns)
        if not os.path.exists(overbought_fileName) : 
           df_final.to_csv(overbought_fileName, index=False, mode='a')
        else:
           df_final.to_csv(overbought_fileName,index=False, header=False,  mode='a')
        
        
    
    return 'Success' 
   
   
   