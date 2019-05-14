#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 14 19:37:46 2019

@author: rajubingi
"""

import csv
import psycopg2
import datetime

def Load_Data_Database(pdetails,pstats, ptech,pindex):
    conn = psycopg2.connect("host=localhost dbname=equity user=rajubingi")
    transdate = datetime.date.today()
    
    '''if (pindex == 'FTSE'):
      Tech_table = "INSERT INTO " + pindex + "_tech VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s,%s, %s, %s, %s, %s)"
      Del_Sql = "DELETE FROM "+ pindex +"_tech WHERE transdate = %s"
      Trunc_sql = "TRUNCATE "+ pindex +"_details"
      Details_table = "INSERT INTO "+ pindex +"_details VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s)"
      
    elif (pindex == 'NIFTY'):
      Tech_table = "INSERT INTO nifty_tech VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s,%s, %s, %s, %s, %s)"
      Del_Sql = "DELETE FROM nifty_tech WHERE transdate = %s"
      Trunc_sql = "TRUNCATE nifty_details"
      Details_table = "INSERT INTO nifty_details VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s)"  '''
    cur = conn.cursor()
    if (ptech == 'Y'):
        with open("/Users/rajubingi/Equity_Data/"+pindex+"_tech.csv", 'r') as f:
            reader = csv.reader(f)
            print(reader)
            next(reader)  # Skip the header row.
            cur.execute("DELETE FROM "+ pindex +"_tech WHERE transdate = %s", (transdate,))
            conn.commit()
            
            for row in reader:
                    print(row)
                    cur.execute(
                    "INSERT INTO " + pindex + "_tech VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s,%s, %s, %s, %s, %s, %s)",
                                          
                        row
                    )
        conn.commit()
        
        with open("/Users/rajubingi/Equity_Data/"+pindex+"_Last5days.csv", 'r') as f:
            reader = csv.reader(f)
            #print(reader)
            next(reader)  # Skip the header row.
            cur.execute("DELETE FROM "+ pindex +"_last7days")
            conn.commit()
            
            for row in reader:
                    #print(row)
                    cur.execute(
                    "INSERT INTO " + pindex + "_last7days VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s)",
                                          
                        row
                    )
        conn.commit()        
        
        
    if (pdetails == 'Y'):
        with open("/Users/rajubingi/Equity_Data/"+ pindex + "_details.csv", 'r') as f:
            reader = csv.reader(f)
            print(reader)
            next(reader)  # Skip the header row.
            cur.execute("TRUNCATE "+ pindex +"_details")
            conn.commit()
            rows_deleted = cur.rowcount
            print(rows_deleted)
            #cur.execute("TRUNCATE ftse_details" )
            
            for row in reader:
                #print(row)
                cur.execute(
                "INSERT INTO "+ pindex +"_details VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s)",
                                      
                    row
                )
        conn.commit()
    return 'Data loaded into Tables'