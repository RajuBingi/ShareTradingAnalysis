#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 30 21:09:42 2019

@author: rajubingi
"""



from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime
import pandas as pd
from pyspark.sql.types import IntegerType
import os.path
#import org.apache.spark.sql.types.IntegerType as IntegerType


#if os.path.exists("/Users/rajubingi/Desktop/FTSE_LowValueShares.csv")  :      
 #  os.remove('/Users/rajubingi/Desktop/FTSE_LowValueShares.csv')

def SQL(filename,pindex ):
    spark = SparkSession \
           .builder    \
           .appName(" SQL ") \
           .config ("spark", "some value") \
           .getOrCreate()
           
    
    
    df = spark.read.format("csv").option("header", "true").load(filename)
    
    #df.show()
    
    now = datetime.datetime.now()
    date_string = now.strftime('%Y-%m-%d')
    print(date_string)
    
    df = df.withColumnRenamed(date_string , 'close')
    
    
    
    #df1 = df.withColumn('low_value', date)
    #df.show()
    #df2 = df.withColumn("pe_ratio1", df.pe_ratio.cast(IntegerType))
    
    
    df.createOrReplaceTempView("FTSE")
    
    df1 = spark.sql( "select *, round(((close - 52_week_low)/close) * 100) as diff_close_52low,                  \
                                round(((52_week_high - close )/close) * 100) as diff_close_52high,                \
                                round(((((close - Buy_Value)) /close)*100),2) as diff_buyvalue,                    \
                                round(((((close - Sell_Value)) /close)*100),2) as diff_sellval from FTSE            \
                        where pe_ratio > 1 and round(close) >= MA_20 and MA_20 >= MA20_Yesterday                     \
                                and RSI >= 45       \
                             and ( ((close - 52_week_low)/close) * 100 <= 15  )                                         \
                             --and MACD  > MACD_Yesterday and MACD_9 > MACD_Yesterday and MACD > MACD_9                  \
                             -- and (((((close - Buy_Value)) /close)*100) <= 0.75 or  ((((close - Sell_Value)) /close)*100) <= -3  )    \
                            --  and MACD >= 0 and MACD_9 >= 0 \
                            "                   
                    )
    
    df1.repartition(1).write.option("header", "true").csv("/Users/rajubingi/Desktop/" + pindex + "_filtered.csv")
    
    #df1.toPandas().to_csv("/Users/rajubingi/Desktop/FTSE_LowValueShares.csv", header=True)
    #df1.repartition(1).write.csv("/Users/rajubingi/Desktop/FTSE_LowValueShares.csv", sep=',')
    #df1.write.csv("/Users/rajubingi/Desktop/FTSE_LowValueShares.csv")
    #df1.write.format("csv").save("/Users/rajubingi/Desktop/FTSE_LowValueShares.csv")
    #df1.to_csv("/Users/rajubingi/Desktop/FTSE_LowValueShares.csv",index=False)
    
    
    #df1.toPandas().to_csv('/Users/rajubingi/Desktop/FTSE_LowValueShares.csv')
    
    '''df1.select("ticker","Company_Name") \
      .write \
      .save('/Users/rajubingi/Desktop/FTSE_LowValueShares.csv', format="csv")'''
    
    '''df1.coalesce(1) \
       .write.format("com.databricks.spark.csv") \
       .option("header", "true")\
       .save("/Users/rajubingi/Desktop/FTSE_LowValueShares.csv")'''
    
    
    
    '''
    df2 = spark.sql( "select ticker,Company_Name,RSI,change from FTSE where round(RSI) >= 45 and round(RSI) <= 68" )
    
    df2.show()
    
    df3 = spark.sql( "select * from \
                       (select * from FTSE \
                         where  pe_ratio > 1 and MACD > 0 and MACD_9 >0 \
                        ) T  where MACD_Yesterday < MACD_9 \
                            and MACD_Yesterday < MACD \
                            and MACD_9 < MACD \
                            and MA_20 < close "
                             
                    )
    
    
    df3.show()
    
    
    #df.select("ticker","Comapany_Name", F.when(df.change < 0,1).otherwise(0)).show()
    #df.filter(df["change"] > 2).show()
    
    #df.collect()
    
    
    #countsByAge = df2.groupBy('Industry').avg('pe_ratio')
    
    #countsByAge.show()
    
    #avg = df.groupby('Industry')['pe_ratio'].mean()
    
    '''
    return 'success'
