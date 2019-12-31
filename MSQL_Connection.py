# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 14:57:41 2019

@author: Talha.Iftikhar
"""

#import pyodbc
#import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from six.moves import urllib
import Config as Cfg
import pandas as pd

Server= Cfg.Server
Database=Cfg.Database
username=Cfg.Username
password=Cfg.Password
Table=Cfg.Table


def get_latestDate():
    connect=mssql_con()
    LatestDate=connect.execute("select isnull(CONVERT(char(10),max(date),126),'2019-01-01') from [dbo].[temp_GA_PagesDetails]")
    for row in LatestDate:
        return row


def mssql_con():
    params = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER="+Server+";DATABASE="+Database+";UID="+username+";PWD="+password)
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    engine.connect()
    return engine

def ETL(Dataframe):
    connect=mssql_con()
    Dataframe.to_sql(Table, con=connect, if_exists='append',index=0)
    #with connect.begin() as conn:
       #  conn.execute("PushGAData")

    print('Success!!!')
    get_latestDate()

# def printdf(df):
#     print(df.head())


# df1 = pd.DataFrame({'name' : ['User 4', 'User 5']})

# printdf(df1)
