

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from MSQL_Connection import ETL,get_latestDate
import Config as Cnf
from datetime import datetime,timedelta

#region Variables
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = Cnf.KeyFilePath
VIEW_ID = Cnf.View_Id

Country=[]
Region=[]
Device_Categ=[]
Page_Path=[]
Date=[]
Users=[]
Sessions=[]
Page_V=[]
U_Page_V=[]
Avg_Sess=[]
Sess_Dur=[]
Time_Page=[]
#endregion


def initialize_analyticsreporting():

  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics



def get_report(analytics,date):
  print(date)
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': date, 'endDate': 'Yesterday'}],
          'metrics': [
              {
                "expression": 'ga:users'
              },
			  {
                "expression": 'ga:sessions',
				"alias":"sessions"
              },
			  {
                "expression": 'ga:pageviews'
              },
			  {
                "expression": 'ga:uniquePageviews'
              },
			  {
                "expression": 'ga:avgTimeOnPage',
				"alias":"Avg. Session Duration"
              },
			  {
                "expression": 'ga:sessionDuration'
              },
			  {
                "expression": 'ga:timeOnPage',
				"alias":"TimeOnPage"
              }

              ],
          'dimensions': [{'name': 'ga:country'},
			  {
				"name":"ga:region"
			  },
			  {
				"name":"ga:deviceCategory"
			  },
			  {
				"name":"ga:pagePath"
			  },
              {
				"name":"ga:date"
			  }




              ]
        }]
      }
  ).execute()


def print_response(response):

  #print("In print response func")
  for report in response.get('reports', []):

    for row in report.get('data', {}).get('rows', []):


      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      Country.append(dimensions[0])
      Region.append(dimensions[1])
      Device_Categ.append(dimensions[2])
      Page_Path.append(dimensions[3])
      Date.append(dimensions[4])

      for i, values in enumerate(dateRangeValues):
        Users.append(values.get('values')[0])
        Sessions.append(values.get('values')[1])
        Page_V.append(values.get('values')[2])
        U_Page_V.append(values.get('values')[3])
        Avg_Sess.append(values.get('values')[4])
        Sess_Dur.append(values.get('values')[5])
        Time_Page.append(values.get('values')[6])

    DataframeLoad();

def DataframeLoad():
    col_names =  ['Country']
    DF_GA=pd.DataFrame(columns=col_names)

    DF_GA.loc[:,'Country']=Country
    DF_GA.loc[:,'Region']=Region
    DF_GA.loc[:,'Device']=Device_Categ
    DF_GA.loc[:,'PagePath']=Page_Path
    DF_GA.loc[:,'Date']=Date
    DF_GA.loc[:,'Users']=Users
    DF_GA.loc[:,'Sessions']=Sessions
    DF_GA.loc[:,'PageViews']=Page_V
    DF_GA.loc[:,'UPageViews']=U_Page_V
    DF_GA.loc[:,'AvgSessDur']=Avg_Sess
    DF_GA.loc[:,'SessDur']=Sess_Dur
    DF_GA.loc[:,'TimeOnPage']=Time_Page
    #DF_GA['TillDate']=(datetime.now()- timedelta(1)).strftime('%Y-%m-%d')
    LoadMSSQL(DF_GA)

def LoadMSSQL(df):
    ETL(df)



def main():
  date=get_latestDate()
  for row in date:
      res=row
  analytics = initialize_analyticsreporting()
  response = get_report(analytics,res)
  print_response(response)

if __name__ == '__main__':

  main()
