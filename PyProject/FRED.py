#================================================================
#VERSION
#================================================================
#00.00			Original basic download with tweaking and working proof of concept.
#================================================================

import datetime
import pandas as pd
import pyodbc
import time

from datetime import datetime, timedelta
from pandas_datareader import data as pdr
from datetime import date

from fredapi import Fred

#Algo from: https://seekingalpha.com/article/4396238-stock-gold-switch-signals-from-yield-curve-and-federal-funds-rate

#================================================================
#Open API connection with FRED
#================================================================

fred = Fred(api_key='e15032e4ab0447f9cc0324db6416678d')

# print(dataEFFR)
# input('press key to continue')
# print(data02YR)
# input('press key to continue')
# print(data10YR)
# input('press key to continue')


#================================================================
#Define variables
#================================================================
#date = datetime.date.today()
year = str(datetime.now().year)
month = str(datetime.now().month)
day = str(datetime.now().day)
timestamp = time.strftime('%H%M%S')
today = date.today()

while len(month) < 2:
	month = '0' + month

while len(day) < 2:
	day = '0' + day

#===============================================
#Error log file
#===============================================

logfilelocation = r'C:\Users\brett\OneDrive\Documents\Excel\Trading\PyProject\ErrorReports\\'
print('Logfilelocation: ' + logfilelocation)

logfilename = str(year + month + day +  '_' + timestamp + '_FRED_ErrorReport.txt')
print('LogFileName: ' + logfilename)

logfile = logfilelocation + logfilename
print('LogFile: ' + logfile)

ErrorLog = '=================================================================================================\r\n' + 'JOB FAILURES:\r\n'  + '=================================================================================================\r\n'

f = open(logfile,'a+')
f.write(ErrorLog)

#===============================================
#New code configured from: https://reasonabledeviations.com/2018/02/01/stock-price-database/#database-schema
#===============================================

conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='LAPTOP-D6TKOBQR\SQLEXPRESS',DATABASE='stockdb',Trusted_connection='yes')
crsr = conn.cursor()

#dataEFFR = fred.get_series('EFFR')
#data02YR = fred.get_series('DGS2')
#data10YR = fred.get_series('DGS10')

ticker_list = ['EFFR','DGS2','DGS10']

def getData(ticker):
	print(ticker)
	try:
		data = fred.get_series(ticker)
		print(data)

		for row in data.itertuples():
			print(value[row[:2]])
			# values = list(row)
			# values.append(ticker)
			# newval = str(values[0])
			# newval = newval.replace(' 00:00:00','')
			# newval = newval.replace('-','')
			# newval = (values[7] + '_' + newval)
			# values.append(newval)
			# try:
				# crsr.execute("""INSERT INTO snp500_test (dtDate,decOpen,decHigh,decLow,decClose,decAdjClose,intVol,strTick,ID)
				# VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
				# tuple(values))
				# conn.commit()
			# except Exception as e:
				# try: 
					# miscerr = miscerr + 'error: ' + str(newval) + str(e) + '\r\n'
				# except:
					# miscerr = 'error: ' + str(newval) + str(e) + '\r\n'
		#===============================================
	except Exception as e:
		ErrorLog = """'""" + str(ticker) + """'""" + ','
		f.write(ErrorLog)
		print(str(e))

for tik in ticker_list:
	timestamp = time.strftime('%H%M%S')
	print(tik + ' started at ' + str(timestamp))
	getData(tik)
	timestamp = time.strftime('%H%M%S')
	print(tik + ' completed at ' + str(timestamp))
	

ErrorLog = '\r\n'  + '================================================================================================='

f.write(ErrorLog)
try:
	f.write(miscerr)
except:
	print('No misc errors')
f.close()
print("""Job's done""")