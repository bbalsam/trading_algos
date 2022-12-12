#================================================================
#VERSION
#================================================================
#00.00			Original basic download with tweaking and working proof of concept.
#00.01			Testing to upload data to database.
#00.02			Upload to database confirmed. Added log file for error reporting, and error bypass.
#================================================================

import datetime
import yfinance as yf
import pandas as pd
import pyodbc
import time

from datetime import datetime, timedelta
from pandas_datareader import data as pdr
from datetime import date



#================================================================
#Code to allow Yahoo! to recognize API call
#================================================================
yf.pdr_override()


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

logfilelocation = r'.\ErrorReports\\'
print('Logfilelocation: ' + logfilelocation)

logfilename = str(year + month + day +  '_' + timestamp + '_ErrorReport.txt')
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

# Tickers list
# We can add and delete any ticker from the list to get desired ticker live data
#'ADI','ADP','ATO'
#ticker_list=['GLD','SLV','AAL','AAPL','ACN','ADBE','ADS','ADSK','AEP','AES','AIV','AKAM','ALK','ALLE','AMAT','AMCR','AMD','AME','AMT','ANET','ANSS','AOS','APD','APH','ARE','ATVI','AVB','AVGO','AVY','AWK','BA','BLL','BR','BXP','CARR','CAT','CBRE','CCI','CDNS','CDW','CE','CF','CHRW','CHTR','CMCSA','CMI','CMS','CNP','CPRT','CRM','CSCO','CSX','CTAS','CTL','CTSH','CTVA','CTXS','D','DAL','DD','DE','DIS','DISCA','DISCK','DISH','DLR','DOV','DOW','DRE','DTE','DUK','DXC','EA','ECL','ED','EFX','EIX','EMN','EMR','EQIX','EQR','ES','ESS','ETN','ETR','EVRG','EXC','EXPD','EXR','FAST','FB','FBHS','FCX','FDX','FE','FFIV','FIS','FISV','FLIR','FLS','FLT','FMC','FOX','FOXA','FRT','FTNT','FTV','GD','GE','GLW','GOOG','GOOGL','GPN','GWW','HII','HON','HPE','HPQ','HST','HWM','IBM','IEX','IFF','INFO','INTC','INTU','IP','IPG','IPGP','IR','IRM','IT','ITW','J','JBHT','JCI','JKHY','JNPR','KEYS','KIM','KLAC','KSU','LDOS','LHX','LIN','LMT','LNT',
#ERROR LIST: ADS,AIV,AMAT,AOS,CAT,CTVA,DRE,DUK,ETR,HON,IP,LNT,,,,,,,,,
ticker_list=['GLD','SLV','AAL','AAPL','ACN','ADBE','ADS','ADSK','AEP','AES','AIV','AKAM','ALK','ALLE','AMAT','AMCR','AMD','AME','AMT','ANET','ANSS','AOS','APD','APH','ARE','ATVI','AVB','AVGO','AVY','AWK','BA','BLL','BR','BXP','CARR','CAT','CBRE','CCI','CDNS','CDW','CE','CF','CHRW','CHTR','CMCSA','CMI','CMS','CNP','CPRT','CRM','CSCO','CSX','CTAS','CTL','CTSH','CTVA','CTXS','D','DAL','DD','DE','DIS','DISCA','DISCK','DISH','DLR','DOV','DOW','DRE','DTE','DUK','DXC','EA','ECL','ED','EFX','EIX','EMN','EMR','EQIX','EQR','ES','ESS','ETN','ETR','EVRG','EXC','EXPD','EXR','FAST','FB','FBHS','FCX','FDX','FE','FFIV','FIS','FISV','FLIR','FLS','FLT','FMC','FOX','FOXA','FRT','FTNT','FTV','GD','GE','GLW','GOOG','GOOGL','GPN','GWW','HII','HON','HPE','HPQ','HST','HWM','IBM','IEX','IFF','INFO','INTC','INTU','IP','IPG','IPGP','IR','IRM','IT','ITW','J','JBHT','JCI','JKHY','JNPR','KEYS','KIM','KLAC','KSU','LDOS','LHX','LIN','LMT','LNT','LRCX','LUV','LYB','LYV','MA','MAA','MAS','MCHP','MLM','MMM','MOS','MSFT','MSI','MU','MXIM','NEE','NEM','NFLX','NI','NLOK','NLSN','NOC','NOW','NRG','NSC','NTAP','NUE','NVDA','NWS','NWSA','O','ODFL','OMC','ORCL','OTIS','PAYC','PAYX','PCAR','PEAK','PEG','PH','PKG','PLD','PNR','PNW','PPG','PPL','PSA','PWR','PYPL','QCOM','QRVO','REG','RHI','ROK','ROL','ROP','RSG','RTX','SBAC','SEE','SHW','SLG','SNA','SNPS','SO','SPG','SRE','STX','SWK','SWKS','T','TDG','TEL','TMUS','TT','TTWO','TWTR','TXN','TXT','UAL','UDR','UNP','UPS','URI','V','VIAC','VMC','VNO','VRSK','VRSN','VTR','VZ','WAB','WDC','WEC','WELL','WM','WRK','WU','WY','XEL','XLNX','XRX','XYL','ZBRA','A','AAP','ABBV','ABC','ABMD','ABT','ADM','AEE','AFL','AIG','AIZ','AJG','ALB','ALGN','ALL','ALXN','AMGN','AMP','AMZN','ANTM','AON','APA','APTV','AXP','AZO','BAC','BAX','BBY','BDX','BEN','BF.B','BIIB','BK','BKNG','BKR','BLK','BMY','BRK.B','BSX','BWA','C','CAG','CAH','CB','CBOE','CCL','CERN','CFG','CHD','CI','CINF','CL','CLX','CMA','CME','CMG','CNC','COF','COG','COO','COP','COST','COTY','CPB','CVS','CVX','CXO','DFS','DG','DGX','DHI','DHR','DLTR','DPZ','DRI','DVA','DVN','DXCM','EBAY','EL','EOG','ETFC','EW','EXPE','F','FANG','FITB','FRC','FTI','GILD','GIS','GL','GM','GPC','GPS','GRMN','GS','HAL','HAS','HBAN','HBI','HCA','HD','HES','HFC','HIG','HLT','HOG','HOLX','HRB','HRL','HSIC','HSY','HUM','ICE','IDXX','ILMN','INCY','IQV','ISRG','IVZ','JNJ','JPM','JWN','K','KEY','KHC','KMB','KMI','KMX','KO','KR','KSS','L','LB','LEG','LEN','LH','LKQ','LLY','LNC','LOW','LVS','LW','MAR','MCD','MCK','MCO','MDLZ','MDT','MET','MGM','MHK','MKC','MKTX','MMC','MNST','MO','MPC','MRK','MRO','MS','MSCI','MTB','MTD','MYL','NBL','NCLH','NDAQ','NKE','NOV','NTRS','NVR','NWL','OKE','ORLY','OXY','PBCT','PEP','PFE','PFG','PG','PGR','PHM','PKI','PM','PNC','PRGO','PRU','PSX','PVH','PXD','RCL','RE','REGN','RF','RJF','RL','RMD','ROST','SBUX','SCHW','SIVB','SJM','SLB','SPGI','STE','STT','STZ','SYF','SYK','SYY','TAP','TFC','TFX','TGT','TIF','TJX','TMO','TPR','TROW','TRV','TSCO','TSN','UA','UAA','UHS','ULTA','UNH','UNM','USB','VAR','VFC','VLO','VRTX','WAT','WBA','WFC','WHR','WLTW','WMB','WMT','WRB','WST','WYNN','XOM','XRAY','YUM','ZBH','ZION','ZTS']

#ticker_list=['AAPL','AMZN','GOOG','SLV','TSLA','GLD']

# We can get data by our choice by giving days bracket
#start_date = datetime.datetime.strptime('2020–01–01','%Y-%m-%d')
#end_date= datetime.date(2020, 6, 26)#'2020–06–26'
#===============================================
#commented below for testing
#files=[]
#commented above for testing
#===============================================


def getData(ticker):
	#print(ticker)
	try:
		data = pdr.get_data_yahoo(ticker, start=today-timedelta(10080), end=today)
		#===============================================
		#commented below for testing
			#dataname= ticker+'_'+str(today)
			#files.append(dataname)
			#SaveData(data, dataname)
		#commented above for testing
		#===============================================
		# Create a data folder in your current dir.
		#===============================================
		#commented below for testing
		# def SaveData(df, filename):
			# df.to_csv('.\SANDPDownload\\'+filename+'.csv')
		#commented above for testing
		#===============================================
		#This loop will iterate over ticker list, will pass one ticker to get data, and save that data as file.
		#print(data)
		#===============================================
		#New code configured from: https://reasonabledeviations.com/2018/02/01/stock-price-database/#database-schema
		#===============================================
		for row in data.itertuples():
			values = list(row)
			values.append(ticker)
			#print(values)
			crsr.execute("""INSERT INTO snp500_test (dtDate,decOpen,decHigh,decLow,decClose,decAdjClose,intVol,strTick)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
			tuple(values))
			conn.commit()
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
f.close()
print("""Job's done""")
