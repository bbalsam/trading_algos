#================================================================
#VERSION
#================================================================
#00.00			Original basic download with tweaking and working proof of concept.
#00.01			Testing to upload data to database.
#00.02			Upload to database confirmed. Added log file for error reporting, and error bypass.
#00.03			Create key handling for SQL upload.
#00.04			Fix download bug.
#00.05			Previous long time prod, slight differentces to version 00.04
#FORKED			00.06			Add time specific execution.
#00.07			00.06 changes removed.  Version 00.07 added current holdings and watch list stocks.
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

logfilelocation = r'C:\Users\brett\OneDrive\Documents\Excel\Trading\PyProject\ErrorReports\\'
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


ticker_list=['^GSPC','^TNX','A','AAL','AAP','AAPL','ABBV','ABC','ABMD','ABNB','ABT','ACB','ACCD','ACES','ACIC','ACN','ADBE','ADI','ADM','ADP','ADS','ADSK','AEE','AEP','AES','AFL','AIG','AITX','AIV','AIZ','AJG','AKAM','ALB','ALGM','ALGN','ALK','ALL','ALLE','ALXN','AMAT','AMC','AMCR','AMD','AME','AMGN','AMP','AMT','AMZN','ANET','ANSS','ANTM','AON','AONE','AOS','APA','APD','APH','APHA','APTV','ARE','ARKF','ARKG','ARKK','ARKQ','ARKW','ARRY','ASAN','ASLN','ATO','ATOS','ATVI','AVB','AVGO','AVO','AVY','AWK','AXP','AZO','BA','BAC','BAX','BB','BBBY','BBKCF','BBY','BDX','BEAM','BEKE','BEN','BF.B','BIIB','BK','BKNG','BKR','BLI','BLK','BLL','BMY','BNGO','BR','BRK.A','BRK.B','BSX','BWA','BXP','BYND','C','CAG','CAH','CARR','CAT','CB','CBOE','CBRE','CCI','CCIV','CCL','CDNS','CDW','CE','CERN','CF','CFG','CGNT','CHD','CHRW','CHTR','CI','CINF','CL','CLOV','CLX','CMA','CMCSA','CME','CMG','CMI','CMS','CNC','CNP','COF','COG','COIN','COO','COP','COST','COTY','CPB','CPNG','CPRT','CRDF','CRM','CRSP','CRSR','CRWD','CSCO','CSX','CTAS','CTL','CTSH','CTVA','CTXS','CVS','CVX','CXO','D','DAL','DASH','DBTX','DD','DDD','DDS','DE','DFS','DG','DGX','DHI','DHR','DIS','DISCA','DISCK','DISH','DLR','DLTR','DMTK','DOV','DOW','DPZ','DRE','DRI','DTE','DUK','DVA','DVN','DXC','DXCM','EA','EADSF','EAR','EBAY','ECL','ED','EFX','EIX','EL','EMN','EMR','ENPH','EOG','EQIX','EQR','ES','ESLT','ESS','ETFC','ETN','ETR','ETSY','EVRG','EW','EXC','EXPD','EXPE','EXPR','EXR','F','FANG','FAST','FATE','FB','FBHS','FCEL','FCX','FDX','FE','FFIV','FIS','FISV','FITB','FLIR','FLS','FLT','FMC','FNMA','FOLD','FOX','FOXA','FRC','FRT','FSLY','FTI','FTNT','FTV','FUV','GBR','GD','GE','GHVI','GILD','GIS','GL','GLD','GLW','GM','GME','GOOG','GOOGL','GOTU','GPC','GPN','GPRO','GPS','GRMN','GS','GWW','HAL','HAS','HBAN','HBI','HCA','HD','HES','HFC','HG=F','HIG','HII','HLT','HOG','HOLX','HON','HPE','HPQ','HRB','HRL','HSIC','HST','HSY','HUM','HUTMF','HWM','IBIO','IBM','ICE','ICLN','IDXX','IEX','IFF','ILMN','INCY','INFO','INNV','INTC','INTU','IP','IPG','IPGP','IQV','IR','IRM','ISRG','IT','ITW','IVZ','J','JBHT','JCI','JKHY','JMIA','JNJ','JNPR','JPM','JWN','K','KEY','KEYS','KHC','KIM','KLAC','KMB','KMI','KMX','KO','KOSS','KR','KSS','KSU','KTOS','L','LAZR','LB','LDOS','LEG','LEN','LH','LHX','LIN','LKQ','LLY','LMND','LMT','LNC','LNT','LOW','LPRO','LRCX','LSF','LULU','LUV','LVS','LW','LYB','LYV','MA','MAA','MAR','MARA','MAS','MASS','MCD','MCHP','MCK','MCO','MCOA','MDLZ','MDT','MET','MGM','MHK','MJ','MKC','MKTX','MLM','MMC','MMM','MNMD','MNST','MO','MOS','MPC','MRK','MRO','MS','MSCI','MSFT','MSI','MSTR','MT','MTB','MTD','MU','MVIS','MXIM','MYL','NAKD','NBL','NCLH','NDAQ','NEE','NEM','NFLX','NI','NIO','NKE','NKLA','NLOK','NLSN','NNDM','NOC','NOV','NOW','NRG','NSC','NTAP','NTRS','NUE','NVDA','NVR','NVS','NWL','NWS','NWSA','O','OCGN','ODFL','OKE','OMC','ONEM','OPEN','ORCL','ORLY','OTIS','OXY','PATH','PAYC','PAYX','PBCT','PCAR','PEAK','PEG','PEP','PFE','PFG','PG','PGR','PH','PHM','PKG','PKI','PLD','PLL','PLTR','PM','PNC','PNR','PNW','PPG','PPL','PRGO','PRU','PSA','PSX','PTON','PVH','PWR','PXD','PYPL','QCOM','QQQ','QRVO','RAVN','RBLX','RCL','RE','REG','REGN','RF','RHI','RIOT','RJF','RL','RMD','ROK','ROKU','ROL','ROP','ROST','RPTX','RSG','RTX','SAFRF','SBAC','SBUX','SCHW','SEDG','SEE','SEER','SENS','SHW','SIVB','SJM','SLB','SLG','SLV','SLVDF','SLX','SNA','SNES','SNOW','SNPS','SNPW','SO','SOFI','SONY','SPCE','SPG','SPGI','SPXL','SPY','SQ','SQQQ','SRE','STE','STIC','STT','STX','STZ','SURF','SWK','SWKS','SYF','SYK','SYY','T','TAP','TDG','TEL','TER','TFC','TFX','TGT','TIF','TJX','TLRY','TMO','TMUS','TPR','TQQQ','TROW','TRV','TSCO','TSLA','TSM','TSN','TT','TTWO','TWLO','TWTR','TXN','TXT','U','UA','UAA','UAL','UDR','UHS','ULTA','UNH','UNM','UNP','UPS','URI','USB','USMJ','V','VAR','VFC','VIAC','VLDR','VLO','VMC','VNO','VRSK','VRSN','VRTX','VTR','VZ','WAB','WAT','WBA','WDC','WEC','WELL','WFC','WHR','WISH','WKHS','WLTW','WM','WMB','WMT','WOOD','WRB','WRK','WST','WU','WY','WYNN','XEL','XLB','XLC','XLE','XLF','XLI','XLK','XLNX','XLP','XLRE','XLU','XLV','XLY','XOM','XRAY','XRX','XYL','YUM','ZBH','ZBRA','ZION','ZM','ZOM','ZTS']

ticker_list = ['ESPR','FRX','FSR','GEO','GOGO','PUBM','SKLZ','SNDL']

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
		data = pdr.get_data_yahoo(ticker, start=today-timedelta(18000), end=today) #18000 in range
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
			newval = str(values[0])
			newval = newval.replace(' 00:00:00','')
			newval = newval.replace('-','')
			newval = (values[7] + '_' + newval)
			values.append(newval)
			try:
				crsr.execute("""INSERT INTO snp500_test (dtDate,decOpen,decHigh,decLow,decClose,decAdjClose,intVol,strTick,ID)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
				tuple(values))
				conn.commit()
			except Exception as e:
				try: 
					miscerr = miscerr + 'error: ' + str(newval) + str(e) + '\r\n'
				except:
					miscerr = 'error: ' + str(newval) + str(e) + '\r\n'
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
