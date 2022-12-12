#================================================================
#VERSION
#================================================================
#00.00			Original basic download with tweaking and working proof of concept.

#================================================================

import datetime
import yfinance as yf
from datetime import datetime, timedelta
from pandas_datareader import data as pdr
from datetime import date

yf.pdr_override()

import pandas as pd

# Tickers list
# We can add and delete any ticker from the list to get desired ticker live data
#'ADI','ADP','ATO'
ticker_list=['AAL']
# ,'AAPL','ACN','ADBE','ADS','ADSK','AEP','AES','AIV','AKAM','ALK','ALLE','AMAT','AMCR','AMD','AME','AMT','ANET','ANSS','AOS','APD','APH','ARE','ATVI','AVB','AVGO','AVY','AWK','BA','BLL','BR','BXP','CARR','CAT','CBRE','CCI','CDNS','CDW','CE','CF','CHRW','CHTR','CMCSA','CMI','CMS','CNP','CPRT','CRM','CSCO','CSX','CTAS','CTL','CTSH','CTVA','CTXS','D','DAL','DD','DE','DIS','DISCA','DISCK','DISH','DLR','DOV','DOW','DRE','DTE','DUK','DXC','EA','ECL','ED','EFX','EIX','EMN','EMR','EQIX','EQR','ES','ESS','ETN','ETR','EVRG','EXC','EXPD','EXR','FAST','FB','FBHS','FCX','FDX','FE','FFIV','FIS','FISV','FLIR','FLS','FLT','FMC','FOX','FOXA','FRT','FTNT','FTV','GD','GE','GLW','GOOG','GOOGL','GPN','GWW','HII','HON','HPE','HPQ','HST','HWM','IBM','IEX','IFF','INFO','INTC','INTU','IP','IPG','IPGP','IR','IRM','IT','ITW','J','JBHT','JCI','JKHY','JNPR','KEYS','KIM','KLAC','KSU','LDOS','LHX','LIN','LMT','LNT','LRCX','LUV','LYB','LYV','MA','MAA','MAS','MCHP','MLM','MMM','MOS','MSFT','MSI','MU','MXIM','NEE','NEM','NFLX','NI','NLOK','NLSN','NOC','NOW','NRG','NSC','NTAP','NUE','NVDA','NWS','NWSA','O','ODFL','OMC','ORCL','OTIS','PAYC','PAYX','PCAR','PEAK','PEG','PH','PKG','PLD','PNR','PNW','PPG','PPL','PSA','PWR','PYPL','QCOM','QRVO','REG','RHI','ROK','ROL','ROP','RSG','RTX','SBAC','SEE','SHW','SLG','SNA','SNPS','SO','SPG','SRE','STX','SWK','SWKS','T','TDG','TEL','TMUS','TT','TTWO','TWTR','TXN','TXT','UAL','UDR','UNP','UPS','URI','V','VIAC','VMC','VNO','VRSK','VRSN','VTR','VZ','WAB','WDC','WEC','WELL','WM','WRK','WU','WY','XEL','XLNX','XRX','XYL','ZBRA','A','AAP','ABBV','ABC','ABMD','ABT','ADM','AEE','AFL','AIG','AIZ','AJG','ALB','ALGN','ALL','ALXN','AMGN','AMP','AMZN','ANTM','AON','APA','APTV','AXP','AZO','BAC','BAX','BBY','BDX','BEN','BF.B','BIIB','BK','BKNG','BKR','BLK','BMY','BRK.B','BSX','BWA','C','CAG','CAH','CB','CBOE','CCL','CERN','CFG','CHD','CI','CINF','CL','CLX','CMA','CME','CMG','CNC','COF','COG','COO','COP','COST','COTY','CPB','CVS','CVX','CXO','DFS','DG','DGX','DHI','DHR','DLTR','DPZ','DRI','DVA','DVN','DXCM','EBAY','EL','EOG','ETFC','EW','EXPE','F','FANG','FITB','FRC','FTI','GILD','GIS','GL','GM','GPC','GPS','GRMN','GS','HAL','HAS','HBAN','HBI','HCA','HD','HES','HFC','HIG','HLT','HOG','HOLX','HRB','HRL','HSIC','HSY','HUM','ICE','IDXX','ILMN','INCY','IQV','ISRG','IVZ','JNJ','JPM','JWN','K','KEY','KHC','KMB','KMI','KMX','KO','KR','KSS','L','LB','LEG','LEN','LH','LKQ','LLY','LNC','LOW','LVS','LW','MAR','MCD','MCK','MCO','MDLZ','MDT','MET','MGM','MHK','MKC','MKTX','MMC','MNST','MO','MPC','MRK','MRO','MS','MSCI','MTB','MTD','MYL','NBL','NCLH','NDAQ','NKE','NOV','NTRS','NVR','NWL','OKE','ORLY','OXY','PBCT','PEP','PFE','PFG','PG','PGR','PHM','PKI','PM','PNC','PRGO','PRU','PSX','PVH','PXD','RCL','RE','REGN','RF','RJF','RL','RMD','ROST','SBUX','SCHW','SIVB','SJM','SLB','SPGI','STE','STT','STZ','SYF','SYK','SYY','TAP','TFC','TFX','TGT','TIF','TJX','TMO','TPR','TROW','TRV','TSCO','TSN','UA','UAA','UHS','ULTA','UNH','UNM','USB','VAR','VFC','VLO','VRTX','WAT','WBA','WFC','WHR','WLTW','WMB','WMT','WRB','WST','WYNN','XOM','XRAY','YUM','ZBH','ZION','ZTS']
today = date.today()
# We can get data by our choice by giving days bracket
#start_date = datetime.datetime.strptime('2020–01–01','%Y-%m-%d')
#end_date= datetime.date(2020, 6, 26)#'2020–06–26'
files=[]

def getData(ticker):
	print(ticker)
	data = pdr.get_data_yahoo(ticker, start=today-timedelta(10), end=today)
	dataname= ticker+'_'+str(today)
	files.append(dataname)
	SaveData(data, dataname)
# Create a data folder in your current dir.
def SaveData(df, filename):
	df.to_csv('.\SANDPDownload\\'+filename+'.csv')
#This loop will iterate over ticker list, will pass one ticker to get data, and save that data as file.

for tik in ticker_list:
	getData(tik)
for i in range(0,11):
	df1= pd.read_csv('.\SANDPDownload\\'+ str(files[i])+'.csv')
	print(df1.head())