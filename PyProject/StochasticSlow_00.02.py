#================================================================
#VERSION
#================================================================
#00.01			Working concept
#00.02			Fixed bug averaging wrong date after run first time

#================================================================import pyodbc
import pyodbc
import time

conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='LAPTOP-D6TKOBQR\SQLEXPRESS',DATABASE='stockdb',Trusted_connection='yes')
crsr = conn.cursor()


ticker_list=['A','AAL','AAP','AAPL','ABBV','ABC','ABMD','ABT','ACN','ADBE','ADI','ADM','ADP','ADS','ADSK','AEE','AEP','AES','AFL','AIG','AIV','AIZ','AJG','AKAM','ALB','ALGN','ALK','ALL','ALLE','ALXN','AMAT','AMCR','AMD','AME','AMGN','AMP','AMT','AMZN','ANET','ANSS','ANTM','AON','AOS','APA','APD','APH','APTV','ARE','ATO','ATVI','AVB','AVGO','AVY','AWK','AXP','AZO','BA','BAC','BAX','BBY','BDX','BEN','BF.B','BIIB','BK','BKNG','BKR','BLK','BLL','BMY','BR','BRK.B','BSX','BWA','BXP','C','CAG','CAH','CARR','CAT','CB','CBOE','CBRE','CCI','CCL','CDNS','CDW','CE','CERN','CF','CFG','CHD','CHRW','CHTR','CI','CINF','CL','CLX','CMA','CMCSA','CME','CMG','CMI','CMS','CNC','CNP','COF','COG','COO','COP','COST','COTY','CPB','CPRT','CRM','CSCO','CSX','CTAS','CTL','CTSH','CTVA','CTXS','CVS','CVX','CXO','D','DAL','DD','DE','DFS','DG','DGX','DHI','DHR','DIS','DISCA','DISCK','DISH','DLR','DLTR','DOV','DOW','DPZ','DRE','DRI','DTE','DUK','DVA','DVN','DXC','DXCM','EA','EBAY','ECL','ED','EFX','EIX','EL','EMN','EMR','EOG','EQIX','EQR','ES','ESS','ETFC','ETN','ETR','EVRG','EW','EXC','EXPD','EXPE','EXR','F','FANG','FAST','FB','FBHS','FCX','FDX','FE','FFIV','FIS','FISV','FITB','FLIR','FLS','FLT','FMC','FOX','FOXA','FRC','FRT','FTI','FTNT','FTV','GD','GE','GILD','GIS','GL','GLD','GLW','GM','GOOG','GOOGL','GPC','GPN','GPS','GRMN','GS','GWW','HAL','HAS','HBAN','HBI','HCA','HD','HES','HFC','HIG','HII','HLT','HOG','HOLX','HON','HPE','HPQ','HRB','HRL','HSIC','HST','HSY','HUM','HWM','IBM','ICE','IDXX','IEX','IFF','ILMN','INCY','INFO','INTC','INTU','IP','IPG','IPGP','IQV','IR','IRM','ISRG','IT','ITW','IVZ','J','JBHT','JCI','JKHY','JNJ','JNPR','JPM','JWN','K','KEY','KEYS','KHC','KIM','KLAC','KMB','KMI','KMX','KO','KR','KSS','KSU','L','LB','LDOS','LEG','LEN','LH','LHX','LIN','LKQ','LLY','LMT','LNC','LNT','LOW','LRCX','LUV','LVS','LW','LYB','LYV','MA','MAA','MAR','MAS','MCD','MCHP','MCK','MCO','MDLZ','MDT','MET','MGM','MHK','MKC','MKTX','MLM','MMC','MMM','MNST','MO','MOS','MPC','MRK','MRO','MS','MSCI','MSFT','MSI','MTB','MTD','MU','MXIM','MYL','NBL','NCLH','NDAQ','NEE','NEM','NFLX','NI','NKE','NLOK','NLSN','NOC','NOV','NOW','NRG','NSC','NTAP','NTRS','NUE','NVDA','NVR','NWL','NWS','NWSA','O','ODFL','OKE','OMC','ORCL','ORLY','OTIS','OXY','PAYC','PAYX','PBCT','PCAR','PEAK','PEG','PEP','PFE','PFG','PG','PGR','PH','PHM','PKG','PKI','PLD','PM','PNC','PNR','PNW','PPG','PPL','PRGO','PRU','PSA','PSX','PVH','PWR','PXD','PYPL','QCOM','QRVO','RCL','RE','REG','REGN','RF','RHI','RJF','RL','RMD','ROK','ROL','ROP','ROST','RSG','RTX','SBAC','SBUX','SCHW','SEE','SHW','SIVB','SJM','SLB','SLG','SLV','SNA','SNPS','SO','SPG','SPGI','SRE','STE','STT','STX','STZ','SWK','SWKS','SYF','SYK','SYY','T','TAP','TDG','TEL','TFC','TFX','TGT','TIF','TJX','TMO','TMUS','TPR','TROW','TRV','TSCO','TSN','TT','TTWO','TWTR','TXN','TXT','UA','UAA','UAL','UDR','UHS','ULTA','UNH','UNM','UNP','UPS','URI','USB','V','VAR','VFC','VIAC','VLO','VMC','VNO','VRSK','VRSN','VRTX','VTR','VZ','WAB','WAT','WBA','WDC','WEC','WELL','WFC','WHR','WLTW','WM','WMB','WMT','WRB','WRK','WST','WU','WY','WYNN','XEL','XLNX','XOM','XRAY','XRX','XYL','YUM','ZBH','ZBRA','ZION','ZTS']


#ticker_list=['CINF']

def processStochastic(ticker):

	sql = 	"""
			DECLARE @lastDate	date

			SET @lastDate = (
				SELECT TOP 1 snp.dtDate
				FROM	stochastic sto
				JOIN	snp500_test snp ON snp.ID = sto.snp500ID
				WHERE	snp.strTick like ? and sto.deckslow is null
				GROUP BY snp.dtdate,sto.decKFAST,sto.snp500ID
				HAVING	snp.dtdate <= max(snp.dtdate)
				ORDER BY sto.snp500ID DESC
				)

			SELECT COUNT(*)
			FROM (
			SELECT TOP 3	decKFAST as KSLOW, dtdate
			FROM			stochastic sto
			JOIN			snp500_test snp	ON snp.ID = sto.snp500ID
			WHERE			snp.dtDate <= @lastDate
							AND snp.strTick = ?
			ORDER BY		dtDate desc
				) thr
			"""

	rows = crsr.execute(sql,tik,tik)
	rowct = crsr.fetchone()
	

	while rowct[0] == 3:
		sql = 	"""
				DECLARE @lastDate	date

				SET @lastDate = (
					SELECT TOP 1 snp.dtDate
					FROM	stochastic sto
					JOIN	snp500_test snp ON snp.ID = sto.snp500ID
					WHERE	snp.strTick like ? and sto.deckslow is null
					GROUP BY snp.dtdate,sto.decKFAST,sto.snp500ID
					HAVING	snp.dtdate <= max(snp.dtdate)
					ORDER BY sto.snp500ID DESC
					)

				SELECT COUNT(*)
				FROM (
				SELECT TOP 3	decKFAST as KSLOW, dtdate
				FROM			stochastic sto
				JOIN			snp500_test snp	ON snp.ID = sto.snp500ID
				WHERE			snp.dtDate <= @lastDate
								AND snp.strTick = ?
				ORDER BY		dtDate desc
					) thr
				"""
		rows = crsr.execute(sql,tik,tik)
		rowct = crsr.fetchone()
		print(rowct[0])
		sql = 	"""
				DECLARE @SNP500ID	varchar(16)
				DECLARE @lastDate	date
				DECLARE @kSlow  DECIMAL(6, 3)

				SET @SNP500ID = (SELECT thr.ID as ID
								FROM
								(SELECT TOP 1 snp.ID
								FROM	stochastic sto
								JOIN	snp500_test snp ON snp.ID = sto.snp500ID
								WHERE	snp.strTick like ? and sto.deckslow is null
								GROUP BY snp.dtdate,sto.decKFAST,snp.ID
								HAVING	snp.dtdate = max(snp.dtdate)
								ORDER BY snp.ID DESC
								) thr)

				SET @lastDate = (SELECT thr.dtdate
								FROM
								(SELECT TOP 1 snp.dtdate
								FROM	stochastic sto
								JOIN	snp500_test snp ON snp.ID = sto.snp500ID
								WHERE	snp.strTick like ? and sto.deckslow is null
								GROUP BY snp.dtdate,sto.decKFAST,snp.ID
								HAVING	snp.dtdate = max(snp.dtdate)
								ORDER BY snp.ID DESC
								) thr)

				SET @kSlow = (SELECT AVG(thr.decKFAST) as KSLOW
							FROM
							(SELECT TOP 3 sto.decKFAST
							FROM	stochastic sto
							JOIN	snp500_test snp ON snp.ID = sto.snp500ID
							WHERE	snp.strTick like ? and snp.dtdate <=@lastDate
							GROUP BY snp.dtdate,sto.decKFAST,sto.snp500ID
							HAVING	snp.dtdate <= max(snp.dtdate)
							ORDER BY sto.snp500ID DESC
							) thr)

				UPDATE stochastic SET decKSlow = @kSlow where snp500ID = @SNP500ID
				"""
		rows = crsr.execute(sql,tik,tik,tik)
		conn.commit()
	
for tik in ticker_list:
	timestamp = time.strftime('%H%M%S')
	print(tik + ' started at ' + str(timestamp))
	processStochastic(tik)
	timestamp = time.strftime('%H%M%S')
	print(tik + ' completed at ' + str(timestamp))

print('done')
exit()