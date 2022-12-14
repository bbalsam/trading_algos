--Idea from https://youtu.be/bUejGzheCac

use stock

--1) Select the top 50 performing stocks over past 12 months
--2) Select the top 30 top performers from STEP 1 over last 6 months
--3) Select the top 10 top performers from STEP 2 over the last 3 months

DECLARE @inputdate DATE
DECLARE @yearpriordate DATE
DECLARE @halfyearpriordate DATE
DECLARE @quarterpriordate DATE
DECLARE @startdate DATE
DECLARE @enddate DATE

SET @inputdate = '12/1/2022'

SET @startdate = (SELECT TOP 1
					dtDate
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=@inputdate
				ORDER BY
					dtDate)

SET @yearpriordate = (SELECT TOP 1
					dtDate
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=DATEADD(mm,-12,@inputdate)
				ORDER BY
					dtDate)

SET @halfyearpriordate = (SELECT TOP 1
					dtDate
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=DATEADD(mm,-6,@inputdate)
				ORDER BY
					dtDate)

SET @quarterpriordate = (SELECT TOP 1
					dtDate
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=DATEADD(mm,-3,@inputdate)
				ORDER BY
					dtDate)

SET @enddate = (SELECT TOP 1
					dtDate
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=DATEADD(mm,1,@inputdate)
					);

WITH STOCK_DATA AS 
(
SELECT 
	*
FROM
	stocks
WHERE
	strTick in ('A','AAL','AAP','AAPL','ABBV','ABC','ABMD','ABNB','ABT','ACN','ADBE','ADI','ADM','ADP','ADSK','AEE','AEP','AES','AFL','AIG','AIZ','AJG','AKAM','ALB','ALGN','ALK','ALL','ALLE','AMAT','AMCR','AMD','AME','AMGN','AMP','AMT','AMZN','ANET','ANSS','AON','AOS','APA','APD','APH','APTV','ARE','ATO','ATVI','AVB','AVGO','AVY','AWK','AXP','AZO','BA','BAC','BAX','BBY','BDX','BEN','BIIB','BK','BKNG','BKR','BLK','BMY','BR','BSX','BWA','BXP','C','CAG','CAH','CARR','CAT','CB','CBOE','CBRE','CCI','CCL','CDNS','CDW','CE','CF','CFG','CHD','CHRW','CHTR','CI','CINF','CL','CLX','CMA','CMCSA','CME','CMG','CMI','CMS','CNC','CNP','COF','COIN','COO','COP','COST','CPB','CPRT','CRM','CRWD','CSCO','CSX','CTAS','CTSH','CTVA','CVS','CVX','D','DAL','DD','DE','DFS','DG','DGX','DHI','DHR','DIS','DISH','DLR','DLTR','DOV','DOW','DPZ','DRI','DTE','DUK','DVA','DVN','DXC','DXCM','EA','EBAY','ECL','ED','EFX','EIX','EL','EMN','EMR','ENPH','EOG','EQIX','EQR','ES','ESS','ETHE','ETN','ETR','ETSY','EVRG','EW','EXC','EXPD','EXPE','EXR','F','FANG','FAST','FBHS','FCX','FDX','FE','FFIV','FIS','FISV','FITB','FLT','FMC','FOX','FOXA','FRC','FRT','FTNT','FTV','GBTC','GD','GE','GILD','GIS','GL','GLD','GLW','GM','GOOG','GOOGL','GPC','GPN','GRMN','GS','GWW','HAL','HAS','HBAN','HCA','HD','HES','HIG','HII','HLT','HOG','HOLX','HON','HPE','HPQ','HRB','HRL','HSIC','HST','HSY','HUM','HWM','IBM','ICE','IDXX','IEX','IFF','ILMN','INCY','INTC','INTU','IP','IPG','IQV','IR','IRM','ISRG','IT','ITW','IVZ','J','JBHT','JCI','JKHY','JNJ','JNPR','JPM','K','KEY','KEYS','KHC','KIM','KLAC','KMB','KMI','KMX','KO','KR','L','LDOS','LEN','LH','LHX','LIN','LKQ','LLY','LMT','LNC','LNT','LOW','LRCX','LULU','LUV','LVS','LW','LYB','LYV','MA','MAA','MAR','MAS','MCD','MCHP','MCK','MCO','MDLZ','MDT','MET','MGM','MHK','MKC','MKTX','MLM','MMC','MMM','MNST','MO','MOS','MPC','MRK','MRO','MS','MSCI','MSFT','MSI','MTB','MTD','MU','NCLH','NDAQ','NEE','NEM','NFLX','NI','NKE','NOC','NOW','NRG','NSC','NTAP','NTRS','NUE','NVDA','NVR','NWL','NWS','NWSA','O','ODFL','OKE','OMC','ORCL','ORLY','OTIS','OXY','PAYC','PAYX','PCAR','PEAK','PEG','PEP','PFE','PFG','PG','PGR','PH','PHM','PKG','PKI','PLD','PM','PNC','PNR','PNW','PPG','PPL','PRU','PSA','PSX','PTON','PWR','PXD','PYPL','QCOM','QRVO','RCL','RE','REG','REGN','RF','RHI','RJF','RL','RMD','ROK','ROL','ROP','ROST','RSG','RTX','SBAC','SBUX','SCHW','SEDG','SEE','SHW','SIVB','SJM','SLB','SLV','SNA','SNPS','SO','SPCE','SPG','SPGI','SPXL','SPY','SRE','STE','STT','STX','STZ','SWK','SWKS','SYF','SYK','SYY','T','TAP','TDG','TEL','TER','TFC','TFX','TGT','TJX','TMO','TMUS','TPR','TQQQ','TROW','TRV','TSCO','TSLA','TSN','TT','TTWO','TXN','TXT','UAL','UDR','UHS','ULTA','UNH','UNP','UPS','URI','USB','V','VFC','VLO','VMC','VNO','VRSK','VRSN','VRTX','VTR','VZ','WAB','WAT','WBA','WDC','WEC','WELL','WFC','WHR','WM','WMB','WMT','WRB','WRK','WST','WY','WYNN','XEL','XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY','XOM','XRAY','XRX','XYL','YUM','ZBH','ZBRA','ZION','ZM','ZTS')
	AND dtDate in (@startdate,@quarterpriordate,@halfyearpriordate,@yearpriordate,@enddate)
)
SELECT TOP 10
	strTick
	,CASE WHEN SUM(CASE WHEN dtDate = @startdate THEN decAdjClose ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN dtDate = @enddate THEN decAdjClose ELSE 0 END)/SUM(CASE WHEN dtDate = @startdate THEN decAdjClose ELSE 0 END) END AS END_PCT
	,@startdate AS START_DATE
	,SUM(CASE WHEN dtDate = @startdate THEN decAdjClose ELSE 0 END) AS START_CLOSE
	,@quarterpriordate AS QTR_DATE
	,SUM(CASE WHEN dtDate = @quarterpriordate THEN decAdjClose ELSE 0 END) AS QTR_CLOSE
	,CASE WHEN SUM(CASE WHEN dtDate = @quarterpriordate THEN decAdjClose ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN dtDate = @startdate THEN decAdjClose ELSE 0 END)/SUM(CASE WHEN dtDate = @quarterpriordate THEN decAdjClose ELSE 0 END) END AS QTR_PCT
	,@halfyearpriordate AS HALF_DATE
	,SUM(CASE WHEN dtDate = @halfyearpriordate THEN decAdjClose ELSE 0 END) AS HALF_CLOSE
	,CASE WHEN SUM(CASE WHEN dtDate = @halfyearpriordate THEN decAdjClose ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN dtDate = @startdate THEN decAdjClose ELSE 0 END)/SUM(CASE WHEN dtDate = @halfyearpriordate THEN decAdjClose ELSE 0 END) END AS HALF_PCT
	,@yearpriordate AS YEAR_DATE
	,SUM(CASE WHEN dtDate = @yearpriordate THEN decAdjClose ELSE 0 END) AS YEAR_CLOSE
	,CASE WHEN SUM(CASE WHEN dtDate = @yearpriordate THEN decAdjClose ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN dtDate = @startdate THEN decAdjClose ELSE 0 END)/SUM(CASE WHEN dtDate = @yearpriordate THEN decAdjClose ELSE 0 END) END AS YEAR_PCT
	,@enddate AS END_DATE
	,SUM(CASE WHEN dtDate = @enddate THEN decAdjClose ELSE 0 END) AS END_CLOSE
FROM
	STOCK_DATA
GROUP BY
	strTick
ORDER BY
	CASE WHEN SUM(CASE WHEN dtDate = @yearpriordate THEN decAdjClose ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN dtDate = @startdate THEN decAdjClose ELSE 0 END)/SUM(CASE WHEN dtDate = @yearpriordate THEN decAdjClose ELSE 0 END) END DESC
	,CASE WHEN SUM(CASE WHEN dtDate = @halfyearpriordate THEN decAdjClose ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN dtDate = @startdate THEN decAdjClose ELSE 0 END)/SUM(CASE WHEN dtDate = @halfyearpriordate THEN decAdjClose ELSE 0 END) END DESC
	,CASE WHEN SUM(CASE WHEN dtDate = @quarterpriordate THEN decAdjClose ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN dtDate = @startdate THEN decAdjClose ELSE 0 END)/SUM(CASE WHEN dtDate = @quarterpriordate THEN decAdjClose ELSE 0 END) END DESC
