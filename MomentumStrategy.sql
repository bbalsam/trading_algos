--Idea from https://youtu.be/bUejGzheCac

--Adjust on a fixed day monthly
use stock

DECLARE @inputdate DATE
DECLARE @startdate DATE
DECLARE @enddate DATE

SET @inputdate = '01/01/2021'

SET @startdate = (SELECT TOP 1
					dtDate
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=@inputdate)


SET @enddate = (SELECT TOP 1
					dtDate
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=DATEADD(mm,1,@startdate)
					);


WITH STOCK_DATA AS 
(
SELECT 
	*
	,LAG(decAdjClose,63) OVER (PARTITION BY strTick ORDER BY dtDate ASC) AS PREV_CLOSE_063
	,LAG(dtDate,63) OVER (PARTITION BY strTick ORDER BY dtDate ASC) AS PREV_DATE_063
	,LAG(decAdjClose,126) OVER (PARTITION BY strTick ORDER BY dtDate ASC) AS PREV_CLOSE_126
	,LAG(dtDate,126) OVER (PARTITION BY strTick ORDER BY dtDate ASC) AS PREV_DATE_126
	,LAG(decAdjClose,252) OVER (PARTITION BY strTick ORDER BY dtDate ASC) AS PREV_CLOSE_252
	,LAG(dtDate,252) OVER (PARTITION BY strTick ORDER BY dtDate ASC) AS PREV_DATE_252
FROM
	stocks
WHERE
	strTick in ('A','AAL','AAP','AAPL','ABBV','ABC','ABMD','ABNB','ABT','ACN','ADBE','ADI','ADM','ADP','ADSK','AEE','AEP','AES','AFL','AIG','AIZ','AJG','AKAM','ALB','ALGN','ALK','ALL','ALLE','AMAT','AMCR','AMD','AME','AMGN','AMP','AMT','AMZN','ANET','ANSS','AON','AOS','APA','APD','APH','APTV','ARE','ATO','ATVI','AVB','AVGO','AVY','AWK','AXP','AZO','BA','BAC','BAX','BBY','BDX','BEN','BIIB','BK','BKNG','BKR','BLK','BMY','BR','BSX','BWA','BXP','C','CAG','CAH','CARR','CAT','CB','CBOE','CBRE','CCI','CCL','CDNS','CDW','CE','CF','CFG','CHD','CHRW','CHTR','CI','CINF','CL','CLX','CMA','CMCSA','CME','CMG','CMI','CMS','CNC','CNP','COF','COIN','COO','COP','COST','CPB','CPRT','CRM','CRWD','CSCO','CSX','CTAS','CTSH','CTVA','CVS','CVX','D','DAL','DD','DE','DFS','DG','DGX','DHI','DHR','DIS','DISH','DLR','DLTR','DOV','DOW','DPZ','DRI','DTE','DUK','DVA','DVN','DXC','DXCM','EA','EBAY','ECL','ED','EFX','EIX','EL','EMN','EMR','ENPH','EOG','EQIX','EQR','ES','ESS','ETHE','ETN','ETR','ETSY','EVRG','EW','EXC','EXPD','EXPE','EXR','F','FANG','FAST','FBHS','FCX','FDX','FE','FFIV','FIS','FISV','FITB','FLT','FMC','FOX','FOXA','FRC','FRT','FTNT','FTV','GBTC','GD','GE','GILD','GIS','GL','GLD','GLW','GM','GOOG','GOOGL','GPC','GPN','GRMN','GS','GWW','HAL','HAS','HBAN','HCA','HD','HES','HIG','HII','HLT','HOG','HOLX','HON','HPE','HPQ','HRB','HRL','HSIC','HST','HSY','HUM','HWM','IBM','ICE','IDXX','IEX','IFF','ILMN','INCY','INTC','INTU','IP','IPG','IQV','IR','IRM','ISRG','IT','ITW','IVZ','J','JBHT','JCI','JKHY','JNJ','JNPR','JPM','K','KEY','KEYS','KHC','KIM','KLAC','KMB','KMI','KMX','KO','KR','L','LDOS','LEN','LH','LHX','LIN','LKQ','LLY','LMT','LNC','LNT','LOW','LRCX','LULU','LUV','LVS','LW','LYB','LYV','MA','MAA','MAR','MAS','MCD','MCHP','MCK','MCO','MDLZ','MDT','MET','MGM','MHK','MKC','MKTX','MLM','MMC','MMM','MNST','MO','MOS','MPC','MRK','MRO','MS','MSCI','MSFT','MSI','MTB','MTD','MU','NCLH','NDAQ','NEE','NEM','NFLX','NI','NKE','NOC','NOW','NRG','NSC','NTAP','NTRS','NUE','NVDA','NVR','NWL','NWS','NWSA','O','ODFL','OKE','OMC','ORCL','ORLY','OTIS','OXY','PAYC','PAYX','PCAR','PEAK','PEG','PEP','PFE','PFG','PG','PGR','PH','PHM','PKG','PKI','PLD','PM','PNC','PNR','PNW','PPG','PPL','PRU','PSA','PSX','PTON','PWR','PXD','PYPL','QCOM','QRVO','RCL','RE','REG','REGN','RF','RHI','RJF','RL','RMD','ROK','ROL','ROP','ROST','RSG','RTX','SBAC','SBUX','SCHW','SEDG','SEE','SHW','SIVB','SJM','SLB','SLV','SNA','SNPS','SO','SPCE','SPG','SPGI','SPXL','SPY','SRE','STE','STT','STX','STZ','SWK','SWKS','SYF','SYK','SYY','T','TAP','TDG','TEL','TER','TFC','TFX','TGT','TJX','TMO','TMUS','TPR','TQQQ','TROW','TRV','TSCO','TSLA','TSN','TT','TTWO','TXN','TXT','UAL','UDR','UHS','ULTA','UNH','UNP','UPS','URI','USB','V','VFC','VLO','VMC','VNO','VRSK','VRSN','VRTX','VTR','VZ','WAB','WAT','WBA','WDC','WEC','WELL','WFC','WHR','WM','WMB','WMT','WRB','WRK','WST','WY','WYNN','XEL','XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY','XOM','XRAY','XRX','XYL','YUM','ZBH','ZBRA','ZION','ZM','ZTS')
)


,STEP1 AS --Select the top 50 performing stocks over past 12 months (PREV_CLOSE_252)
(
SELECT TOP 50
	*
	,decAdjClose/PREV_CLOSE_252 AS PCT_CHG_252
FROM
	STOCK_DATA
WHERE
	dtDate = @startdate
ORDER BY
	decAdjClose/PREV_CLOSE_252 DESC
)


,STEP2 AS --Select the top 30 top performers from STEP1 over last 6 months (PREV_CLOSE_126)
(
SELECT TOP 30
	*
	,decAdjClose/PREV_CLOSE_126 AS PCT_CHG_126
FROM
	STEP1
ORDER BY
	decAdjClose/PREV_CLOSE_126 DESC
)

,STEP3 AS --Select the top 10 top performers from STEP2 over last 3 months
(
SELECT TOP 10
	*
	,decAdjClose/PREV_CLOSE_126 AS PCT_CHG_063
FROM
	STEP2
ORDER BY
	decAdjClose/PREV_CLOSE_063 DESC
)
,STOCK_SELECTION AS
(
SELECT
	STOCKS.*
	,LAG(STOCKS.decAdjClose,21) OVER (PARTITION BY STOCKS.strTick ORDER BY STOCKS.dtDate ASC) AS PREV_CLOSE_021
	,STOCKS.decAdjClose/LAG(STOCKS.decAdjClose,21) OVER (PARTITION BY STOCKS.strTick ORDER BY STOCKS.dtDate ASC) AS PCT_CHG_021
	,@startdate AS BEG_DATE
	,LAG(STOCKS.dtDate,21) OVER (PARTITION BY STOCKS.strTick ORDER BY STOCKS.dtDate ASC) AS PREV_DATE_021
FROM
	STOCKS
JOIN
	STEP3 ON STEP3.strTick = STOCKS.strTick
)



SELECT
	strTick
	,@startdate AS START_DATE
	,@enddate AS END_DATE
	,PCT_CHG_021
	,PREV_DATE_021
FROM
	STOCK_SELECTION
WHERE
	dtDate = @enddate

/*SELECT
strTick
,min(dtdate) as MINDATE
,MIN(decClose) AS MINCLOSE
,MAX(decClose) AS MAXCLOSE
FROM
STOCKS
GROUP BY
strTick
ORDER BY
1*/