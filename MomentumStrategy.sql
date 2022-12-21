--Adjust on a fixed day monthly
use stock

DECLARE @inputdate DATE
DECLARE @volumeMin BIGINT
DECLARE @yearpriordate DATE
DECLARE @halfyearpriordate DATE
DECLARE @quarterpriordate DATE
DECLARE @pulldate DATE
DECLARE @startdate DATE
DECLARE @endpulldate DATE
DECLARE @enddate DATE

SET @inputdate = '11/01/2022'
SET @volumeMin = 25000000

/*SET @inputdate = (SELECT MAX(dtDate)
				FROM
					STOCKS
				WHERE
					strTick = 'SPY')*/

SET @pulldate = (SELECT MAX(dtDate)
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate <@inputdate)

SET @startdate = (SELECT MIN(dtDate)
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=@inputdate)

SET @yearpriordate = (SELECT MIN(dtDate)
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=DATEADD(mm,-12,@pulldate))

SET @halfyearpriordate = (SELECT MIN(dtDate)
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=DATEADD(mm,-6,@pulldate))

SET @quarterpriordate = (SELECT MIN(dtDate)
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=DATEADD(mm,-3,@pulldate))

SET @enddate = (SELECT MIN(dtDate)
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=DATEADD(mm,1,@inputdate)
					)
					
SET @endpulldate = (SELECT MIN(dtDate)
				FROM
					STOCKS
				WHERE
					strTick = 'SPY'
					AND dtDate >=DATEADD(mm,1,@pulldate)
					);

--Query to view variables:
--SELECT @inputdate input,@pulldate pull,@startdate startdate,@yearpriordate yearprior,@halfyearpriordate half,@quarterpriordate qtr,@enddate enddate,@endpulldate endpulldate


WITH STOCK_DATA AS 
(
SELECT 
	s.*
FROM
	stocks s
JOIN
	(SELECT
		strtick
		,avg(intVol) AS AVGVol
	FROM
		stocks
	WHERE
		dtDate >='01/01/2022'
		AND strTick not like '%-%'
		AND strTick not like '%^%'
		AND strTick not like '%=%'
	GROUP BY
		strtick
	HAVING
		AVG(intvol) > @volumeMin
	) ins ON ins.strtick = s.strtick
	--IndexStocks ins ON ins.Symbol = s.strTick
WHERE
	--strTick in ('A','AAL','AAP','AAPL','ABBV','ABC','ABMD','ABNB','ABT','ACN','ADBE','ADI','ADM','ADP','ADSK','AEE','AEP','AES','AFL','AIG','AIZ','AJG','AKAM','ALB','ALGN','ALK','ALL','ALLE','AMAT','AMCR','AMD','AME','AMGN','AMP','AMT','AMZN','ANET','ANSS','AON','AOS','APA','APD','APH','APTV','ARE','ATO','ATVI','AVB','AVGO','AVY','AWK','AXP','AZO','BA','BAC','BAX','BBY','BDX','BEN','BIIB','BK','BKNG','BKR','BLK','BMY','BR','BSX','BWA','BXP','C','CAG','CAH','CARR','CAT','CB','CBOE','CBRE','CCI','CCL','CDNS','CDW','CE','CF','CFG','CHD','CHRW','CHTR','CI','CINF','CL','CLX','CMA','CMCSA','CME','CMG','CMI','CMS','CNC','CNP','COF','COIN','COO','COP','COST','CPB','CPRT','CRM','CRWD','CSCO','CSX','CTAS','CTSH','CTVA','CVS','CVX','D','DAL','DD','DE','DFS','DG','DGX','DHI','DHR','DIS','DISH','DLR','DLTR','DOV','DOW','DPZ','DRI','DTE','DUK','DVA','DVN','DXC','DXCM','EA','EBAY','ECL','ED','EFX','EIX','EL','EMN','EMR','ENPH','EOG','EQIX','EQR','ES','ESS','ETHE','ETN','ETR','ETSY','EVRG','EW','EXC','EXPD','EXPE','EXR','F','FANG','FAST','FBHS','FCX','FDX','FE','FFIV','FIS','FISV','FITB','FLT','FMC','FOX','FOXA','FRC','FRT','FTNT','FTV','GBTC','GD','GE','GILD','GIS','GL','GLD','GLW','GM','GOOG','GOOGL','GPC','GPN','GRMN','GS','GWW','HAL','HAS','HBAN','HCA','HD','HES','HIG','HII','HLT','HOG','HOLX','HON','HPE','HPQ','HRB','HRL','HSIC','HST','HSY','HUM','HWM','IBM','ICE','IDXX','IEX','IFF','ILMN','INCY','INTC','INTU','IP','IPG','IQV','IR','IRM','ISRG','IT','ITW','IVZ','J','JBHT','JCI','JKHY','JNJ','JNPR','JPM','K','KEY','KEYS','KHC','KIM','KLAC','KMB','KMI','KMX','KO','KR','L','LDOS','LEN','LH','LHX','LIN','LKQ','LLY','LMT','LNC','LNT','LOW','LRCX','LULU','LUV','LVS','LW','LYB','LYV','MA','MAA','MAR','MAS','MCD','MCHP','MCK','MCO','MDLZ','MDT','MET','MGM','MHK','MKC','MKTX','MLM','MMC','MMM','MNST','MO','MOS','MPC','MRK','MRO','MS','MSCI','MSFT','MSI','MTB','MTD','MU','NCLH','NDAQ','NEE','NEM','NFLX','NI','NKE','NOC','NOW','NRG','NSC','NTAP','NTRS','NUE','NVDA','NVR','NWL','NWS','NWSA','O','ODFL','OKE','OMC','ORCL','ORLY','OTIS','OXY','PAYC','PAYX','PCAR','PEAK','PEG','PEP','PFE','PFG','PG','PGR','PH','PHM','PKG','PKI','PLD','PM','PNC','PNR','PNW','PPG','PPL','PRU','PSA','PSX','PTON','PWR','PXD','PYPL','QCOM','QRVO','RCL','RE','REG','REGN','RF','RHI','RJF','RL','RMD','ROK','ROL','ROP','ROST','RSG','RTX','SBAC','SBUX','SCHW','SEDG','SEE','SHW','SIVB','SJM','SLB','SLV','SNA','SNPS','SO','SPCE','SPG','SPGI','SPXL','SPY','SRE','STE','STT','STX','STZ','SWK','SWKS','SYF','SYK','SYY','T','TAP','TDG','TEL','TER','TFC','TFX','TGT','TJX','TMO','TMUS','TPR','TQQQ','TROW','TRV','TSCO','TSLA','TSN','TT','TTWO','TXN','TXT','UAL','UDR','UHS','ULTA','UNH','UNP','UPS','URI','USB','V','VFC','VLO','VMC','VNO','VRSK','VRSN','VRTX','VTR','VZ','WAB','WAT','WBA','WDC','WEC','WELL','WFC','WHR','WM','WMB','WMT','WRB','WRK','WST','WY','WYNN','XEL','XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY','XOM','XRAY','XRX','XYL','YUM','ZBH','ZBRA','ZION','ZM','ZTS')
	--AND strTick NOT IN('ACGL','TRGP','CSGP','INVH','KDP','ON','VICI','CPT','MOH','NDSN','FDS','SBNY','SEDG','EPAM','BRO','CDAY','MTCH','TECH','MRNA','OGN','CRL','PTC','CZR','GNRC','NXPI','MPWR','TRMB','ENPH','TSLA','POOL','CTLT','ETSY','BIO','TDY','TYL','WST','DPZ','DXCM','CARR','OTIS','HWM','PAYC','LYV','STE','ZBRA','ODFL','WRB','NOW','LVS','NVR','CDW','IEX','LDOS','TMUS','MKTX','AMCR','CTVA','WAB','ATO','TFX','FRC')
	--AND
	s.dtDate in (@pulldate,@startdate,@quarterpriordate,@halfyearpriordate,@yearpriordate,@enddate,@endpulldate)
	--AND ins.date_first_added <= @startdate
)
SELECT TOP 10
	strTick
	,CASE  --Sell over Buy (@enddate/@startdate)
		WHEN SUM(CASE
					WHEN dtDate = @startdate
					THEN decAdjClose
					ELSE 0
				END) = 0
		THEN 0
		ELSE SUM(CASE
					WHEN dtDate = @enddate
					THEN decAdjClose
					ELSE 0
				END)
			/SUM(CASE
					WHEN dtDate = @startdate
					THEN decAdjClose
					ELSE 0
				END)
	END AS END_PCT
	,@startdate AS START_DATE
	,@enddate AS END_DATE
	,@pulldate AS PULL_DATE
	,@quarterpriordate AS QTR_DATE	
	,@halfyearpriordate AS HALF_DATE
	,@yearpriordate AS YEAR_DATE
	,SUM(CASE
			WHEN dtDate = @startdate
			THEN decAdjClose
			ELSE 0
		END) AS START_CLOSE
	,SUM(CASE
			WHEN dtDate = @enddate
			THEN decAdjClose
			ELSE 0
		END) AS END_CLOSE
	,SUM(CASE
			WHEN dtDate = @quarterpriordate
			THEN decAdjClose
			ELSE 0
		END) AS QTR_CLOSE
	,SUM(CASE
			WHEN dtDate = @halfyearpriordate
			THEN decAdjClose
			ELSE 0
		END) AS HALF_CLOSE
	,SUM(CASE
			WHEN dtDate = @yearpriordate
			THEN decAdjClose
			ELSE 0
		END) AS YEAR_CLOSE
	,CASE
		WHEN SUM(CASE
					WHEN dtDate = @quarterpriordate
					THEN decAdjClose
					ELSE 0
				END) = 0
		THEN 0
		ELSE SUM(CASE
					WHEN dtDate = @pulldate
					THEN decAdjClose
					ELSE 0
				END)
			/SUM(CASE
					WHEN dtDate = @quarterpriordate
					THEN decAdjClose
					ELSE 0
				END)
	END AS QTR_PCT
	,CASE
		WHEN SUM(CASE
					WHEN dtDate = @halfyearpriordate
					THEN decAdjClose
					ELSE 0
				END) = 0
		THEN 0
		ELSE SUM(CASE
					WHEN dtDate = @pulldate
					THEN decAdjClose
					ELSE 0
				END)
			/SUM(CASE
					WHEN dtDate = @halfyearpriordate
					THEN decAdjClose
					ELSE 0
				END)
	END AS HALF_PCT
	,CASE
		WHEN SUM(CASE
					WHEN dtDate = @yearpriordate
					THEN decAdjClose
					ELSE 0
				END) = 0
		THEN 0
		ELSE SUM(CASE
					WHEN dtDate = @pulldate
					THEN decAdjClose
					ELSE 0
				END)
			/SUM(CASE
					WHEN dtDate = @yearpriordate
					THEN decAdjClose
					ELSE 0
				END)
	END AS YEAR_PCT
FROM
	STOCK_DATA
GROUP BY
	strTick
ORDER BY
	CASE
		WHEN SUM(CASE
					WHEN dtDate = @yearpriordate
					THEN decAdjClose
					ELSE 0
				END) = 0
		THEN 0
		ELSE SUM(CASE
					WHEN dtDate = @pulldate
					THEN decAdjClose
					ELSE 0
				END)
			/SUM(CASE
					WHEN dtDate = @yearpriordate
					THEN decAdjClose
					ELSE 0
				END)
	END DESC
	,CASE
		WHEN SUM(CASE
					WHEN dtDate = @halfyearpriordate
					THEN decAdjClose
					ELSE 0
				END) = 0
		THEN 0
		ELSE SUM(CASE
					WHEN dtDate = @pulldate
					THEN decAdjClose
					ELSE 0
				END)
			/SUM(CASE
					WHEN dtDate = @halfyearpriordate
					THEN decAdjClose
					ELSE 0
				END)
	END DESC
	,CASE
		WHEN SUM(CASE
					WHEN dtDate = @quarterpriordate
					THEN decAdjClose
					ELSE 0
				END) = 0
		THEN 0
		ELSE SUM(CASE
					WHEN dtDate = @pulldate
					THEN decAdjClose
					ELSE 0
				END)
			/SUM(CASE
					WHEN dtDate = @quarterpriordate
					THEN decAdjClose
					ELSE 0
				END)
	END DESC



