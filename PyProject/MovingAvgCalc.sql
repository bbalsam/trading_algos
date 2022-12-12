use stockdb
DECLARE @Amount INT
SET @Amount = 360

SELECT	val.strTick as Tick,
		val.dtDate as TriggerDate,
		CASE
		WHEN hol.strTick is null
			THEN CAST(CAST(GETDATE() as date) as varchar)
		WHEN hol.decCostBasis = 0
			THEN 'PASS'		
		ELSE 'REPEAT'
		END as DatePurchased,
		CASE
			WHEN hol.strTick is null
				THEN cast(cast(dateadd(day,14,getdate()) as date) as varchar)
			WHEN hol.decCostBasis = 0
				THEN 'PASS'		
		ELSE 'REPEAT'
		END as ProposedSaleDate,
		CASE
			WHEN hol.strTick is null
				THEN CAST(ROUND((@amount / decAdjClose),0) as int)	
		ELSE 0
		END as Shares,
		decAdjClose as CostBasis

FROM	(
		SELECT	com.strTick,com.dtDate,com.decAdjClose,com.MA05,MA10,MA20,MA50,
				KFAST,
				AVG(com.KFAST) OVER (ORDER BY com.strtick asc, com.dtDate ASC ROWS 2 PRECEDING) AS KSLOW
		FROM
					(SELECT	strtick,
							dtDate,
							decAdjClose,
							AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 4 PRECEDING) AS MA05,
							AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 9 PRECEDING) AS MA10,
							AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 19 PRECEDING) AS MA20,
							AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 49 PRECEDING) AS MA50,
							MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) AS L14,
							MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) AS H14,
							CASE
							WHEN MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) = MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING)
							THEN NULL
							ELSE
							(100 * (decAdjClose-MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING))) / (MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING)-MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING))
							END as KFAST
					FROM	snp500_test
					WHERE	dtDate >= DATEADD(DAY,-365,GETDATE())
					) com
				) val
LEFT JOIN	Holdings hol ON hol.strTick = val.strTick
WHERE	dtDate = (select MAX(dtdate) from snp500_test)
		AND (val.KFAST < 20 AND val.KSLOW < val.KFAST)

/*
Current price

select strTick,decAdjClose,dtDate
from snp500_test
WHERE dtDate = (select max(dtDate) from snp500_test)
		and strTick in ('DVA','EVRG','FFIV','FLIR','FTNT','GILD','NEM','ORCL','OXY','RMD','AMT','ANET','CMS','IFF','IPGP','LLY','MYL','AEP','SYK','AIG','CDW','EIX','FFIV','HII','NTAP','PNW','ANET','BXP','CDW','CSCO','DGX','NTAP','ANET','AVY','CMS','CSCO','DGX','DRE','ES','ESS','ETR','EXC','FFIV','FIS','GILD','GPN','HPE','IPGP','MTB','PKI','PNC','SWKS','WRB','XEL','XLNX','XRAY','APA','ARE','ATO','CMS','CVS','DUK','DVN','EOG','ETR','EVRG','FANG','FE','FIS','FISV','FLIR','HES','HII','HSIC','INFO','IPGP','J','JNPR','MLM','MU','NI','OKE','OXY','PNW','VAR','WBA','WDC','XLNX','ZBRA','ABC','APA','BDX','BIIB','CAH','CDW','CNC','DGX','EFX','EOG','EVRG','FLIR','HES','HOLX','ILMN','IPGP','IQV','JKHY','JNPR','LH','LLY','LRCX','MLM','MMC','OKE','OXY','PAYC','PEG','STE','TJX','TROW','VRSK','WDC','AMT','BIIB','CCI','CLX','CSCO','DGX','HOLX','HSIC','JKHY','LRCX','PCAR','ROP','STE','WDC','ABC','AEE','BIIB','CLX','CMS','CNC','COP','CSCO','EIX','ES','ETR','FE','FTV','HII','HRL','KMB','KMI','NEE','NWL','OKE','PNW','REG','SO','SRE','UNH','VMC','WBA','XEL'

)
GROUP BY strTick,decAdjClose,dtDate
order by strTick asc
*/






;

