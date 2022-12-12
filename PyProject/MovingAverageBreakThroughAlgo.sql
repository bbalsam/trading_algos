use stockdb
DECLARE @Amount INT
SET @Amount = 360
DECLARE @KFAST INT
SET @KFAST = 20
DECLARE @SampleDate date
SET @SampleDate = '2019-09-01'

SELECT	/*@KFAST as KFAST,*/
		val.strTick as Tick,
		val.dtDate as TriggerDate,
		LeadOpen as CostBasis,
		MaxGain,
		MinGain,
		MaxGain/LeadOpen as MaxPNL,
		MinGain/LeadOpen as MinPNL,
		LeadClose00,
		LeadClose01,
		LeadClose02,
		LeadClose03,
		LeadClose04,
		LeadClose05,
		LeadClose06,
		LeadClose07,
		LeadClose08,
		LeadClose09,
		LeadClose10,
		LeadClose11,
		LeadClose12,
		LeadClose13,
		LeadClose14,
		LeadClose15,
		LeadClose16,
		LeadClose17,
		LeadClose18,
		LeadClose19,
		LeadClose20,
		LeadClose21,
		LeadClose00/LeadOpen as PNL00,
		LeadClose01/LeadOpen as PNL01,
		LeadClose02/LeadOpen as PNL02,
		LeadClose03/LeadOpen as PNL03,
		LeadClose04/LeadOpen as PNL04,
		LeadClose05/LeadOpen as PNL05,
		LeadClose06/LeadOpen as PNL06,
		LeadClose07/LeadOpen as PNL07,
		LeadClose08/LeadOpen as PNL08,
		LeadClose09/LeadOpen as PNL09,
		LeadClose10/LeadOpen as PNL10,
		LeadClose11/LeadOpen as PNL11,
		LeadClose12/LeadOpen as PNL12,
		LeadClose13/LeadOpen as PNL13,
		LeadClose14/LeadOpen as PNL14,
		LeadClose15/LeadOpen as PNL15,
		LeadClose16/LeadOpen as PNL16,
		LeadClose17/LeadOpen as PNL17,
		LeadClose18/LeadOpen as PNL18,
		LeadClose19/LeadOpen as PNL19,
		LeadClose20/LeadOpen as PNL20,
		LeadClose21/LeadOpen as PNL21


FROM	(
		SELECT	com.strTick,com.dtDate,
				com.decAdjClose,
				MA50,
				LeadOpen,
				LeadClose00,
				LeadClose01,
				LeadClose02,
				LeadClose03,
				LeadClose04,
				LeadClose05,
				LeadClose06,
				LeadClose07,
				LeadClose08,
				LeadClose09,
				LeadClose10,
				LeadClose11,
				LeadClose12,
				LeadClose13,
				LeadClose14,
				LeadClose15,
				LeadClose16,
				LeadClose17,
				LeadClose18,
				LeadClose19,
				LeadClose20,
				LeadClose21,
				LagClose,
				MinGain,
				MaxGain
				,
				KFAST,
				AVG(com.KFAST) OVER (ORDER BY com.strtick asc, com.dtDate ASC ROWS 2 PRECEDING) AS KSLOW
		FROM
					(SELECT	strtick,
							dtDate,
							decAdjClose,
							CAST(LAG(decOpen,1) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LagClose,
							CAST(LEAD(decOpen,1) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadOpen,
							CAST(decAdjClose  AS decimal(10,6)) AS LeadClose00,
							CAST(LEAD(decAdjClose,1) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose01,
							CAST(LEAD(decAdjClose,2) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose02,
							CAST(LEAD(decAdjClose,3) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose03,
							CAST(LEAD(decAdjClose,4) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose04,
							CAST(LEAD(decAdjClose,5) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose05,
							CAST(LEAD(decAdjClose,6) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose06,
							CAST(LEAD(decAdjClose,7) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose07,
							CAST(LEAD(decAdjClose,8) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose08,
							CAST(LEAD(decAdjClose,9) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose09,
							CAST(LEAD(decAdjClose,10) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose10,
							CAST(LEAD(decAdjClose,11) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose11,
							CAST(LEAD(decAdjClose,12) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose12,
							CAST(LEAD(decAdjClose,13) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose13,
							CAST(LEAD(decAdjClose,14) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose14,
							CAST(LEAD(decAdjClose,15) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose15,
							CAST(LEAD(decAdjClose,16) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose16,
							CAST(LEAD(decAdjClose,17) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose17,
							CAST(LEAD(decAdjClose,18) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose18,
							CAST(LEAD(decAdjClose,19) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose19,
							CAST(LEAD(decAdjClose,20) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose20,
							CAST(LEAD(decAdjClose,21) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose21,
/*MIN*/						MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS BETWEEN CURRENT ROW AND 
							20 FOLLOWING) AS MinGain,
/*MAX*/						MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS BETWEEN CURRENT ROW AND 
							20 FOLLOWING) AS MaxGain,
/*MA*/						AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 
							49 PRECEDING) AS MA50
,
							MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) AS L14,
							MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) AS H14,
							CASE
							WHEN MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) = MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING)
							THEN NULL
							ELSE
							(100 * (decAdjClose-MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING))) / (MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING)-MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING))
							END as KFAST

					FROM	snp500_test
					WHERE	dtDate >= DATEADD(DAY,-80,@SampleDate)
					) com
				) val
WHERE	dtDate between @SampleDate and dateadd(day,314,@SampleDate) /*= @SampleDate*//*(select MAX(dtdate) from snp500_test)*/
		AND (val.KFAST < @KFAST AND (val.decAdjClose > val.MA50 AND val.LagClose < val.MA50))
order by dtDate ASC
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






/*
DECLARE @SampleDate date
SET @SampleDate = '9/4/2019'


select * from snp500_test
where strTick = 'JWN' and dtDate between @SampleDate and dateadd(day,344,@SampleDate)
*/