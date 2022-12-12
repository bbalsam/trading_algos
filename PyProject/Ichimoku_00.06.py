#=========================================================================================================================
#VERSIONS
#Ichimoku_00.01.py..............................Initial version
#Ichimoku_00.02.py..............................Fixes for original
#Ichimoku_00.03.py..............................Addition of KellyCriterion and first publication
#Ichimoku_00.04.py..............................Major rework.  Changed to median values, added ATR and Bollinger. Corresponds to Ichimoku Cloud_4.sql
#Ichimoku_00.05.py..............................Added Engulfing pattern to second tab
#Ichimoku_00.06.py..............................Added logic to only drop tables if tables exist.

#=========================================================================================================================
environment = 'PROD'

if environment == 'TEST':
	print('Environment is in: ' + environment)
	emailto = ''
else:
	print('Environment is in: ' + environment)
	emailto = 'bjaddie@gmail.com'



import pyodbc
import pandas as pd
import xlwt
import datetime


date = datetime.date.today()
year = str(date.year)
month = str(date.month)

while len(month) < 2:
	month = '0' + month

day = str(date.day)

while len(day) < 2:
	day = '0' + day

filelocation = str(r'C:\Users\brett\OneDrive\Documents\Excel\Trading\PyProject\DailyReports\\')

filename = str(year + month + day + '_RECOMMENDATIONS_' + environment + '.xlsx')

file = filelocation + filename

subject = 'Daily stock recommendations for ' + year + '/' + month + '/' + day

writer = pd.ExcelWriter(file,engine='xlsxwriter',options={'strings_to_numbers': True})

try:
	cnxn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='LAPTOP-D6TKOBQR\SQLEXPRESS',DATABASE='stockdb',Trusted_connection='yes')
	cursor = cnxn.cursor()
	#Create ICHIMOKU_STAGE
	script = """
	IF OBJECT_ID('ICHIMOKU_STAGE', 'u') IS NOT NULL
	BEGIN
	DROP TABLE ICHIMOKU_STAGE
	END
	SELECT --TOP 1000
			dtdate,
			strTick,
			row_number() over (partition by strTick order by dtdate) as QuoteID,
			decAdjClose,
			decClose,
			decOpen,
			decHigh,
			decLow,
			(min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING))/2 as CL,
			(min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING))/2 as BL,
			(((min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING))/2)+((min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING))/2))/2 as SPAN_A,
			(min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 51 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 51 PRECEDING))/2 as SPAN_B
	INTO ICHIMOKU_STAGE
	FROM snp500_test
	WHERE dtdate >= dateadd(day,-2900,getdate())
	ORDER BY strTick,dtDate ASC
	"""

	cursor.execute(script)
	cursor.commit()
	print('ICHIMOKU_STAGE completed')
	
	#Create ICHIMOKU
	script = """
	IF OBJECT_ID('ICHIMOKU', 'u') IS NOT NULL BEGIN
	DROP TABLE ICHIMOKU
	END
	/*delete from Ichimoku
	INSERT INTO Ichimoku (strTick,dtBuyDate,decBuyCost)*/
	SELECT row_number() OVER (ORDER BY strtick,dtdate) as ID,strTick,BuyDate as dtBuyDate,BuyPrice as decBuyCost,BuyPrice,
			QuoteID,decAdjClose,decClose,decOpen,decHigh,decLow
	INTO ICHIMOKU
	FROM
	(select StrTick,
			CASE	WHEN	SPAN_A > SPAN_B
					THEN	1
					ELSE	0
			END AS BUY1,
			CASE	WHEN	decAdjClose > (LAG(SPAN_A,26) OVER (partition by strTick order by dtdate ASC))
					AND		decAdjClose > (LAG(SPAN_B,26) OVER (partition by strTick order by dtdate ASC))
					THEN	1
					ELSE	0
			END AS BUY2,
			CASE	WHEN	CL > BL
					THEN	1
					ELSE	0
			END AS BUY3,
			CASE	WHEN	decAdjClose > (LAG(SPAN_A,52) OVER (partition by strTick order by dtdate ASC))
					AND		decAdjClose > (LAG(SPAN_B,52) OVER (partition by strTick order by dtdate ASC))
					THEN	1
					ELSE	0
			END AS BUY4,
			CASE	WHEN	decAdjClose > (AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 4 PRECEDING))
					THEN	1
					ELSE	0
			END AS BUY5,
			dtdate,
			dtdate as BuyDate,
			LEAD(decOpen,1) OVER (partition by strTick Order by dtDate asc) as BuyPrice,
			QuoteID,
			decAdjClose,
			decClose,
			decOpen,
			decHigh,
			decLow

	from ICHIMOKU_STAGE
	) IBT
	WHERE IBT.buy1 = 1 AND IBT.buy2 = 1 AND IBT.buy3 = 1 AND IBT.buy4 = 1 AND IBT.BUY5 = 1
	"""

	cursor.execute(script)
	cursor.commit()
	print('ICHIMOKU completed')	
	
	#Create IchimokuBacktestBUY
	script = """
	IF OBJECT_ID('IchimokuBacktestBUY', 'u') IS NOT NULL BEGIN
	DROP TABLE IchimokuBacktestBUY
	END
	Select *
	into IchimokuBacktestBUY
	FROM
	(SELECT ID,strTick,BuyDate,decBuyCost,QuoteID AS BUYID,
			CASE WHEN BuyDate IS NOT NULL
				THEN LEAD(QuoteID,1) OVER (partition by strTick Order by ID)
				ELSE NULL
			END AS SELLID,
			CASE WHEN BuyDate IS NOT NULL
				THEN LEAD(SellDate,1) OVER (partition by strTick Order by ID)
				ELSE NULL
			END AS SellDate,
			CASE WHEN decBuyCost IS NOT NULL
				THEN LEAD(SellCost,1) OVER (partition by strTick Order by ID)
				ELSE NULL
			END AS SellCost,
			decAdjClose,decClose,decOpen,decHigh,decLow,NULL as PeriodLow,NULL as PeriodHigh
	FROM
	(
	SELECT ichi.ID,
			QuoteID,
			ichi.strTick,
			CASE /*BuyDate*/
				WHEN smpl.BUYSELLSTATUS = 'BUY'
				THEN ichi.dtBuyDate
				ELSE NULL
			END as BuyDate,
			CASE /*decBuyCost*/
				WHEN smpl.BUYSELLSTATUS = 'BUY'
				THEN ichi.decBuyCost
				ELSE NULL
			END as decBuyCost,
			CASE /*SellDate*/
				WHEN smpl.BUYSELLSTATUS = 'SELL'
				THEN ichi.dtBuyDate
				ELSE NULL
			END as SellDate,
			CASE  /*SellCost*/
				WHEN smpl.BUYSELLSTATUS = 'SELL'
				THEN  ichi.decBuyCost
				ELSE NULL
			END as SellCost,
			decAdjClose,decClose,decOpen,decHigh,decLow
	from Ichimoku ichi
	JOIN (

		SELECT	ID,
				strTick,
				dtBuyDate,
				LEAD(dtBuyDate,1) OVER (partition by strTick ORDER BY dtBuyDate asc) as NextDt,
				LAG(dtBuyDate,1) OVER (partition by strTick ORDER BY dtBuyDate asc) as PrevDate,
				CASE
					WHEN	DATEDIFF(day, dtBuyDate, LEAD(dtBuyDate,1) OVER (partition by strTick ORDER BY dtBuyDate asc))>5
						THEN 'SELL'
					WHEN	DATEDIFF(day, dtBuyDate, LAG(dtBuyDate,1) OVER (partition by strTick ORDER BY dtBuyDate asc))<-5
							OR	DATEDIFF(day, dtBuyDate, LAG(dtBuyDate,1) OVER (partition by strTick ORDER BY dtBuyDate asc)) IS NULL
						THEN 'BUY'
					ELSE 'HOLD'
				END AS BUYSELLSTATUS
		FROM	Ichimoku
		) smpl ON smpl.ID = ichi.ID
	WHERE smpl.BUYSELLSTATUS <> 'HOLD'
	) bs) fin
	WHERE fin.buyDate is not null
	"""
	
	cursor.execute(script)
	cursor.commit()
	print('IchimokuBacktestBUY completed')		
	
	
	#Create BOLLINGER
	script = """
	IF OBJECT_ID('BOLLINGER', 'u') IS NOT NULL BEGIN
	DROP TABLE BOLLINGER
	END

	SELECT	ID,
			strTick,
			dtDate,
			CAST(decOpen as decimal(10,2)) AS decOpen,
			CAST(decHigh as decimal(10,2)) AS decHigh,
			CAST(decLow as decimal(10,2)) AS decLow,
			CAST(decAdjClose as decimal(10,2)) AS decClose,
			CAST(decAdjClose as decimal(10,2)) AS decAdjClose,
			CAST(LEAD(decOpen,1) OVER (PARTITION BY strTick ORDER BY dtDate ASC) as decimal(10,2)) as NextOpen,
			CAST(AVG(decAdjClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC ROWS 20 PRECEDING) as decimal(10,2)) as SMA21,
			CAST(STDEV(decAdjClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC ROWS 20 PRECEDING)*2 as decimal(10,2)) as BollBand21
	INTO BOLLINGER
	FROM snp500_test
	WHERE dtdate >= '01/01/2000'	
	"""
	
	cursor.execute(script)
	cursor.commit()
	print('BOLLINGER completed')		
	
	#Create ATR
	script = """
	IF OBJECT_ID('tempdb..#Stock_ListATR') IS NOT NULL BEGIN
	DROP TABLE #Stock_ListATR
	END
	SELECT	ID,
			dtDate,
			strTick,
			decHigh,
			decLow,
			decOpen,
			decAdjClose,
			row_number() over (partition by strTick order by dtdate) as QuoteID
	INTO	#Stock_listATR
	FROM	snp500_test
	WHERE dtdate >= '1/1/2010'
	ORDER BY dtDate
	/*=========================*/
	IF OBJECT_ID('tempdb..#TR_CALC') IS NOT NULL BEGIN
	DROP TABLE #TR_CALC
	END
	SELECT *,
			CASE WHEN	QuoteID = 1
				THEN	decHigh - decLow
				WHEN	(decHigh - decLow) > ABS(decHigh - LAG(decAdjClose,1) OVER (partition by strtick ORDER BY dtdate asc))
						AND (decHigh - decLow) > ABS(decLow - LAG(decAdjClose,1) OVER (partition by strtick ORDER BY dtdate asc))
				THEN	decHigh - decLow
				WHEN	ABS(decHigh - LAG(decAdjClose,1) OVER (partition by strtick ORDER BY dtdate asc)) > ABS(decLow - LAG(decAdjClose,1) OVER (partition by strtick ORDER BY dtdate asc))
				THEN	ABS(decHigh - LAG(decAdjClose,1) OVER (partition by strtick ORDER BY dtdate asc))
				ELSE ABS(decLow - LAG(decAdjClose,1) OVER (partition by strtick ORDER BY dtdate asc))
			END as TR
	INTO #TR_CALC
	FROM #Stock_listATR
	/*=========================*/
	/*====================================================================
		Step 2: ESTABLISH WILDER'S EMA

			The standard exponential moving average formula converts the
			time period to a fraction using the formula EMA% = 2/(n + 1)
			where n is the number of days. For example, the EMA% for 14
			days is 2/(14 days +1) = 13.3%. Wilder, however, uses an EMA%
			of 1/14 which equals 7.1%. This equates to a 27-day exponential
			moving average using the standard formula.
	====================================================================*/

	IF OBJECT_ID('tempdb..#TBL_ATR14_RT') IS NOT NULL BEGIN
	DROP TABLE #TBL_ATR14_RT
	END
	 
	SELECT	*,
			CAST(NULL AS FLOAT) AS ATR14
	INTO	#TBL_ATR14_RT
	FROM	#TR_CALC
	/*=========================*/
	CREATE UNIQUE CLUSTERED INDEX ATR14_IDX_RT ON #TBL_ATR14_RT (strTick, QuoteId)
	/*=========================*/
	IF OBJECT_ID('tempdb..#TBL_START_AVG14') IS NOT NULL BEGIN
	DROP TABLE #TBL_START_AVG14
	END
	/*=========================*/ 
	SELECT		strTick,
				AVG(TR) AS Start_Avg
	INTO		#TBL_START_AVG14
	FROM		#TR_CALC
	WHERE		QuoteId <= 14
	GROUP BY	strTick
	/*=========================*/
	DECLARE @C14 float = 1.0 / 14, @ATR14 float
	/*=========================*/

	UPDATE
		T114
	SET
		@ATR14 =
			CASE
				WHEN T114.QuoteId = 14 THEN T214.Start_Avg
				WHEN T114.QuoteId > 14 THEN (T114.TR * @C14) + (@ATR14 * (1 - @C14))
			END
		,ATR14 = @ATR14 
	FROM
		#TBL_ATR14_RT T114
	JOIN
		#TBL_START_AVG14 T214
			ON T114.strTick = T214.strTick
	option (maxrecursion 0);

	/*=========================*/

	IF OBJECT_ID('ATR') IS NOT NULL BEGIN
	DROP TABLE ATR
	END
	SELECT	rt2.strTick,
			rt2.QuoteId as ID,
			rt2.dtDate,
			rt2.decAdjClose,
			rt2.decOpen,
			rt2.decHigh,
			rt2.decLow,
			CAST(ATR14 AS NUMERIC(10,2)) AS ATR14
	INTO ATR
	FROM #TBL_ATR14_RT rt2
	"""
	
	cursor.execute(script)
	cursor.commit()
	print('ATR completed')		
	
	#Create IchimokuLookBack
	script = """
	declare @daysback as int
	set @daysback = -3653

	IF OBJECT_ID('IchimokuLookBack') IS NOT NULL BEGIN
	DROP TABLE IchimokuLookBack
	END

	select ibuy.ID,ibuy.strTick,ibuy.BuyDate,ibuy.decBuyCost,ibuy.SellDate,ibuy.SellCost,
				max(snp.decHigh) as PeriodHigh,
				min(snp.decLow) as PeriodLow,
				Sellcost/decbuycost as PCTPNL,
				max(snp.decHigh)/ibuy.decBuyCost as PeriodHighPCTPNL,
				min(snp.decLow)/ibuy.decBuyCost as PeriodLowPCTPNL,
				SELLID-BUYID as DaysBetween
	INTO IchimokuLookBack
	from IchimokuBacktestBUY ibuy
	JOIN snp500_test snp ON snp.strTick = ibuy.strTick
							AND (snp.dtDate >= ibuy.BuyDate
								AND snp.dtDate <= ibuy.SellDate)
	where 	BuyDate >= (select dateadd(day,@daysback,max(buydate)) from IchimokuBacktestBUY)
	GROUP BY ibuy.ID,ibuy.strTick,ibuy.BuyDate,ibuy.decBuyCost,ibuy.SellDate,ibuy.SellCost,BUYID,SELLID
	ORDER BY ibuy.ID asc
	"""

	cursor.execute(script)
	cursor.commit()
	print('IchimokuLookBack completed')		
	
	#Create IchimokuLookBack
	script = """
	IF OBJECT_ID('IchimokuPerformance') IS NOT NULL BEGIN
	DROP TABLE IchimokuPerformance
	END
	select strTick,
			FORMAT(CASE WHEN avg(MEDIANPNLGain) IS NULL THEN (1-(avg(MEDIANPNLLoss)-1))-1 ELSE avg(MEDIANPNLGain)-1 END, 'P6') as MED_PNL_Gain,
			FORMAT(CASE WHEN avg(MEDIANPNLLoss) IS NULL THEN (1-(avg(MEDIANPNLGain)-1))-1 ELSE avg(MEDIANPNLLoss)-1 END, 'P6') as MED_PNL_Loss,
			FORMAT(CASE WHEN avg(MEDIANPNLDailyGain) IS NULL THEN (1-(avg(MEDIANPNLDailyLoss)-1))-1 ELSE avg(MEDIANPNLDailyGain)-1 END, 'P6') as MED_PNL_DailyGain,
			FORMAT(CASE WHEN avg(MEDIANPNLDailyLoss) IS NULL THEN (1-(avg(MEDIANPNLDailyGain)-1))-1 ELSE avg(MEDIANPNLDailyLoss)-1 END, 'P6') as MED_PNL_DailyLoss,
			CASE WHEN avg(MEDIANDaysToGain) IS NULL THEN 7 ELSE avg(MEDIANDaysToGain) END as MED_DaysToGain,
			CASE WHEN avg(MEDIANDaysToLoss) IS NULL THEN 2 ELSE avg(MEDIANDaysToLoss) END as MED_DaysToLoss,
			SUM(Gain) as Wins,
			SUM(Loss) as Losses,
			FORMAT(CASE WHEN SUM(Gain+0.0)/(SUM(Gain+0.0) + SUM(Loss+0.0)) = 1 THEN ROUND(SUM(Gain)/(SUM(Gain) + SUM(Loss)+1.0),3) ELSE ROUND(SUM(Gain)/(SUM(Gain) + SUM(Loss)+0.0),3) END, 'P2') AS WinPCT,
			ROUND(AVG(MEDIANATR),3) MED_ATR,
			ROUND(AVG(MEDIANBOLL),3) MED_BOLL,
			100 *(((1+(CASE WHEN avg(MEDIANPNLGain) IS NULL THEN (1-(avg(MEDIANPNLLoss)-1))-1 ELSE avg(MEDIANPNLGain)-1 END))*(CASE WHEN SUM(Gain+0.0)/(SUM(Gain+0.0) + SUM(Loss+0.0)) = 1 THEN ROUND(SUM(Gain)/(SUM(Gain) + SUM(Loss)+1.0),3) ELSE ROUND(SUM(Gain)/(SUM(Gain) + SUM(Loss)+0.0),3) END))-(1-(CASE WHEN SUM(Gain+0.0)/(SUM(Gain+0.0) + SUM(Loss+0.0)) = 1 THEN ROUND(SUM(Gain)/(SUM(Gain) + SUM(Loss)+1.0),3) ELSE ROUND(SUM(Gain)/(SUM(Gain) + SUM(Loss)+0.0),3) END))/(1+(CASE WHEN avg(MEDIANPNLGain) IS NULL THEN (1-(avg(MEDIANPNLLoss)-1))-1 ELSE avg(MEDIANPNLGain)-1 END))) as KC,
			100 *(((1+(CASE WHEN avg(MEDIANPNLDailyGain) IS NULL THEN (1-(avg(MEDIANPNLDailyLoss)-1))-1 ELSE avg(MEDIANPNLDailyGain)-1 END))*(CASE WHEN SUM(Gain+0.0)/(SUM(Gain+0.0) + SUM(Loss+0.0)) = 1 THEN ROUND(SUM(Gain)/(SUM(Gain) + SUM(Loss)+1.0),3) ELSE ROUND(SUM(Gain)/(SUM(Gain) + SUM(Loss)+0.0),3) END))-(1-(CASE WHEN SUM(Gain+0.0)/(SUM(Gain+0.0) + SUM(Loss+0.0)) = 1 THEN ROUND(SUM(Gain)/(SUM(Gain) + SUM(Loss)+1.0),3) ELSE ROUND(SUM(Gain)/(SUM(Gain) + SUM(Loss)+0.0),3) END))/(1+(CASE WHEN avg(MEDIANPNLDailyGain) IS NULL THEN (1-(avg(MEDIANPNLDailyLoss)-1))-1 ELSE avg(MEDIANPNLDailyGain)-1 END))) as KC_Daily
	INTO IchimokuPerformance
	FROM(
	SELECT	strTick,
			PERCENTILE_CONT(0.5) 
			 WITHIN GROUP (ORDER BY PNLGain) OVER ( 
			   PARTITION BY strtick) AS MEDIANPNLGain,
			PERCENTILE_CONT(0.5) 
			 WITHIN GROUP (ORDER BY PNLLoss) OVER ( 
			   PARTITION BY strtick) AS MEDIANPNLLoss,
			PERCENTILE_CONT(0.5) 
			 WITHIN GROUP (ORDER BY PNLDailyGain) OVER ( 
			   PARTITION BY strtick) AS MEDIANPNLDailyGain,
			PERCENTILE_CONT(0.5) 
			 WITHIN GROUP (ORDER BY PNLDailyLoss) OVER ( 
			   PARTITION BY strtick) AS MEDIANPNLDailyLoss,
			PERCENTILE_CONT(0.5) 
			 WITHIN GROUP (ORDER BY DaysToGain) OVER ( 
			   PARTITION BY strtick) AS MEDIANDaysToGain,
			PERCENTILE_CONT(0.5) 
			 WITHIN GROUP (ORDER BY DaysToLoss) OVER ( 
			   PARTITION BY strtick) AS MEDIANDaysToLoss,
			PERCENTILE_CONT(0.5) 
			 WITHIN GROUP (ORDER BY ATR14) OVER ( 
			   PARTITION BY strtick) AS MEDIANATR,
			PERCENTILE_CONT(0.5) 
			 WITHIN GROUP (ORDER BY BollBand21) OVER ( 
			 PARTITION BY strtick) AS MEDIANBOLL,
			Gain,
			Loss
	FROM(
	SELECT	ilb.strtick,
			BuyDate,
			CASE WHEN sellcost/decbuycost > 1 THEN sellcost/decbuycost ELSE NULL END AS PNLGain,
			CASE WHEN sellcost/decbuycost < 1 THEN sellcost/decbuycost ELSE NULL END AS PNLLoss,
			CASE WHEN sellcost/decbuycost > 1 THEN POWER(1.0*sellcost/decbuycost,(1.0/DaysBetween)) ELSE NULL END AS PNLDailyGain,
			CASE WHEN sellcost/decbuycost < 1 THEN POWER(1.0*sellcost/decbuycost,(1.0/DaysBetween)) ELSE NULL END AS PNLDailyLoss,
			CASE WHEN sellcost/decbuycost > 1 THEN DaysBetween ELSE NULL END AS DaysToGain,
			CASE WHEN sellcost/decbuycost < 1 THEN DaysBetween ELSE NULL END AS DaysToLoss,
			CASE WHEN sellcost/decbuycost > 1 THEN 1 ELSE 0 END AS Gain,
			CASE WHEN sellcost/decbuycost < 1 THEN 1 ELSE 0 END AS Loss,
			atr.ATR14,
			bol.BollBand21
	FROM IchimokuLookBack ilb
	JOIN	ATR			atr	ON	atr.strTick	=	ilb.strTick	AND	atr.dtDate	=	ilb.BuyDate
	JOIN	BOLLINGER	bol	ON	bol.strTick	=	ilb.strTick	AND	bol.dtDate	=	ilb.BuyDate
	GROUP BY ilb.strTick,BuyDate,SellCost,decBuyCost,DaysBetween,atr.ATR14,bol.BollBand21
	) ichi
	GROUP BY strtick,PNLGain,PNLLoss,PNLDailyGain,PNLDailyLoss,DaysToGain,DaysToLoss,ATR14,BollBand21,Gain,Loss
	) roll
	GROUP BY strtick
	"""
	
	cursor.execute(script)
	cursor.commit()
	print('IchimokuPerformance completed')		
	
	#Create RECOMMENDATIONS
	script = """
	declare @daysback as int

	set @daysback = 0

	SELECT	ibuy.strTick,
			cl.strCompanyName,
			sl.strSectorName,
			perf.MED_PNL_Gain,
			perf.MED_PNL_Loss,
			perf.MED_PNL_DailyGain,
			perf.MED_PNL_DailyLoss,
			perf.MED_DaysToGain,
			perf.MED_DaysToLoss,
			perf.Wins,
			perf.Losses,
			perf.WinPCT,
			perf.MED_ATR,
			atr.ATR14 as CurrentATR,
			perf.MED_BOLL as MED_BOLL2STD,
			bol.BollBand21 as CurrentBoll2STD,
			ibuy.decClose,
			ROUND(perf.KC,2) as KC,
			ROUND(perf.KC_Daily,2) as KC_Daily
	FROM		IchimokuBacktestBUY	ibuy
	JOIN		ATR					atr		ON	atr.strTick		=	ibuy.strTick AND atr.dtDate = ibuy.BuyDate
	JOIN		BOLLINGER			bol		ON	bol.strTick		=	ibuy.strTick AND bol.dtDate = ibuy.BuyDate
	LEFT JOIN	CompanyList			cl		ON	cl.strTick		=	ibuy.strTick
	LEFT JOIN	SectorList			sl		ON	sl.ID			=	cl.SectorListID
	JOIN		IchimokuPerformance	perf	ON	perf.strTick	=	ibuy.strTick
	WHERE		BuyDate		=	(SELECT	dateadd(day,@daysback,max(buydate))
								FROM	IchimokuBacktestBUY
								)
				OR	SellDate =	(SELECT dateadd(day,@daysback,max(buydate))
								FROM IchimokuBacktestBUY
								)
	ORDER BY KC_Daily desc	
	"""

	cursor.execute(script)
	print('Ichimoku recs completed')	
	
	columns = [desc[0] for desc in cursor.description]
	data = cursor.fetchall()
	df1 = pd.DataFrame.from_records(data=data,columns=columns)
	df1.to_excel(writer, sheet_name=str('IchimokuRecs'),startcol=0,index=False)

	#Create ENGULFING RECS
	script = """
			select	snp.strtick,
					snp.dtdate,
					decClose,
					'=RTD("tos.rtd",,"OPEN",A2)' as CurOpen,
					'=RTD("tos.rtd",,"LAST",A2)' as CurClose
			FROM (
			select	*,
					LAG(decOpen,1) OVER (partition by strtick order by dtdate) as PrevOpen,
					LAG(decClose,1) OVER (partition by strtick order by dtdate) as PrevClose,
					LAG(intVol,1) OVER (partition by strtick order by dtdate) as PrevVol,
					LEAD(decOpen,1) OVER (partition by strtick order by dtdate) as Buy,
					LEAD(decClose,1) OVER (partition by strtick order by dtdate) as Sell
			from snp500_test
			) snp
			where (snp.decOpen < PrevClose and snp.decClose > PrevOpen and PrevClose < PrevOpen and snp.decClose > snp.decOpen)
			AND dtdate  = (select MAX(dtdate) from snp500_test)
			"""

	cursor.execute(script)
	print('Engulfing recs completed')	
	
	columns = [desc[0] for desc in cursor.description]
	data = cursor.fetchall()
	df2 = pd.DataFrame.from_records(data=data,columns=columns)
	df2.to_excel(writer, sheet_name=str('EngulfingRecs'),startcol=0,index=False)

	writer.save()
	print('File saved')	
	#writer.close()

	# Email	source: https://gist.github.com/ITSecMedia/b45d21224c4ea16bf4a72e2a03f741af
	import win32com.client
	from win32com.client import Dispatch, constants

	const=win32com.client.constants
	olMailItem = 0x0
	obj = win32com.client.Dispatch("Outlook.Application")
	newMail = obj.CreateItem(olMailItem)
	newMail.Subject = subject
	newMail.BodyFormat = 2 # olFormatHTML https://msdn.microsoft.com/en-us/library/office/aa219371(v=office.11).aspx
	newMail.HTMLBody = """<html><body><span style="font-family: 'Calibri Light', sans-serif;">
	<p>Brandon,</p>
	<p>See attached daily recommendations report.</p>
	<p>Thanks,</p>
	<p><b>Brett Balsam</b><br></p>
	</span><span style="font-size:14px; color: gray; font-family: 'Calibri Light', sans-serif;">	14809 Carriage Pl. Dr. | Burnsville, MN  55306<br>
	c) 612-419-1544<br>
	<a href="brettbalsam@gmail.com">brettbalsam@gmail.com</a>
	</span><span style="font-size:12px; color: green; font-family: 'Calibri Light', sans-serif;">
	<p><i>Please consider the environment before printing this email!</i></p>
	</span><span style="font-size:10px; color: gray; font-family: 'Calibri Light', sans-serif;"><p>
	<i>This e-mail may contain confidential and privileged material for the sole use of the intended recipient and is for entertainment purposes only and does not constitute financial advice. Any review, use, distribution or disclosure by others is strictly prohibited. This email and the contents attached are should not be considered an offer to buy or sell. If you are not the intended recipient (or authorized to receive for the recipient), please contact the sender by reply e-mail and delete all copies of this message.</i></p>
	</span></body></html>"""
	newMail.To = emailto #<-----enable for production
	#newMail.CC = ""
	newMail.BCC = "brettbalsam@gmail.com"
	attachment1 = file
	newMail.Attachments.Add(Source=attachment1)
	newMail.send
	print(subject + ' successfully sent')
except Exception as e:
	
	subject = 'FAILED - PROD - ' + subject

	const=win32com.client.constants
	olMailItem = 0x0
	obj = win32com.client.Dispatch("Outlook.Application")
	newMail = obj.CreateItem(olMailItem)
	newMail.Subject = subject
	newMail.BodyFormat = 2 # olFormatHTML https://msdn.microsoft.com/en-us/library/office/aa219371(v=office.11).aspx
	newMail.HTMLBody = """<html><body><span style="font-family: 'Calibri Light', sans-serif;">
	<p>The daily recommendations report failed to run properly.  See error below:</p><p>""" + str(e) + """</p></span></body></html>"""
	newMail.To = "brettbalsam@gmail.com"
	newMail.send
