use stockdb
/*
ATR
Bollinger
Stochastic
RSI
MACD
*/

/*====================================================================
AVERAGE TRUE RANGE

	True Range is calculated as the greater of:

		Average True Range is typically a 14 day exponential moving average* of True Range.

	High for the period less the Low for the period.
	High for the period less the Close for the previous period.
	Close for the previous period and the Low for the current period.

====================================================================*/

/*====================================================================
	Step 1: ESTABLISH ATR
====================================================================*/
IF OBJECT_ID('tempdb..#Stock_ListATR') IS NOT NULL DROP TABLE #Stock_ListATR
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

IF OBJECT_ID('tempdb..#ATR') IS NOT NULL DROP TABLE #ATR 
SELECT	rt2.strTick,
		rt2.QuoteId as ID,
		rt2.dtDate,
		rt2.decAdjClose,
		rt2.decOpen,
		rt2.decHigh,
		rt2.decLow,
		CAST(ATR14 AS NUMERIC(10,2)) AS ATR14
INTO #ATR
FROM #TBL_ATR14_RT rt2
/*
select * from #ATR where dtdate = (select max(dtdate) from snp500_test)
select * from #ATR where strtick like '^%'
*/
/*=========================*/
/*END ATR*/
/*=========================*/
/*=========================*/
/*START BOLLINGER*/
/*=========================*/

IF OBJECT_ID('tempdb..#BOLLINGER') IS NOT NULL DROP TABLE #BOLLINGER

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
INTO #BOLLINGER
FROM snp500_test
WHERE dtdate >= '01/01/2019'
/*
select * from #BOLLINGER where dtdate = (select MAX(dtdate) from snp500_test)
*/
/*=========================*/
/*END BOLLINGER*/
/*=========================*/
/*=========================*/
/*START STOCHASTIC*/
/*=========================*/
IF OBJECT_ID('tempdb..#Stochastic') IS NOT NULL DROP TABLE #Stochastic
Select *,AVG(k.DFAST) OVER (partition by strtick order by dtdate asc ROWS 2 PRECEDING) DSLOW
INTO #Stochastic
FROM
(select *,
	CASE
		WHEN (MAX(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING) = MIN(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING))
		THEN 50
		ELSE (100 * (decadjClose - MIN(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING)))
				/ (MAX(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING) - MIN(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING))
	END as DFAST--%K= (100∗(CP−L14)) / (H14−L14)

from snp500_test
where dtdate >= '01/01/2019'
) k
order by dtdate desc
/*
select *
FROM(
select	strtick,dtdate,
		case when lag(dfast,5) OVER (partition by strtick order by dtdate asc) = 0
		then 0
		else DFAST/lag(dfast,5) OVER (partition by strtick order by dtdate asc)
		end as StochasticChange
from	(select *
		from #Stochastic
		where dtdate >= '11/4/2020') sel
		) sto
where StochasticChange is not null
*/
/*=========================*/
/*END STOCHASTIC*/
/*=========================*/
/*=========================*/
/*START RSI*/
/*=========================*/
IF OBJECT_ID('tempdb..#RSI') IS NOT NULL DROP TABLE #RSI
select	strtick,
		dtdate, /*=100-(100/(1-(AVERAGE(D3:D16)/AVERAGE(E3:E16))))*/
		avg(GAIN) OVER (partition by strtick ORDER BY dtdate asc ROWS 13 PRECEDING ) as GAINPct,
		avg(LOSS)  OVER (partition by strtick ORDER BY dtdate asc ROWS 13 PRECEDING) as LOSSPct,
		CASE WHEN avg(LOSS)  OVER (partition by strtick ORDER BY dtdate asc ROWS 13 PRECEDING) = 0
		THEN 50
		ELSE 100-(100/(1+
		(avg(GAIN) OVER (partition by strtick ORDER BY dtdate asc ROWS 13 PRECEDING )
		/avg(LOSS)  OVER (partition by strtick ORDER BY dtdate asc ROWS 13 PRECEDING))))
		END RSI
INTO #RSI
FROM(
select	dtDate,
		strTick,
		decAdjClose,
		CASE WHEN 
		(decAdjClose/lag(decadjclose,1) OVER (partition by strtick order by dtdate asc))-1 >= 0
		THEN
		(decAdjClose/lag(decadjclose,1) OVER (partition by strtick order by dtdate asc))-1
		ELSE 0
		END as GAIN,
		CASE WHEN 
		(decAdjClose/lag(decadjclose,1) OVER (partition by strtick order by dtdate asc))-1 < 0
		THEN
		ABS((decAdjClose/lag(decadjclose,1) OVER (partition by strtick order by dtdate asc))-1)
		ELSE 0
		END as LOSS
from snp500_test
where dtdate >= '2010-01-01') GL
GROUP BY dtdate,strtick,GAIN,LOSS
/*
select snp.strtick,snp.dtdate,RSI,decOpen,decHigh,decLow,decClose,decAdjClose
from #RSI rsi
JOIN snp500_test snp ON snp.strTick = rsi.strTick
AND snp.dtDate = rsi.dtDate
where snp.strtick like '^%'
order by snp.dtdate asc
*/
/*=========================*/
/*END RSI*/
/*=========================*/
/*=========================*/
/*START MACD*/
/*=========================*/

use stockdb

IF OBJECT_ID('tempdb..#Stock_ListMACD') IS NOT NULL DROP TABLE #Stock_ListMACD
SELECT	ID,
		strTick,
		dtdate,
		decOpen,
		decHigh,
		decLow,
		decAdjClose,
		row_number() over (partition by strTick order by dtdate) as QuoteID
INTO	#Stock_listMACD
FROM	snp500_test
ORDER BY dtDate
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_EMAFast_RT') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMAFast_RT
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMAFast
INTO	#TBL_EMAFast_RT
FROM	#Stock_ListMACD
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
IF OBJECT_ID('tempdb..#TBL_EMASlow_RT') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMASlow_RT
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMASlow
INTO	#TBL_EMASlow_RT
FROM	#Stock_ListMACD
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
IF OBJECT_ID('tempdb..#TBL_EMA200') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMA200
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMA200
INTO	#TBL_EMA200
FROM	#Stock_ListMACD
/*=========================*/
CREATE UNIQUE CLUSTERED INDEX EMAFast_IDX_RT ON #TBL_EMAFast_RT (strTick, QuoteId)
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
CREATE UNIQUE CLUSTERED INDEX EMASlow_IDX_RT ON #TBL_EMASlow_RT (strTick, QuoteId)
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
CREATE UNIQUE CLUSTERED INDEX EMA200 ON #TBL_EMA200 (strTick, QuoteId)
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_START_AVGFast') IS NOT NULL BEGIN
    DROP TABLE #TBL_START_AVGFast
END
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
IF OBJECT_ID('tempdb..#TBL_START_AVGSlow') IS NOT NULL BEGIN
    DROP TABLE #TBL_START_AVGSlow
END
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
IF OBJECT_ID('tempdb..#TBL_START_EMA200') IS NOT NULL BEGIN
    DROP TABLE #TBL_START_EMA200
END
/*=========================*/ 
SELECT		strTick,
			AVG(decAdjClose) AS Start_Avg
INTO #TBL_START_AVGFast
FROM		#stock_ListMACD
WHERE		QuoteId <= 12
GROUP BY	strTick
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
SELECT		strTick,
			AVG(decAdjClose) AS Start_Avg
INTO #TBL_START_AVGSlow
FROM		#stock_ListMACD
WHERE		QuoteId <= 26
GROUP BY	strTick
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
SELECT		strTick,
			AVG(decAdjClose) AS Start_Avg
INTO #TBL_START_EMA200
FROM		#stock_ListMACD
WHERE		QuoteId <= 200
GROUP BY	strTick
/*=========================*/
DECLARE @CFast FLOAT = 2.0 / (1 + 12), @EMAFast FLOAT
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
DECLARE @CSlow FLOAT = 2.0 / (1 + 26), @EMASlow FLOAT
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
DECLARE @C200 FLOAT = 2.0 / (1 + 200), @EMA200 FLOAT
/*=========================*/
UPDATE
    T1Fast
SET
    @EMAFast =
        CASE
            WHEN QuoteId = 12 then T2Fast.Start_Avg
            WHEN QuoteId > 12 then T1Fast.decAdjClose * @CFast + @EMAFast * (1 - @CFast)
        END
    ,EMAFast = @EMAFast 
FROM
    #TBL_EMAFast_RT T1Fast
JOIN
    #TBL_START_AVGFast T2Fast
ON
    T1Fast.strTick = T2Fast.strTick
option (maxrecursion 0);
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
UPDATE
    T1Slow
SET
    @EMASlow =
        CASE
            WHEN QuoteId = 26 then T2Slow.Start_Avg
            WHEN QuoteId > 26 then T1Slow.decAdjClose * @CSlow + @EMASlow * (1 - @CSlow)
        END
    ,EMASlow = @EMASlow 
FROM
    #TBL_EMASlow_RT T1Slow
JOIN
    #TBL_START_AVGSlow T2Slow
ON
    T1Slow.strTick = T2Slow.strTick
option (maxrecursion 0);
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
UPDATE
    T1EMA200
SET
    @EMA200 =
        CASE
            WHEN QuoteId = 200 then T2EMA200.Start_Avg
            WHEN QuoteId > 200 then T1EMA200.decAdjClose * @C200 + @EMA200 * (1 - @C200)
        END
    ,EMA200 = @EMA200 
FROM
    #TBL_EMA200 T1EMA200
JOIN
    #TBL_START_EMA200 T2EMA200
ON
    T1EMA200.strTick = T2EMA200.strTick
option (maxrecursion 0);
/*=========================*/


IF OBJECT_ID('tempdb..#EMA') IS NOT NULL DROP TABLE #EMA  
SELECT	rt2.ID,
		rt2.QuoteId as QuoteID,
		rt2.strTick,
		rt2.dtdate,
		rt2.decOpen,
		rt2.decHigh,
		rt2.decLow,
		rt2.decAdjClose, 
		CAST(EMAFast AS NUMERIC(10,2)) AS EMAFast,
		CAST(EMASlow AS NUMERIC(10,2)) AS EMASlow,
		CAST(EMA200 AS NUMERIC(10,2)) AS EMA200
INTO #EMA
FROM #TBL_EMAFast_RT rt2
JOIN #TBL_EMASlow_RT rt5 	ON rt5.strTick = rt2.strTick AND rt5.QuoteID = rt2.QuoteID
JOIN #TBL_EMA200 rt6 		ON rt6.strTick = rt2.strTick AND rt6.QuoteID = rt2.QuoteID

/*==================================================*/
/*END Exponential Moving Average (EMA)*/
/*==================================================*/
/*==================================================*/
/*BEGIN MACD SIGNAL LINE (9 EMA OF MACD)*/
/*==================================================*/
/*
====TABLES====

#Stock_ListMACD = #PREMACD
snp500_test = #EMA
#TBL_EMAFast_RT = #TBL_MACD_SIGNAL 
	T1Fast = MACDSig
#TBL_EMASlow_RT = DELETE
#TBL_START_AVGFast = #TBL_START_MACD_SIGNAL
	T2Fast = StMACDSig
#TBL_START_AVGSlow = DELETE
#EMA = #MACD

====VARIABLES====

@CFast = @SigFast
@EMAFast = @MACDFast

*/

IF OBJECT_ID('tempdb..#PREMACD') IS NOT NULL DROP TABLE #PREMACD 
SELECT	ID,strTick,dtdate,decOpen,decHigh,decLow,decAdjClose,EMAFast,EMASlow,EMA200,
		EMAFast-EMASlow as MACD,
		row_number() over (partition by strTick order by dtdate) as QuoteID
INTO	#PREMACD
FROM	#EMA
ORDER BY dtDate
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_MACD_SIGNAL') IS NOT NULL BEGIN
    DROP TABLE #TBL_MACD_SIGNAL
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS MACDFast
INTO	#TBL_MACD_SIGNAL
FROM	#PREMACD
/*=========================*/
CREATE UNIQUE CLUSTERED INDEX MACDFast_IDX_RT ON #TBL_MACD_SIGNAL (strTick, QuoteId)
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_START_MACD_SIGNAL') IS NOT NULL BEGIN
    DROP TABLE #TBL_START_MACD_SIGNAL
END
/*=========================*/ 
SELECT		strTick,
			AVG(MACD) AS Start_Avg INTO #TBL_START_MACD_SIGNAL
FROM		#PREMACD
WHERE		QuoteId <= ((12+26)-1)
GROUP BY	strTick
/*=========================*/
DECLARE @SigFast FLOAT = 2.0 / (1 + 9), @MACDFast FLOAT
/*=========================*/
UPDATE
    MACDSig
SET
    @MACDFast =
        CASE
            WHEN QuoteId = ((12+26)-1) then StMACDSig.Start_Avg
            WHEN QuoteId > ((12+26)-1) then MACDSig.MACD * @SigFast + @MACDFast * (1 - @SigFast)
        END
    ,MACDFast = @MACDFast 
FROM
    #TBL_MACD_SIGNAL MACDSig
JOIN
    #TBL_START_MACD_SIGNAL StMACDSig
ON
    MACDSig.strTick = StMACDSig.strTick
option (maxrecursion 0);
/*=========================*/
IF OBJECT_ID('tempdb..##StochasticMACD') IS NOT NULL  DROP TABLE #StochasticMACD 
Select *,AVG(k.DFAST) OVER (partition by strtick order by dtdate asc ROWS 2 PRECEDING) DSLOW
INTO #StochasticMACD
FROM
(select *,
	CASE
		WHEN (MAX(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING) = MIN(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING))
		THEN 50
		ELSE (100 * (decadjClose - MIN(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING)))
				/ (MAX(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING) - MIN(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING))
	END as DFAST--%K= (100∗(CP−L14)) / (H14−L14)
from snp500_test
where dtdate > '01/01/2019'
) k
/*=========================*/
IF OBJECT_ID('tempdb..#MACD') IS NOT NULL DROP TABLE #MACD  
SELECT	rt2.*,
		CAST(MACDFast AS NUMERIC(10,2)) AS MACDSignalLine
INTO #MACD
FROM #TBL_MACD_SIGNAL rt2

/*
TEST QUERY
*/

IF OBJECT_ID('tempdb..#MACDList') IS NOT NULL DROP TABLE #MACDList  
Select *,
		CASE WHEN (ct.EMA200 < ct.decLow) 
		THEN 0
		WHEN (ct.EMA200 >= ct.decLow)
		THEN 1
		ELSE NULL
		END as EMA200HL
into #MACDList
FROM(
Select	ma.strTick,ma.dtDate,ma.decAdjClose,LEAD(ma.decOpen,1) OVER (PARTITION BY ma.strTick ORDER BY ma.dtdate ASC) AS decOpen,ma.dechigh,ma.declow,ma.EMAFast,ma.EMASlow,ma.EMA200,ma.MACD,ma.MACDSignalLine,
		LAG(ma.MACDSignalLine,1) OVER (PARTITION BY ma.strTick ORDER BY ma.dtdate ASC) AS PriorMACDSignalLine,
		LAG(ma.MACD,1) OVER (PARTITION BY ma.strTick ORDER BY ma.dtdate ASC) AS PriorMACD,DSLOW,DFAST,
		LAG(sto.DSLOW,3) OVER (Partition by sto.strtick order by sto.dtdate asc) as PriorDSLOW
from #MACD ma
JOIN #StochasticMACD sto ON sto.ID = ma.ID) ct
where ct.dtDate > = '1/1/2018'
		AND(ct.EMA200 < ct.decLow)
		AND(ct.MACDSignalLine < 0)
		AND(ct.MACDSignalLine < ct.MACD)
		AND(ct.PriorMACDSignalLine > ct.PriorMACD)
		AND(PriorDSLOW < DSLOW)
Order by ct.strTick, ct.dtdate

/* 
FINAL QUERY
*/


SELECT	macd.strTick,
		macd.dtDate,
		macd.decAdjClose,
		macd.decOpen,
		macd.dechigh,
		macd.declow,
		sto.intVol,
		macd.EMA200,
		macd.MACD,
		macd.MACDSignalLine,
		atr.ATR14,
		sto.DFAST,
		sto.DSLOW,
		rsi.RSI
FROM #MACD				macd
JOIN #ATR			atr		ON atr.strTick = macd.strTick AND atr.dtDate = macd.dtDate
JOIN #Stochastic	sto		ON sto.strTick = macd.strTick AND sto.dtDate = macd.dtDate
JOIN #RSI			rsi		ON rsi.strTick = macd.strTick AND rsi.dtDate = macd.dtDate
ORDER BY	strTick asc,
			dtDate asc