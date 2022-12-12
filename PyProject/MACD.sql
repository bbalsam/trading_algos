/*MACD*/
/*
TEMP TABLES:
#EMA
#MACD
#PREMACD
#Stock_List_MACD
#TBL_EMAFast_RT
#TBL_EMASlow_RT
#TBL_MACD_SIGNAL
#TBL_START_AVGFast
#TBL_START_AVGSlow
#TBL_START_MACD_SIGNAL
*/
/*
Exponential Moving Average (EMA)
===================================================================
https://www.dropbox.com/s/vxxjr0afdpxwabp/EMA.sql?dl=0
===================================================================
*/
/*=========================*/
IF OBJECT_ID('tempdb..#Stock_List_MACD') IS NOT NULL DROP TABLE #Stock_List_MACD 
SELECT	dtDate,
		decAdjClose,
		strTick,
		row_number() over (partition by strTick order by dtdate) as QuoteID
INTO	#Stock_List_MACD
FROM	STOCKS
ORDER BY dtDate
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_EMAFast_RT') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMAFast_RT
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMAFast
INTO	#TBL_EMAFast_RT
FROM	#Stock_List_MACD
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
IF OBJECT_ID('tempdb..#TBL_EMASlow_RT') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMASlow_RT
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMASlow
INTO	#TBL_EMASlow_RT
FROM	#Stock_List_MACD
/*=========================*/
CREATE UNIQUE CLUSTERED INDEX EMAFast_IDX_RT ON #TBL_EMAFast_RT (strTick, QuoteId)
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
CREATE UNIQUE CLUSTERED INDEX EMASlow_IDX_RT ON #TBL_EMASlow_RT (strTick, QuoteId)
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_START_AVGFast') IS NOT NULL BEGIN
    DROP TABLE #TBL_START_AVGFast
END
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
IF OBJECT_ID('tempdb..#TBL_START_AVGSlow') IS NOT NULL BEGIN
    DROP TABLE #TBL_START_AVGSlow
END
/*=========================*/ 
SELECT		strTick,
			AVG(decAdjClose) AS Start_Avg INTO #TBL_START_AVGFast
FROM		#Stock_List_MACD
WHERE		QuoteId <= 12
GROUP BY	strTick
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
SELECT		strTick,
			AVG(decAdjClose) AS Start_Avg INTO #TBL_START_AVGSlow
FROM		#Stock_List_MACD
WHERE		QuoteId <= 26
GROUP BY	strTick
/*=========================*/
DECLARE @CFast FLOAT = 2.0 / (1 + 12), @EMAFast FLOAT
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
DECLARE @CSlow FLOAT = 2.0 / (1 + 26), @EMASlow FLOAT
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
            WHEN QuoteId = 12 then T2Slow.Start_Avg
            WHEN QuoteId > 12 then T1Slow.decAdjClose * @CSlow + @EMASlow * (1 - @CSlow)
        END
    ,EMASlow = @EMASlow 
FROM
    #TBL_EMASlow_RT T1Slow
JOIN
    #TBL_START_AVGSlow T2Slow
ON
    T1Slow.strTick = T2Slow.strTick
option (maxrecursion 0);
/*=========================*/

IF OBJECT_ID('tempdb..#EMA_MACD') IS NOT NULL DROP TABLE #EMA_MACD  
SELECT	rt2.strTick,
		rt2.QuoteId as ID,
		rt2.dtDate,
		rt2.decAdjClose, 
		CAST(EMAFast AS NUMERIC(10,2)) AS EMAFast,
		CAST(EMASlow AS NUMERIC(10,2)) AS EMASlow
INTO #EMA_MACD
FROM #TBL_EMAFast_RT rt2
JOIN #TBL_EMASlow_RT rt5 ON rt5.strTick = rt2.strTick AND rt5.QuoteID = rt2.QuoteID
/*
/*==================================================*/
END Exponential Moving Average (EMA)
/*==================================================*/
*/
/*
/*==================================================*/
BEGIN MACD SIGNAL LINE (9 EMA OF MACD)
/*==================================================*/
*/
/*
====TABLES====

#Stock_List_MACD = #PREMACD
STOCKS = #EMA_MACD
#TBL_EMAFast_RT = #TBL_MACD_SIGNAL 
	T1Fast = MACDSig
#TBL_EMASlow_RT = DELETE
#TBL_START_AVGFast = #TBL_START_MACD_SIGNAL
	T2Fast = StMACDSig
#TBL_START_AVGSlow = DELETE
#EMA_MACD = #MACD

====VARIABLES====

@CFast = @SigFast
@EMAFast = @MACDFast

*/
IF OBJECT_ID('tempdb..#PREMACD') IS NOT NULL DROP TABLE #PREMACD 
SELECT	*,
		EMAFast-EMASlow as MACD,
		row_number() over (partition by strTick order by dtdate) as QuoteID
INTO	#PREMACD
FROM	#EMA_MACD
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

IF OBJECT_ID('tempdb..#MACD') IS NOT NULL DROP TABLE #MACD  
SELECT	rt2.*,
		CAST(MACDFast AS NUMERIC(10,2)) AS MACDSignalLine
INTO #MACD
FROM #TBL_MACD_SIGNAL rt2

/*
TEST QUERY
*/

Select strTick,dtDate,decAdjClose,EMAFast,EMASlow,MACD,MACDSignalLine
from #MACD
where dtDate > = '9/18/2020'
Order by strTick, dtdate