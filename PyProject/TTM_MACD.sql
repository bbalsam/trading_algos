/*
Notes on algorithm:
Look only at stocks with 1 year and half year positive trends (this needs to be tested)
Start with 2 stdev drop as trigger for watch.
Once macd bottoms and crosses over, buy signal
Sell for gain when upper bollinger crosses above upper keltner line AND price is at or above 2 stdev

Need to investigate for stop loss.

*/



use stock
/*
--MACD TABLES:
DROP TABLE #EMA
DROP TABLE #MACD
DROP TABLE #PREMACD
DROP TABLE #Stock_List_MACD
DROP TABLE #TBL_EMAFast_RT
DROP TABLE #TBL_EMASlow_RT
DROP TABLE #TBL_MACD_SIGNAL
DROP TABLE #TBL_START_AVGFast
DROP TABLE #TBL_START_AVGSlow
DROP TABLE #TBL_START_MACD_SIGNAL
DROP TABLE #MACD_REPORT

--TTM TABLES:
DROP TABLE #Stock_List
DROP TABLE #TBL_EMA20_RT
DROP TABLE #TBL_START_AVG20
DROP TABLE #EMA
DROP TABLE #ATR01
DROP TABLE #TTM
DROP TABLE #REPORT
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


IF OBJECT_ID('tempdb..#MACD_REPORT') IS NOT NULL DROP TABLE #MACD_REPORT
SELECT
	*
	,CASE
		WHEN MACD IS NULL
		THEN NULL
		WHEN MACD > MACDFast
		THEN 1
		ELSE 0
	END AS CURR_SIGNAL
	,CASE
		WHEN LAG(MACD,1) OVER (PARTITION BY strTick ORDER BY dtDate) IS NULL
		THEN NULL
		WHEN LAG(MACD,1) OVER (PARTITION BY strTick ORDER BY dtDate) > LAG(MACDFast,1) OVER (PARTITION BY strTick ORDER BY dtDate)
		THEN 1
		ELSE 0
	END AS PRIOR_SIGNAL
INTO #MACD_REPORT
FROM
	#MACD

	

/*
TTM START
*/

IF OBJECT_ID('tempdb..#Stock_List') IS NOT NULL DROP TABLE #Stock_List 
SELECT
	dtDate
	,decClose
	,strTick
	,row_number() over (partition by strTick order by dtdate) as QuoteID
INTO
	#Stock_list
FROM
	STOCKS
WHERE
	dtDate >='1/1/2020'
ORDER BY dtDate
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_EMA20_RT') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMA20_RT
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMA20
INTO	#TBL_EMA20_RT
FROM	#Stock_List

/*=========================*/
CREATE UNIQUE CLUSTERED INDEX EMA20_IDX_RT ON #TBL_EMA20_RT (strTick, QuoteId)
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_START_AVG20') IS NOT NULL BEGIN
    DROP TABLE #TBL_START_AVG20
END
/*=========================*/ 
SELECT		strTick,
			AVG(decClose) AS Start_Avg INTO #TBL_START_AVG20
FROM		#stock_List
WHERE		QuoteId <= 20
GROUP BY	strTick
/*=========================*/
DECLARE @C20 FLOAT = 2.0 / (1 + 20), @EMA20 FLOAT
/*=========================*/
UPDATE
    T120
SET
    @EMA20 =
        CASE
            WHEN QuoteId = 20 then T220.Start_Avg
            WHEN QuoteId > 20 then T120.decClose * @C20 + @EMA20 * (1 - @C20)
        END
    ,EMA20 = @EMA20 
FROM
    #TBL_EMA20_RT T120
JOIN
    #TBL_START_AVG20 T220
ON
    T120.strTick = T220.strTick
option (maxrecursion 0);

/*=========================*/

IF OBJECT_ID('tempdb..#EMA') IS NOT NULL DROP TABLE #EMA  
SELECT	rt2.strTick,
		rt2.QuoteId as ID,
		rt2.dtDate,
		rt2.decClose, 
		CAST(EMA20 AS NUMERIC(15,2)) AS EMA20,
		CAST(avg(rt2.decClose) OVER (PARTITION BY rt2.strtick ORDER BY rt2.dtdate ASC ROWS 19 PRECEDING) AS NUMERIC(15,2)) AS SMA20
INTO #EMA
FROM #TBL_EMA20_RT rt2
GROUP BY rt2.strTick,
		rt2.QuoteId,
		rt2.dtDate,
		rt2.decClose,
		EMA20

IF OBJECT_ID('tempdb..#ATR01') IS NOT NULL DROP TABLE #ATR01
SELECT
	dtDate
	,decOpen
	,decHigh
	,decLow
	,decClose
	,decAdjClose
	,intVol
	,strTick
	,ID

--TR = ​max [(high − low), abs(high − closeprev​), abs(low – closeprev​)]
,decHigh - decLow AS "H-L"
,ABS(decHigh - LAG(decClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC)) AS "H-PC"
,ABS(decLow - LAG(decClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC)) AS "L-PC"
,CASE
	WHEN (decHigh - decLow) >= (ABS(decHigh - LAG(decClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC)))
		AND (decHigh - decLow) >= (ABS(decLow - LAG(decClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC)))
	THEN (decHigh - decLow)
	WHEN (ABS(decHigh - LAG(decClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC))) >= (decHigh - decLow)  
		AND (ABS(decHigh - LAG(decClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC))) >= (ABS(decLow - LAG(decClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC)))
	THEN ABS((decHigh - LAG(decClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC)))
	ELSE ABS((decLow - LAG(decClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC)))
END AS TRUE_RANGE
INTO
#ATR01
FROM
STOCKS
WHERE
	dtDate >= '01/01/2020'
ORDER BY
	strTick
	,dtDate DESC

IF OBJECT_ID('tempdb..#TTM') IS NOT NULL DROP TABLE #TTM
SELECT
	ATR.strTick
	,ATR.dtDate
	,ATR.intVol
	,ATR.decClose
	,ATR.decHigh
	,ATR.decLow
	,ATR.decOpen
	,(ATR.decClose/LAG(ATR.decClose,251) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate))-1 AS YOY_Trend
	,(ATR.decClose/LAG(ATR.decClose,125) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate))-1 AS HalfYear_Trend
	,(ATR.decClose/LAG(ATR.decClose,(21)) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate))-1 AS LastMo_Trend
	,EMA.EMA20
	,EMA.SMA20
	/*calculate histogram
	(Highest high in 20 periods + lowest low in 20 periods) / 2
	Donchian midline
	*/
	,((MAX(ATR.decHigh) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
	-MIN(ATR.decLow) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW))/2) AS DonchianMidline
	/*Momentum Histogram
	Second, calculate the SMA of the close for the specified number of momentum periods (so by default, a 20-period SMA of price).

	Third, calculate the delta between the close and the average of the Donchian midline and SMA values using the following formula:
	Finally, use linear regression on the delta values to smooth them. The formula for linear regression is beyond the scope of this article, but it essentially looks for the “line of best fit” given the available data. The momentum histogram values show how far above or below the average the price is expected to be.
	*/
	,ATR.decClose - ((((MAX(ATR.decHigh) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
	-MIN(ATR.decLow) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW))/2)+EMA.SMA20)/2)
	AS MomentumHistogram --apply linear regression for true formula
	/*,LEAD(ATR.decClose,8) OVER (Partition by ATR.strTick ORDER BY ATR.dtdate ASC) AS FuturePrice
	,LEAD(ATR.decClose,8) OVER (Partition by ATR.strTick ORDER BY ATR.dtdate ASC)/ATR.decClose AS FUTUREGainLoss
	,LAG(ATR.dtDate,125) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate) AS LAGDATE
	,LAG(ATR.decClose,125) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate) AS LAGVAL
	,ATR.decClose/LAG(ATR.decClose,125) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate) AS PAST2QTRS
	*/
	,AVG(ATR.TRUE_RANGE) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ATR
	,EMA20+(2*AVG(ATR.TRUE_RANGE) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) AS UKeltner
	,EMA20-(2*AVG(ATR.TRUE_RANGE) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) AS LKeltner
	,STDEV(ATR.decClose) OVER (PARTITION BY EMA.strTick ORDER BY EMA.dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS StandardDevClose
	,EMA.SMA20+(2*(STDEV(ATR.decClose) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW))) AS UBollinger
	,EMA.SMA20-(2*(STDEV(ATR.decClose) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW))) AS LBollinger
INTO
	#TTM
FROM
	#ATR01 ATR
LEFT JOIN
	#EMA EMA ON ATR.strTick = EMA.strTick
			AND ATR.dtDate = EMA.dtDate



IF OBJECT_ID('tempdb..#REPORT') IS NOT NULL DROP TABLE #REPORT

SELECT
	strTick
	,dtDate
	,intVol
	,decClose
	,decHigh
	,decLow
	,decOpen
	,YOY_Trend
	,HalfYear_Trend
	,LastMo_Trend
	/*,LAGDATE
	,LAGVAL
	,PAST2QTRS
	,FuturePrice
	,FUTUREGainLoss*/
	,ATR
	,EMA20
	,SMA20
	,UKeltner
	,UBollinger
	,CASE WHEN (UBollinger>UKeltner OR LBollinger<LKeltner)
		AND (LAG(UBollinger,1) OVER (PARTITION BY strTick ORDER BY dtDate)<LAG(UKeltner,1) OVER (PARTITION BY strTick ORDER BY dtDate)
		AND LAG(LBollinger,1) OVER (PARTITION BY strTick ORDER BY dtDate)>LAG(LKeltner,1) OVER (PARTITION BY strTick ORDER BY dtDate))
	THEN 1
	WHEN (UBollinger<UKeltner AND LBollinger>LKeltner)
	THEN .5
	ELSE 0
	END AS BuyIndicator
	,LKeltner
	,LBollinger
	,DonchianMidline
	,MomentumHistogram
INTO #REPORT
FROM
	#TTM
WHERE
	dtDate > = '01/01/2020'
	--and strTick = 'LULU'


IF OBJECT_ID('tempdb..#TTM_MACD_COMBINED') IS NOT NULL DROP TABLE #TTM_MACD_COMBINED
SELECT
	TTM.strTick
	,TTM.dtDate
	,MACD.dtDate AS MACD_DATE
	,TTM.BuyIndicator
	,TTM.decClose
	,TTM.decHigh
	,TTM.decLow
	,TTM.decOpen
	,TTM.ATR
	,TTM.UKeltner
	,TTM.UBollinger
	,TTM.LKeltner
	,TTM.LBollinger
	,TTM.SMA20
	,TTM.EMA20
	,TTM.DonchianMidline
	,TTM.MomentumHistogram
	,TTM.YOY_Trend
	,TTM.HalfYear_Trend
	,TTM.LastMo_Trend
	,MACD.EMAFast
	,MACD.EMASlow
	,MACD.MACD
	,MACD.MACDFast
	,MACD.MACDSignalLine
INTO
	#TTM_MACD_COMBINED
FROM
	#REPORT TTM
JOIN
	#MACD_REPORT MACD ON TTM.strTick = MACD.strTick
					AND TTM.dtDate < = MACD.dtDate
WHERE
	(TTM.decLow <= TTM.LKeltner
		OR TTM.decLow <= TTM.LBollinger)
	--AND TTM.YOY_Trend > 1
	--AND TTM.HalfYear_Trend > 1
	--AND TTM.LastMo_Trend > 1
	AND TTM.dtDate >= DATEADD(dd, DATEDIFF(dd, 0, getdate()), 0)-251
	AND MACD.CURR_SIGNAL = 0
	AND MACD.PRIOR_SIGNAL = 1
	AND MACD.MACD < 0
ORDER BY
	TTM.strTick ASC
	,MACD.dtDate ASC

SELECT
	#REPORT.strTick
	,WATCH_DATE
	,TRIGGER_DATE
FROM
	#REPORT
JOIN
(SELECT
	SUB1.strTick
	,MIN(SUB1.WATCH_DATE) AS WATCH_DATE
	,SUB1.TRIGGER_DATE
FROM
	(
	SELECT
		strTick
		,dtDate AS WATCH_DATE
		,MIN(MACD_DATE) AS TRIGGER_DATE
	FROM
		#TTM_MACD_COMBINED
	WHERE
		YOY_Trend > 1
		AND HalfYear_Trend > 1
		AND LastMo_Trend > 1
	GROUP BY
		strTick
		,dtDate
	) SUB1
GROUP BY
	SUB1.strTick
	,SUB1.TRIGGER_DATE
) LIST ON #REPORT.strTick = LIST.strTick AND #REPORT.dtDate = LIST.TRIGGER_DATE
/*WHERE
	YOY_Trend > 1
	AND HalfYear_Trend  > 1
	AND LastMo_Trend  > 0*/
ORDER BY
	TRIGGER_DATE DESC


SELECT
	*
FROM
	#REPORT TTM
JOIN
	#MACD_REPORT MCD ON TTM.strTick = MCD.strTick AND TTM.dtDate = MCD.dtDate
JOIN
	(SELECT
		strTick
		,MAX(dtDate) AS MAXDATE
	FROM
		#REPORT
	GROUP BY
		strTick
	) MXD ON TTM.dtDate = MXD.MAXDATE AND TTM.strTick = MXD.strTick
		
WHERE 
	TTM.UKeltner > TTM.UBollinger
	AND TTM.LBollinger > TTM.LKeltner
	AND MCD.CURR_SIGNAL = 1 --Zero MACD below signal line, 1 above
	AND MCD.PRIOR_SIGNAL = 0 --Zero MACD below signal line, 1 above
	AND MCD.MACD < 0
ORDER BY
	MACD ASC
	

