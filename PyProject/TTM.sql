/*
Notes on algorithm:
Look only at stocks with 1 year and half year positive trends (this needs to be tested)
Start with 2 stdev drop as trigger for watch.
Once macd bottoms and crosses over, buy signal
Sell for gain when upper bollinger crosses above upper keltner line AND price is at or above 2 stdev

Need to investigate for stop loss.

*/

SELECT TOP 10 * FROM STOCKS

use stock
/*DROP TABLE #Stock_List
DROP TABLE #TBL_START_AVG20
DROP TABLE #EMA
DROP TABLE #ATR01
DROP TABLE #TTM
DROP TABLE #REPORT*/

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
	,ATR.decClose/LAG(ATR.decClose,251) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate) AS YOY_Trend
	,ATR.decClose/LAG(ATR.decClose,125) OVER (PARTITION BY ATR.strTick ORDER BY ATR.dtDate) AS HalfYear_Trend
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

/*SELECT
	strTick
	,COUNT(*)
FROM
	#REPORT
WHERE
	BuyIndicator >0.6
GROUP BY
	strTick
ORDER BY
	COUNT(*) DESC
*/
/*SELECT
	strTick
	,dtDate 
	,BuyIndicator
	,decClose
	,UKeltner
	,UBollinger
	,LKeltner
	,LBollinger
	,ATR
	,SMA20
	,EMA20
	,DonchianMidline
	,MomentumHistogram
	,YOY_Trend
	,HalfYear_Trend
FROM
	#REPORT
WHERE
	--BuyIndicator >0.0
	--AND 
	dtDate >= DATEADD(dd, DATEDIFF(dd, 0, getdate()), 0)-30
	AND
	strTick = 'SNPS'
	*/

SELECT
	strTick
	,dtDate 
	,BuyIndicator
	,decClose
	,decHigh
	,decLow
	,decOpen
	,ATR
	,UKeltner
	,UBollinger
	,LKeltner
	,LBollinger
	,SMA20
	,EMA20
	,DonchianMidline
	,MomentumHistogram
	,YOY_Trend
	,HalfYear_Trend
FROM
	#REPORT
WHERE
	(decLow <= LKeltner OR decLow <= LBollinger)
	AND YOY_Trend > 1
	AND HalfYear_Trend > 1
	AND dtDate >= DATEADD(dd, DATEDIFF(dd, 0, getdate()), 0)-120
ORDER BY
	dtDate
	


/*SELECT
	strTick
	,dtDate 
	,BuyIndicator
	,decClose
	,UKeltner
	,UBollinger
	,LKeltner
	,LBollinger
	,SMA20
	,DonchianMidline
	,MomentumHistogram
FROM
	#REPORT
WHERE
	strTick IN ('FNMA')
	AND dtDate >= DATEADD(dd, DATEDIFF(dd, 0, getdate()), 0)-560*/