/*
Schaff Trend Cycle (STC)
===================================================================
The STC is calculated in the following order:
===================================================================
First, the 23-period and the 50-period EMA and the MACD values are calculated:

EMA1 = EMA (Close, Short Length);

EMA2 = EMA (Close, Long Length);

MACD = EMA1 – EMA2.

Second, the 10-period Stochastic from the MACD values is calculated:

%K (MACD) = %KV (MACD, 10);

%D (MACD) = %DV (MACD, 10);

Schaff = 100 x (MACD – %K (MACD)) / (%D (MACD) – %K (MACD)).
*/


/*
Exponential Moving Average (EMA)
===================================================================
https://www.dropbox.com/s/vxxjr0afdpxwabp/EMA.sql?dl=0
===================================================================
*/
/*=========================*/
IF OBJECT_ID('tempdb..#Stock_List') IS NOT NULL DROP TABLE #Stock_List 
SELECT	dtDate,
		decAdjClose,
		strTick,
		row_number() over (partition by strTick order by dtdate) as QuoteID
INTO	#Stock_list
FROM	snp500_test
ORDER BY dtDate
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_EMA23_RT') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMA23_RT
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMA23
INTO	#TBL_EMA23_RT
FROM	#Stock_List
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
IF OBJECT_ID('tempdb..#TBL_EMA50_RT') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMA50_RT
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMA50
INTO	#TBL_EMA50_RT
FROM	#Stock_List
/*=========================*/
CREATE UNIQUE CLUSTERED INDEX EMA23_IDX_RT ON #TBL_EMA23_RT (strTick, QuoteId)
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
CREATE UNIQUE CLUSTERED INDEX EMA50_IDX_RT ON #TBL_EMA50_RT (strTick, QuoteId)
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_START_AVG23') IS NOT NULL BEGIN
    DROP TABLE #TBL_START_AVG23
END
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
IF OBJECT_ID('tempdb..#TBL_START_AVG50') IS NOT NULL BEGIN
    DROP TABLE #TBL_START_AVG50
END
/*=========================*/ 
SELECT		strTick,
			AVG(decAdjClose) AS Start_Avg INTO #TBL_START_AVG23
FROM		#stock_List
WHERE		QuoteId <= 23
GROUP BY	strTick
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
SELECT		strTick,
			AVG(decAdjClose) AS Start_Avg INTO #TBL_START_AVG50
FROM		#stock_List
WHERE		QuoteId <= 50
GROUP BY	strTick
/*=========================*/
DECLARE @C23 FLOAT = 2.0 / (1 + 23), @EMA23 FLOAT
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
DECLARE @C50 FLOAT = 2.0 / (1 + 50), @EMA50 FLOAT
/*=========================*/
UPDATE
    T123
SET
    @EMA23 =
        CASE
            WHEN QuoteId = 23 then T223.Start_Avg
            WHEN QuoteId > 23 then T123.decAdjClose * @C23 + @EMA23 * (1 - @C23)
        END
    ,EMA23 = @EMA23 
FROM
    #TBL_EMA23_RT T123
JOIN
    #TBL_START_AVG23 T223
ON
    T123.strTick = T223.strTick
option (maxrecursion 0);
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
UPDATE
    T150
SET
    @EMA50 =
        CASE
            WHEN QuoteId = 23 then T250.Start_Avg
            WHEN QuoteId > 23 then T150.decAdjClose * @C50 + @EMA50 * (1 - @C50)
        END
    ,EMA50 = @EMA50 
FROM
    #TBL_EMA50_RT T150
JOIN
    #TBL_START_AVG50 T250
ON
    T150.strTick = T250.strTick
option (maxrecursion 0);
/*=========================*/

IF OBJECT_ID('tempdb..#EMA') IS NOT NULL DROP TABLE #EMA  
SELECT	rt2.strTick,
		rt2.QuoteId as ID,
		rt2.dtDate,
		rt2.decAdjClose, 
		CAST(EMA23 AS NUMERIC(10,2)) AS EMA23,
		CAST(EMA50 AS NUMERIC(10,2)) AS EMA50
INTO #EMA
FROM #TBL_EMA23_RT rt2
JOIN #TBL_EMA50_RT rt5 ON rt5.strTick = rt2.strTick AND rt5.QuoteID = rt2.QuoteID
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
/*second step not necessary*/
/*=========================*/

select * from #EMA
where strTick = 'aapl'
order by dtDate desc
