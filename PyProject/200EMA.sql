
/*
Exponential Moving Average (EMA)
===================================================================
https://www.dropbox.com/s/vxxjr0afdpxwabp/EMA.sql?dl=0
===================================================================
*/
/*=========================*/
use stockdb

IF OBJECT_ID('tempdb..#Stock_List') IS NOT NULL DROP TABLE #Stock_List 
SELECT	dtDate,
		decAdjClose,
		strTick,
		row_number() over (partition by strTick order by dtdate) as QuoteID
INTO	#Stock_list
FROM	snp500_test
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
			AVG(decAdjClose) AS Start_Avg INTO #TBL_START_AVG20
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
            WHEN QuoteId > 20 then T120.decAdjClose * @C20 + @EMA20 * (1 - @C20)
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
		rt2.decAdjClose, 
		CAST(EMA20 AS NUMERIC(10,2)) AS EMA20,
		CAST(avg(rt2.decAdjClose) OVER (PARTITION BY rt2.strtick ORDER BY rt2.dtdate ASC ROWS 199 PRECEDING) AS NUMERIC(10,2)) AS SMA20
INTO #EMA
FROM #TBL_EMA20_RT rt2
GROUP BY rt2.strTick,
		rt2.QuoteId,
		rt2.dtDate,
		rt2.decAdjClose,
		EMA20


select * from #EMA where strtick in ('AAPL')