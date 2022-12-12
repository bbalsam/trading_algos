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
IF OBJECT_ID('tempdb..#Stock_List') IS NOT NULL DROP TABLE #Stock_List 
SELECT	ID,
		dtDate,
		strTick,
		decHigh,
		decLow,
		decOpen,
		decAdjClose,
		row_number() over (partition by strTick order by dtdate) as QuoteID
INTO	#Stock_list
FROM	snp500_test
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
FROM #Stock_list
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



select *,
		avg(decAdjClose) OVER (partition by strtick order by dtdate asc ROWS 29 PRECEDING) SMA30,
		avg(decAdjClose) OVER (partition by strtick order by dtdate asc ROWS 99 PRECEDING) SMA100
from #ATR
where strtick in ('tsla')


