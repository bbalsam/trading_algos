
/*
Exponential Moving Average (EMA)
===================================================================
https://www.dropbox.com/s/vxxjr0afdpxwabp/EMA.sql?dl=0
===================================================================
*/

use stock

IF OBJECT_ID('tempdb..#Stock_List') IS NOT NULL DROP TABLE #Stock_List 
SELECT	dtDate,
		decClose,
		strTick,
		row_number() over (partition by strTick order by dtdate) as QuoteID
INTO	#Stock_list
FROM	STOCKS
ORDER BY dtDate
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_EMA21_RT') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMA21_RT
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMA21
INTO	#TBL_EMA21_RT
FROM	#Stock_List

/*=========================*/
CREATE UNIQUE CLUSTERED INDEX EMA21_IDX_RT ON #TBL_EMA21_RT (strTick, QuoteId)
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_START_AVG21') IS NOT NULL BEGIN
    DROP TABLE #TBL_START_AVG21
END
/*=========================*/ 
SELECT		strTick,
			AVG(decClose) AS Start_Avg INTO #TBL_START_AVG21
FROM		#stock_List
WHERE		QuoteId <= 21
GROUP BY	strTick
/*=========================*/
DECLARE @C21 FLOAT = 2.0 / (1 + 21), @EMA21 FLOAT
/*=========================*/
UPDATE
    T121
SET
    @EMA21 =
        CASE
            WHEN QuoteId = 21 then T221.Start_Avg
            WHEN QuoteId > 21 then T121.decClose * @C21 + @EMA21 * (1 - @C21)
        END
    ,EMA21 = @EMA21 
FROM
    #TBL_EMA21_RT T121
JOIN
    #TBL_START_AVG21 T221
ON
    T121.strTick = T221.strTick
option (maxrecursion 0);

/*=========================*/

IF OBJECT_ID('tempdb..#EMA') IS NOT NULL DROP TABLE #EMA  
SELECT	rt2.strTick,
		rt2.QuoteId as ID,
		rt2.dtDate,
		rt2.decClose, 
		CAST(EMA21 AS NUMERIC(15,2)) AS EMA21,
		CAST(avg(rt2.decClose) OVER (PARTITION BY rt2.strtick ORDER BY rt2.dtdate ASC ROWS 20 PRECEDING) AS NUMERIC(15,2)) AS SMA21
INTO #EMA
FROM #TBL_EMA21_RT rt2
GROUP BY rt2.strTick,
		rt2.QuoteId,
		rt2.dtDate,
		rt2.decClose,
		EMA21

select snp.strTick,snp.ID,snp.dtDate,snp.decHigh,snp.decLow,snp.decOpen,snp.decClose,EMA21,SMA21
 from #EMA ema
 JOIN STOCKS snp ON snp.strTick = ema.strtick and snp.dtDate = ema.dtDate
 where 
 --ema.strtick like 'AAPL' and 
 ema.dtDate >='1/1/2022'
 ORDER BY 1,3
 
 
 
 
 /*OLD CODE*/



/*=========================*/
-- use stockdb

-- IF OBJECT_ID('tempdb..#Stock_List') IS NOT NULL DROP TABLE #Stock_List 
-- SELECT	dtDate,
		-- decClose,
		-- strTick,
		-- row_number() over (partition by strTick order by dtdate) as QuoteID
-- INTO	#Stock_list
-- FROM	snp500_test
-- ORDER BY dtDate
-- /*=========================*/
-- IF OBJECT_ID('tempdb..#TBL_EMA21_RT') IS NOT NULL BEGIN
    -- DROP TABLE #TBL_EMA21_RT
-- END
 
-- SELECT	*,
		-- CAST(NULL AS FLOAT) AS EMA21
-- INTO	#TBL_EMA21_RT
-- FROM	#Stock_List

-- /*=========================*/
-- CREATE UNIQUE CLUSTERED INDEX EMA21_IDX_RT ON #TBL_EMA21_RT (strTick, QuoteId)
-- /*=========================*/
-- IF OBJECT_ID('tempdb..#TBL_START_AVG21') IS NOT NULL BEGIN
    -- DROP TABLE #TBL_START_AVG21
-- END
-- /*=========================*/ 
-- SELECT		strTick,
			-- AVG(decClose) AS Start_Avg INTO #TBL_START_AVG21
-- FROM		#stock_List
-- WHERE		QuoteId <= 21
-- GROUP BY	strTick
-- /*=========================*/
-- DECLARE @C21 FLOAT = 2.0 / (1 + 21), @EMA21 FLOAT
-- /*=========================*/
-- UPDATE
    -- T121
-- SET
    -- @EMA21 =
        -- CASE
            -- WHEN QuoteId = 21 then T221.Start_Avg
            -- WHEN QuoteId > 21 then T121.decClose * @C21 + @EMA21 * (1 - @C21)
        -- END
    -- ,EMA21 = @EMA21 
-- FROM
    -- #TBL_EMA21_RT T121
-- JOIN
    -- #TBL_START_AVG21 T221
-- ON
    -- T121.strTick = T221.strTick
-- option (maxrecursion 0);

-- /*=========================*/

-- IF OBJECT_ID('tempdb..#EMA') IS NOT NULL DROP TABLE #EMA  
-- SELECT	rt2.strTick,
		-- rt2.QuoteId as ID,
		-- rt2.dtDate,
		-- rt2.decClose, 
		-- CAST(EMA21 AS NUMERIC(15,2)) AS EMA21,
		-- CAST(avg(rt2.decClose) OVER (PARTITION BY rt2.strtick ORDER BY rt2.dtdate ASC ROWS 20 PRECEDING) AS NUMERIC(10,2)) AS SMA21
-- INTO #EMA
-- FROM #TBL_EMA21_RT rt2
-- GROUP BY rt2.strTick,
		-- rt2.QuoteId,
		-- rt2.dtDate,
		-- rt2.decClose,
		-- EMA21


-- select snp.strTick,snp.ID,snp.dtDate,snp.decHigh,snp.decLow,snp.decOpen,snp.decClose,EMA21,SMA21
 -- from #EMA ema
 -- JOIN snp500_test snp ON snp.strTick = ema.strtick and snp.dtDate = ema.dtDate
 -- where ema.strtick like 'TSLA' and ema.dtDate >='1/1/2022'
