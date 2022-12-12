IF OBJECT_ID('tempdb..#Stock_List') IS NOT NULL DROP TABLE #Stock_List 
SELECT	ID,
		strTick,
		dtdate,
		decOpen,
		decHigh,
		decLow,
		decAdjClose,
		row_number() over (partition by strTick order by dtdate) as QuoteID
INTO	#Stock_list
FROM	snp500_test
ORDER BY dtDate
/*=========================*/
IF OBJECT_ID('tempdb..#TBL_EMAFast_RT') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMAFast_RT
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMAFast
INTO	#TBL_EMAFast_RT
FROM	#Stock_List
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
IF OBJECT_ID('tempdb..#TBL_EMASlow_RT') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMASlow_RT
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMASlow
INTO	#TBL_EMASlow_RT
FROM	#Stock_List
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
IF OBJECT_ID('tempdb..#TBL_EMA200') IS NOT NULL BEGIN
    DROP TABLE #TBL_EMA200
END
 
SELECT	*,
		CAST(NULL AS FLOAT) AS EMA200
INTO	#TBL_EMA200
FROM	#Stock_List
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
FROM		#stock_List
WHERE		QuoteId <= 12
GROUP BY	strTick
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
SELECT		strTick,
			AVG(decAdjClose) AS Start_Avg
INTO #TBL_START_AVGSlow
FROM		#stock_List
WHERE		QuoteId <= 26
GROUP BY	strTick
/*<<<<<<<<<<<<<<<<<<<<<<<<<<*/
SELECT		strTick,
			AVG(decAdjClose) AS Start_Avg
INTO #TBL_START_EMA200
FROM		#stock_List
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

#Stock_List = #PREMACD
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
Select	strTick,dtDate,decAdjClose,LEAD(decOpen,1) OVER (PARTITION BY strTick ORDER BY dtdate ASC) AS decOpen,dechigh,declow,EMAFast,EMASlow,EMA200,MACD,MACDSignalLine,
		LAG(MACDSignalLine,1) OVER (PARTITION BY strTick ORDER BY dtdate ASC) AS PriorMACDSignalLine,
		LAG(MACD,1) OVER (PARTITION BY strTick ORDER BY dtdate ASC) AS PriorMACD
from #MACD) ct
where ct.dtDate > = '1/1/2018'
		AND(ct.EMA200 > ct.decLow)
		AND(ct.MACDSignalLine < 0)
		AND(ct.MACDSignalLine < ct.MACD)
		AND(ct.PriorMACDSignalLine > ct.PriorMACD)
Order by ct.strTick, ct.dtdate


declare @LimitGain as decimal(12,6)
declare @StopLoss as decimal(12,6)
SET @LimitGain = 1.14
SET @StopLoss = 1-(((@LimitGain-1)/3)*2)



select * from (
select  macd.dtDate as BuyDate,
		snp.dtdate as SellDate,
		snp.strtick,
		macd.decOpen,
		CASE WHEN snp.decLow <= (macd.decOpen*@StopLoss)
		THEN macd.decOpen*@StopLoss
		WHEN snp.decHigh >= (macd.decOpen*@LimitGain)
		THEN macd.decOpen*@LimitGain
		ELSE NULL
		END as SellPrice,
		--POWER((snp.decHigh/macd.decOpen),(1/cast(datediff(day,macd.dtDate,snp.dtdate) as decimal))) as DailyPNLPct,
		CASE WHEN snp.decLow <= (macd.decOpen*@StopLoss)
		THEN 0
		WHEN snp.decHigh >= (macd.decOpen*@StopLoss)
		THEN 1
		ELSE NULL
		END	as WinLoss,
		macd.EMA200HL,
		ROW_NUMBER() OVER(PARTITION BY macd.dtDate,  snp.strtick
                                 ORDER BY snp.dtdate asc) as rk/*,
		macd.**/
from snp500_test snp
JOIN (select *	from #MACDList) macd
		ON  snp.strTick = macd.strtick and snp.dtdate >= macd.dtdate and (snp.decHigh >= (macd.decOpen*@LimitGain) or snp.decLow <= (macd.decOpen*@StopLoss))
) as s
where s.rk = 1
ORDER BY s.BuyDate asc