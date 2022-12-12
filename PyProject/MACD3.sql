use stockdb

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
IF OBJECT_ID('tempdb..#Stochastic') IS NOT NULL  DROP TABLE #Stochastic 
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
JOIN #Stochastic sto ON sto.ID = ma.ID) ct
where ct.dtDate > = '1/1/2018'
		AND(ct.EMA200 < ct.decLow)
		AND(ct.MACDSignalLine < 0)
		AND(ct.MACDSignalLine < ct.MACD)
		AND(ct.PriorMACDSignalLine > ct.PriorMACD)
		AND(PriorDSLOW < DSLOW)
Order by ct.strTick, ct.dtdate



/* LIST of Triggers and PNL%*/
/*

Declare @ATR float
Declare @vari float
SET @ATR = 0.025
SET @vari = 1.5

select sp.*,PriorDSLOW,DSLOW
from #MACDList ml
JOIN(select snp.strTick,snp.dtDate,
	CASE
	/* 1 DAY */
	WHEN	lead(snp.decLow,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
			<= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1-@ATR))
			THEN POWER((1-@ATR),(1/1.0))--(1-@ATR)--
		WHEN	lead(snp.decHigh,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
				>= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1+(@ATR*@vari)))
				THEN POWER((1+(@ATR*@vari)),(1/1.0))--(1+(@ATR*@vari))--
	/* 2 DAY */
	WHEN	lead(snp.decLow,2) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
			<= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1-@ATR))
			THEN POWER((1-@ATR),(1/2.0))--(1-@ATR)--
		WHEN	lead(snp.decHigh,2) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
				>= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1+(@ATR*@vari)))
				THEN POWER((1+(@ATR*@vari)),(1/2.0))--(1+(@ATR*@vari))--
	/* 3 DAY */
	WHEN	lead(snp.decLow,3) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
			<= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1-@ATR))
			THEN POWER((1-@ATR),(1/3.0))--(1-@ATR)--
		WHEN	lead(snp.decHigh,3) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
				>= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1+(@ATR*@vari)))
				THEN POWER((1+(@ATR*@vari)),(1/3.0))--(1+(@ATR*@vari))--
	/* 4 DAY */
	WHEN	lead(snp.decLow,4) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
			<= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1-@ATR))
			THEN POWER((1-@ATR),(1/4.0))--(1-@ATR)--
		WHEN	lead(snp.decHigh,4) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
				>= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1+(@ATR*@vari)))
				THEN POWER((1+(@ATR*@vari)),(1/4.0))--(1+(@ATR*@vari))--
	/* 5 DAY */
	WHEN	lead(snp.decLow,5) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
			<= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1-@ATR))
			THEN POWER((1-@ATR),(1/5.0))--(1-@ATR)--
		WHEN	lead(snp.decHigh,5) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
				>= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1+(@ATR*@vari)))
				THEN POWER((1+(@ATR*@vari)),(1/5.0))--(1+(@ATR*@vari))--
	/* 6 DAY */
	WHEN	lead(snp.decLow,6) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
			<= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1-@ATR))
			THEN POWER((1-@ATR),(1/6.0))--(1-@ATR)--
		WHEN	lead(snp.decHigh,6) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
				>= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1+(@ATR*@vari)))
				THEN POWER((1+(@ATR*@vari)),(1/6.0))--(1+(@ATR*@vari))--
	/* 7 DAY */
	WHEN	lead(snp.decLow,7) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
			<= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1-@ATR))
			THEN POWER((1-@ATR),(1/7.0))--(1-@ATR)--
		WHEN	lead(snp.decHigh,7) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
				>= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1+(@ATR*@vari)))
				THEN POWER((1+(@ATR*@vari)),(1/7.0))--(1+(@ATR*@vari))--
	/* 8 DAY */
	WHEN	lead(snp.decLow,8) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
			<= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1-@ATR))
			THEN POWER((1-@ATR),(1/8.0))--(1-@ATR)
		WHEN	lead(snp.decHigh,8) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
				>= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1+(@ATR*@vari)))
				THEN POWER((1+(@ATR*@vari)),(1/8.0))--(1+(@ATR*@vari))
	/* 9 DAY */
	WHEN	lead(snp.decLow,9) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
			<= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1-@ATR))
			THEN POWER((1-@ATR),(1/9.0))--(1-@ATR)
		WHEN	lead(snp.decHigh,9) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) 
				>= (lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) * (1+(@ATR*@vari)))
				THEN POWER((1+(@ATR*@vari)),(1/9.0))--(1+(@ATR*@vari))
	ELSE POWER(lead(snp.decAdjClose,10) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC)
			/ lead(snp.decOpen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC),1/10.0)
	END as PNLPctGain,
	lead(snp.decopen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) AS NxtOpen,
	lead(snp.decAdjClose,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) AS NxtClose,
	lead(snp.decAdjClose,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC)/lead(snp.decopen,1) OVER (PARTITION BY snp.strTick ORDER BY snp.dtdate ASC) as PNL
from snp500_test snp
where snp.dtdate >= '2020-01-01') sp
		ON sp.dtDate = ml.dtDate AND sp.strTick = ml.strTick
		
*/

select * from #MACDList
where dtDate = (select max(dtdate) from snp500_test)
order by dtdate asc
/**/
