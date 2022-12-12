USE Stock;

Declare @tick varchar(10)
Declare @maxdate date
Declare @daysback decimal
SET @tick = '^GSPC'
SET @maxdate = (select MAX(dtdate) FROM stocks where strtick = @tick)
SET @daysback = -18000;

WITH STEP1 AS
(
SELECT
	dtDate
	,strTick
	,decHigh
	,decLow
	,decOpen
	,decClose
	,decAdjClose
	,(decHigh+decLow+decAdjClose)/3 AS TypicalPrice
	,((decHigh+decLow+decAdjClose)/3)*intVol AS TypcialPricexVol
	,intVol
	,SUM(CAST(intVol as decimal)) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS CUMVOL12
	,SUM(CAST(intVol as decimal)) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS CUMVOL20
	,SUM(CAST(intVol as decimal)) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) AS CUMVOL26
	,SUM(CAST(intVol as decimal)) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS CUMVOL50
	,SUM(CAST(intVol as decimal)) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS CUMVOL100
	,SUM(CAST(intVol as decimal)) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS CUMVOL200
	,LAG(decAdjClose,1) OVER (PARTITION BY strTick ORDER BY dtDate) AS PREV_CLOSE
FROM
	STOCKS
WHERE
	dtDate > DATEADD(DAY,@daysback,@maxdate)
	--AND strTick = @tick
GROUP BY
	dtDate
	,strTick
	,decHigh
	,decLow
	,decOpen
	,decClose
	,decAdjClose
	,intVol
/*ORDER BY
	dtDate ASC*/
)

,STEP2 AS
(
SELECT
*
,SUM(TypcialPricexVol) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS CUMTPA12
,SUM(TypcialPricexVol) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS CUMTPA20
,SUM(TypcialPricexVol) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) AS CUMTPA26
,SUM(TypcialPricexVol) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS CUMTPA50
,SUM(TypcialPricexVol) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS CUMTPA100
,SUM(TypcialPricexVol) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS CUMTPA200
,CASE
	WHEN decHigh-decLow > ABS(decHigh - PREV_CLOSE)
		AND decHigh-decLow > ABS(decLow - PREV_CLOSE)
	THEN decHigh-decLow
	WHEN ABS(decHigh - PREV_CLOSE) > decHigh-decLow
		AND ABS(decHigh - PREV_CLOSE) > ABS(decLow - PREV_CLOSE)
	THEN ABS(decHigh - PREV_CLOSE)
	ELSE ABS(decLow - PREV_CLOSE)
END AS TRUE_RANGE
FROM
STEP1
)

,STEP3 AS
(
SELECT
*
,CASE WHEN CUMVOL12 = 0
	THEN 0
	ELSE CUMTPA12/CUMVOL12
END AS VWAP12
,CASE WHEN CUMVOL20 = 0
	THEN 0
	ELSE CUMTPA20/CUMVOL20
END AS VWAP20
,CASE WHEN CUMVOL26 = 0
	THEN 0
	ELSE CUMTPA26/CUMVOL26
END AS VWAP26
,CASE WHEN CUMVOL200 = 0
	THEN 0
	ELSE CUMTPA50/CUMVOL50
END AS VWAP50
,CASE WHEN CUMVOL200 = 0
	THEN 0
	ELSE CUMTPA100/CUMVOL100
END AS VWAP100
,CASE WHEN CUMVOL200 = 0
	THEN 0
	ELSE CUMTPA200/CUMVOL200
END AS VWAP200
,AVG(TRUE_RANGE) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ATR
FROM
STEP2
)

,STEP4 AS
(
SELECT
*
,VWAP20+(2*STDEV(decAdjClose) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) AS VWAP_BOLL_HIGH
,VWAP20-(2*STDEV(decAdjClose) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) AS VWAP_BOLL_LOW
,VWAP20+(2*ATR) AS VWAP_KELT_HIGH
,VWAP20-(2*ATR) AS VWAP_KELT_LOW
,(VWAP20+(2*STDEV(decAdjClose) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)))/
	(VWAP20-(2*STDEV(decAdjClose) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)))-
	(VWAP20+(2*ATR))/(VWAP20-(2*ATR)) AS SQUEEZE_SIGNAL
,CASE
	WHEN VWAP26 = 0 OR VWAP26 IS NULL
	THEN NULL
	ELSE (VWAP12/VWAP26)-1
END AS MACD_VWAP
FROM
STEP3
)
,STEP5 AS
(
SELECT
	dtDate
	,strTick
	,decOpen
	,decHigh
	,decLow
	,decClose
	,intVol
	,decAdjClose
	,VWAP20
	,VWAP_BOLL_HIGH
	,VWAP_BOLL_LOW
	,VWAP_KELT_HIGH
	,VWAP_KELT_LOW
	,VWAP50
	,VWAP100
	,VWAP200
	,SQUEEZE_SIGNAL
	,VWAP12
	,VWAP26
	,MACD_VWAP
	,AVG(MACD_VWAP) OVER (PARTITION BY strTick ORDER BY dtDate ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS MACD_VWAP_SIGNAL_LINE
FROM
STEP4
)
,STEP6 AS
(
SELECT
	*
	,LAG(MACD_VWAP) OVER (PARTITION BY strTick ORDER BY dtDate) AS PREV_MACD
	,LAG(MACD_VWAP_SIGNAL_LINE) OVER (PARTITION BY strTick ORDER BY dtDate) AS PREV_SIG_LINE
FROM
	STEP5
/*WHERE
	dtDate >= @maxdate
	AND SQUEEZE_SIGNAL < 0
	AND decAdjClose > VWAP200
	AND MACD_VWAP < 0
	AND MACD_VWAP > MACD_VWAP_SIGNAL_LINE*/
)
,STEP7 AS
(
SELECT
	*
FROM
	STEP6
WHERE
	dtDate >= @maxdate
	AND PREV_SIG_LINE > PREV_MACD
	AND MACD_VWAP_SIGNAL_LINE < MACD_VWAP
	AND SQUEEZE_SIGNAL < 0
)

SELECT
	strTick
	,dtDate
	,decClose
	,VWAP50
	,VWAP100
	,VWAP200
	,CASE
		WHEN VWAP50 = 0 OR VWAP50 IS NULL
		THEN NULL
		ELSE (decClose/VWAP50)-1
	END AS CloseVWAP50PCT
	,CASE
		WHEN VWAP100 = 0 OR VWAP100 IS NULL
		THEN NULL
		ELSE (decClose/VWAP100)-1
	END AS CloseVWAP100PCT
	,CASE
		WHEN VWAP200 = 0 OR VWAP200 IS NULL
		THEN NULL
		ELSE (decClose/VWAP200)-1
	END AS CloseVWAP200PCT
FROM STEP6
WHERE strTick = @tick
ORDER BY dtDate
/**/

/*
SELECT
	strTick
	,dtDate	
	,decAdjClose	
	,MACD_VWAP	
	,MACD_VWAP_SIGNAL_LINE	
	,VWAP20
	,VWAP200	
	,VWAP_BOLL_HIGH	
	,VWAP_BOLL_LOW	
	,VWAP_KELT_HIGH	
	,VWAP_KELT_LOW	
	,SQUEEZE_SIGNAL	
	,VWAP12	
	,VWAP26	
	,VWAP26

FROM
	STEP5
WHERE
	dtDate >= DATEADD(month, DATEDIFF(month, 0, DATEADD(DAY,@daysback,@maxdate)), 0)
	AND 
	strTick = 'BBY'
*/

/*
SELECT
*
FROM
	STEP6;*/
/*
SELECT
	strTick
	,dtDate
	,decAdjClose
	,MACD_VWAP
	,MACD_VWAP_SIGNAL_LINE
	,VWAP20
	,VWAP200
	,VWAP_BOLL_HIGH
	,VWAP_BOLL_LOW
	,VWAP_KELT_HIGH
	,VWAP_KELT_LOW
	,SQUEEZE_SIGNAL*100 AS SQZ
	,VWAP12
	,VWAP26
FROM
	STEP5
WHERE
	dtDate >= DATEADD(month, DATEDIFF(month, 0, DATEADD(DAY,@daysback,@maxdate)), 0)
	AND strTick = 'GD'
*/
-- SELECT 117.0000/82.0000	
/**/