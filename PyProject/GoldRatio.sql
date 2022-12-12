USE stockdb

IF OBJECT_ID('GOLD', 'u') IS NOT NULL
BEGIN
DROP TABLE GOLD
END
SELECT
	*
INTO
	GOLD
FROM
	snp500
WHERE
	strTick = 'GC=F'

IF OBJECT_ID('GOLD_RATIO', 'u') IS NOT NULL
BEGIN
DROP TABLE GOLD_RATIO
END

SELECT
	snp.dtDate,
	snp.strTick,
	snp.decOpen,
	snp.decClose,
	snp.decHigh,
	snp.decLow,
	snp.decClose/gd.decClose AS StockGoldRatio
INTO
	GOLD_RATIO
FROM
	snp500 snp
JOIN
	GOLD gd ON snp.dtDate = gd.dtDate
ORDER BY
	1,2

IF OBJECT_ID('ROC1', 'u') IS NOT NULL
BEGIN
DROP TABLE ROC1
END

SELECT
	*,
	MAX(decHigh) OVER (partition by strtick Order by dtDate ROWS BETWEEN 20 preceding and current row) Week4Max,
	MIN(decLow) OVER (partition by strtick Order by dtDate ROWS BETWEEN 20 preceding and current row) Week4Min,
	MAX(decHigh) OVER (partition by strtick Order by dtDate ROWS BETWEEN 6 preceding and current row) Week1Max,
	MIN(decLow) OVER (partition by strtick Order by dtDate ROWS BETWEEN 6 preceding and current row) Week1Min,
	(StockGoldRatio-LAG(StockGoldRatio,15) over (partition by strtick ORDER BY dtDate ASC))/StockGoldRatio AS ROC
INTO
	ROC1
FROM
	GOLD_RATIO
--WHERE
	--strTick IN ('BTC-USD')
	--strTick IN ('MSTR','MARA','HUT','ACB','AITX','AMC','ASLN','ATOS','BODY','CCI','CPB','CRDF','DEFTF','DKNG','EQR','GLD','GME','HOG','IBIO','MAA','MCOA','NERV','PKI','RF','RIOT','SLV','SNDL','SNPW','SPCE','SPXL','TNXP','TWTR','USMJ','XLRE','XRX','ZOM')
	--dtDate >= GETDATE()-100

IF OBJECT_ID('BS_IND', 'u') IS NOT NULL
BEGIN
DROP TABLE BS_IND
END
SELECT
	ROW_NUMBER()  OVER (PARTITION BY strtick ORDER BY dtDate ASC) AS ROWREF, 
	*,
	CASE
		WHEN ROC >0 AND LAG(ROC,1) OVER (partition by strtick ORDER BY dtDate ASC) < 0
		THEN Week1Max
		WHEN ROC <0 AND LAG(ROC,1) over (partition by strtick ORDER BY dtDate ASC) > 0
		THEN Week1Min
		ELSE NULL
	END AS BUY_SELL_PRICE,
	CASE
		WHEN ROC >0 AND LAG(ROC,1) OVER (PARTITION BY strtick ORDER BY dtDate ASC) < 0
		THEN 'BUY'
		WHEN ROC <0 AND LAG(ROC,1) over (partition by strtick ORDER BY dtDate ASC) > 0
		THEN 'SELL'
		ELSE NULL
	END AS BUY_SELL_INDICATOR,
	CASE
		WHEN (ROC >0 AND LAG(ROC,1) OVER (PARTITION BY strtick ORDER BY dtDate ASC) < 0) OR (ROC <0 AND LAG(ROC,1) over (partition by strtick ORDER BY dtDate ASC) > 0)
		THEN ROW_NUMBER()  OVER (PARTITION BY strtick ORDER BY dtDate ASC)
		ELSE NULL
	END AS BUY_SELL_REF
INTO
	BS_IND
FROM
	ROC1


IF OBJECT_ID('BS_IND_1', 'u') IS NOT NULL
BEGIN
DROP TABLE BS_IND_1
END
SELECT
	ROWREF,
	dtDate,
	strTick,
	decOpen,
	decClose,
	decHigh,
	decLow,
	StockGoldRatio,
	Week4Max,
	Week4Min,
	ROC,
	BUY_SELL_PRICE,
	BUY_SELL_INDICATOR,
	BUY_SELL_REF,
	MAX(BUY_SELL_REF) OVER (PARTITION BY strtick ORDER BY dtDate ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS BUY_SELL_REF_UNBOUND
INTO
	BS_IND_1
FROM
	BS_IND



IF OBJECT_ID('INDICATOR', 'u') IS NOT NULL
BEGIN
DROP TABLE INDICATOR
END
SELECT
	BS1.strTick,
	dtDate,
	BUY_SELL_PRICE,
	BUY_SELL_INDICATOR
INTO
	INDICATOR
FROM
	BS_IND_1 BS1
JOIN
	(
	SELECT
		strTick,
		MAX(dtDate) maxdate
	FROM
		BS_IND_1 BS1
	WHERE
		BUY_SELL_INDICATOR IS NOT NULL
	GROUP BY
		strTick
	) BS2 ON BS2.strTick = BS1.strTick
			AND BS2.maxdate = BS1.dtDate
WHERE
	BUY_SELL_INDICATOR IS NOT NULL


SELECT
	snp.dtDate AS MostRecentDataDate,
	ind.dtDate AS TriggerDate,
	snp.strtick,
	ind.BUY_SELL_PRICE,
	ind.BUY_SELL_INDICATOR,
	snp.decOpen,
	snp.decHigh,
	snp.decLow,
	snp.decClose,
	snp.decAdjClose

FROM
	snp500 snp
JOIN
	(
	SELECT
		strtick,
		max(dtdate) AS maxdate
	FROM
		snp500
	GROUP BY
		strtick
	) mx ON mx.maxdate = snp.dtdate
		AND mx.strTick = snp.strTick
JOIN
	INDICATOR ind ON ind.strTick = snp.strTick
ORDER BY 5,2 DESC

	/*

	SELECT
	ROWREF,
	dtDate,
	strTick,
	decOpen,
	decClose,
	decHigh,
	decLow,
	StockGoldRatio,
	Week1Max,
	Week1Min,
	ROC,
	BUY_SELL_PRICE,
	BUY_SELL_INDICATOR,
	BUY_SELL_REF,
	MAX(BUY_SELL_REF) OVER (PARTITION BY strtick ORDER BY dtDate ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS BUY_SELL_REF_UNBOUND
FROM
	BS_IND
WHERE
	strtick in ('BTC-USD')*/