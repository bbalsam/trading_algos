USE stockdb
declare @startdate as date
set @startdate = '01/01/2000'


IF OBJECT_ID('Q1', 'u') IS NOT NULL
BEGIN
DROP TABLE Q1
END
SELECT
	snp500.*,
	LAG(decadjClose,1) OVER (PARTITION BY snp500.strTick ORDER BY dtDate ASC) AS PrevClose
INTO
	Q1
FROM
	snp500
JOIN
	(SELECT strTick FROM snp500 WHERE dtdate <= DATEADD(DAY, -365.25*5, @startdate) GROUP BY strTick) sl ON sl.strTick = snp500.strTick
JOIN
	CompanyList CL on CL.strTick = snp500.strTick

IF OBJECT_ID('Q2', 'u') IS NOT NULL
BEGIN
DROP TABLE Q2
END
SELECT
	*,
	decadjClose/PrevClose AS DOD_CHG
INTO
	Q2
FROM
	Q1
WHERE
	Q1.PrevClose IS NOT NULL
	AND Q1.dtDate >= DATEADD(DAY, -365.25*5, @startdate)
	AND Q1.dtDate <= @startdate

IF OBJECT_ID('Q3', 'u') IS NOT NULL
BEGIN
DROP TABLE Q3
END

SELECT TOP 20
	strTick,
	AVG(DOD_CHG) AvgDOD_CHG
INTO
	Q3
FROM
	Q2
GROUP BY
	strTick
ORDER BY
	2 DESC

IF OBJECT_ID('Q4', 'u') IS NOT NULL
BEGIN
DROP TABLE Q4
END
SELECT
	strtick,
	MAX(Dtdate) AS MAXDATE,
	MIN(dtdate) AS MINDATE
INTO
	Q4
FROM
	Q1
WHERE
	 Q1.dtDate >  @startdate
	AND Q1.dtDate <= DATEADD(DAY, 365.25, @startdate)
GROUP BY
	strTick

IF OBJECT_ID('Q5', 'u') IS NOT NULL
BEGIN
DROP TABLE Q5
END
SELECT
	snp.strtick,
	Q3.AvgDOD_CHG,
	SUM(CASE
		WHEN snp.dtdate = Q4.MINDATE
		THEN snp.decClose
		ELSE 0
	END) AS MINDATE_CLOSE,
	SUM(CASE
		WHEN snp.dtdate = Q4.MAXDATE
		THEN snp.decClose
		ELSE 0
	END) AS MAXDATE_CLOSE,
	SUM(CASE
		WHEN snp.dtdate = Q4.MAXDATE
		THEN snp.decClose
		ELSE 0
	END)/
	SUM(CASE
		WHEN snp.dtdate = Q4.MINDATE
		THEN snp.decClose
		ELSE 0
	END) AS GAIN_LOSS
INTO
	Q5
FROM
	snp500 snp
JOIN
	Q4 ON Q4.strTick = snp.strTick
JOIN
	Q3 ON Q3.strTick = snp.strTick
GROUP BY
	snp.strTick,
	Q3.AvgDOD_CHG
	

SELECT
	AVG(GAIN_LOSS) AS YOY_GAIN_LOSS
FROM
	Q5

SELECT
	*
FROM
	Q5
ORDER BY
	5

