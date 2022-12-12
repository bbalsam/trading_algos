use stockdb

DECLARE @endDate date
SET @endDate = '11/01/2020'

SELECT	strtick,
		AVG(PNL) as AVGPNL,
		AVG(WinLoss) as WINRT
FROM (
	SELECT	strtick,
			decClose/lag(decClose,1) OVER (PARTITION BY strtick ORDER BY dtdate) as PNL,
			CASE
				WHEN decClose/lag(decClose,1) OVER (PARTITION BY strtick ORDER BY dtdate) > 1
				THEN 1.0
				ELSE 0.0
			END as WinLoss
	FROM	snp500_test
	WHERE dtdate between dateadd(month,-1,@endDate) AND @endDate
	) snp
GROUP BY
		strtick
ORDER BY
		AVG(WinLoss) DESC


SELECT trd.strTick,svnth.decClose/trd.decClose as PNL
FROM
	(
	SELECT	strtick,
			fst.dtdate,
			decclose
	FROM	snp500_test fst
	JOIN	(SELECT MIN(dtdate) as dtdate FROM snp500_test WHERE dtdate between @endDate AND dateadd(month,1,@endDate)) snd ON snd.dtdate = fst.dtDate
	WHERE	fst.dtdate between @endDate AND dateadd(month,1,@endDate)
	) trd
JOIN (
SELECT *
FROM
	(
	SELECT	strtick,
			fort.dtdate,
			decclose
	FROM	snp500_test fort
	JOIN	(SELECT MAX(dtdate) as dtdate FROM snp500_test WHERE dtdate between @endDate AND dateadd(month,1,@endDate)) fth ON fth.dtdate = fort.dtDate
	WHERE	fort.dtdate between @endDate AND dateadd(month,1,@endDate)
	) sxth ) svnth
	ON svnth.strTick = trd.strTick