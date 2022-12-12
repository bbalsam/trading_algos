/*
JdK RS-Ratio

Reading what I have, I can only offer a guess.

1: Let's say you're looking at 9 sectors compared to $SPX on a daily chart. Foreach sector, compute relative closing price: 100 * Sector/$SPX

2: It looks like the RS-Ratio is averaged over 14 periods. I say 14 because stockcharts.com shows RS-Ratio peaking after a lag (2-3wks), despite price peaking 2-3 weeks earlier. I use 14 because that's a common number in TA.

3: RS-Momentum looks like it's simply the rate-of-change of the calculation in #1. Indeed, stockcharts.com says exactly this: "RS-Momentum is an indicator that measures the momentum (rate-of-change) of RS-Ratio."

4: When they talk about normalizing, compute the mean & stddev of the 9 calculations in #1, then normalize as ... 100 * ((value-mean)/stddev + 1). I would guess that these values are "normalized" per day. I would guess that a separate normalization would be required for the values from #3 as well.

That's how I would approach the problem.

I consulted: http://stockcharts.com/school/doku.php?st=rrg&id=chart_school:technical_indicators:rrg_relative_strength in formulating my response, and I've had a few months to sleep on it.

*/

use stockdb

SELECT	dtDate,
		((RSRatio-RSRMean)/RSRSTD+1) as RSRatioFinal,
		((RSMomentum-RSMMean)/RSMSTD+1) as RSMomentumFinal,
		SNDClose

FROM
(
	SELECT	*,
			AVG(RSRatio) OVER (PARTITION BY SNDTick ORDER BY dtDate ROWS 20 PRECEDING) as RSRMean,
			STDEV(RSRatio) OVER (PARTITION BY SNDTick ORDER BY dtDate ROWS 20 PRECEDING) as RSRSTD,
			AVG(RSMomentum) OVER (PARTITION BY SNDTick ORDER BY dtDate ROWS 20 PRECEDING) as RSMMean,
			STDEV(RSMomentum) OVER (PARTITION BY SNDTick ORDER BY dtDate ROWS 20 PRECEDING) as RSMSTD
	FROM
	(
		SELECT	dtDate,
				SPYName,
				SPYClose,
				SNDTick,
				SNDClose,
				AVG(DRatio) OVER (PARTITION BY SNDTick ORDER BY dtDate ROWS 20 PRECEDING) as RSRatio,
				DRatio/LAG(DRatio,5) OVER (PARTITION BY SNDTick ORDER BY dtDate) as RSMomentum
		FROM
		(
			SELECT	spy.dtdate as dtDate,
					spy.strTick as SPYName,
					spy.decClose as SPYClose,
					snd.strTick as SNDTick,
					snd.decClose as SNDClose,
					100*(snd.decClose/spy.decClose) as DRatio
			FROM	(select * from snp500_test where strTick in ('^GSPC') and dtDate >= '01/01/2020') spy
					JOIN (select * from snp500_test where strTick in ('GM') and dtDate >= '01/01/2020') snd ON snd.dtDate = spy.dtDate
		) thrd
	) frth
) fth
WHERE ((RSMomentum-RSMMean)/RSMSTD+1) is not null