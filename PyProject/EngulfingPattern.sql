USE stockdb
/*
select *,
		avg(MAC21) OVER (ORDER BY dtdate ROWS 4 PRECEDING) as SigLine
FROM (
SELECT *,
		avg(PNL) OVER (ORDER BY dtdate ROWS BETWEEN 21 PRECEDING AND 1 PRECEDING) as MAC21
FROM(
select --snp.strtick,
		snp.dtdate,
		COUNT(*) as RECS,
		--snp.decClose,PrevClose,snp.decOpen,PrevOpen,PrevClose/PrevOpen as Spread,
		avg(Sell/Buy) AS PNL
FROM (
select	*,
		LAG(decOpen,1) OVER (partition by strtick order by dtdate) as PrevOpen,
		LAG(decClose,1) OVER (partition by strtick order by dtdate) as PrevClose,
		LAG(intVol,1) OVER (partition by strtick order by dtdate) as PrevVol,
		LEAD(decOpen,1) OVER (partition by strtick order by dtdate) as Buy,
		LEAD(decClose,1) OVER (partition by strtick order by dtdate) as Sell--,
		--AVG(decclose) OVER (partition by strtick order by dtdate ROWS 199 preceding) as SMA200
from snp500_test
) snp
where (snp.decOpen < PrevClose and snp.decClose > PrevOpen and PrevClose < PrevOpen and snp.decClose > snp.decOpen)
		AND dtdate  >= '01/01/2000'
		--and SMA200 > snp.decHigh----decLow
		--and snp.intVol > PrevVol
GROUP BY snp.dtdate
) pl
) mac
Order by mac.dtDate--,snp.strTick

--select dtdate,decclose from snp500_test where dtdate > '01/01/2000' and strtick like '^%' order by dtdate


select snp.strtick,
		snp.dtdate,
		decClose,
		'=RTD("tos.rtd",,"OPEN",A2)' as CurOpen,
		'=RTD("tos.rtd",,"LAST",A2)' as CurClose
FROM (
select	*,
		LAG(decOpen,1) OVER (partition by strtick order by dtdate) as PrevOpen,
		LAG(decClose,1) OVER (partition by strtick order by dtdate) as PrevClose,
		LAG(intVol,1) OVER (partition by strtick order by dtdate) as PrevVol,
		LEAD(decOpen,1) OVER (partition by strtick order by dtdate) as Buy,
		LEAD(decClose,1) OVER (partition by strtick order by dtdate) as Sell
from snp500_test
) snp
where (snp.decOpen < PrevClose and snp.decClose > PrevOpen and PrevClose < PrevOpen and snp.decClose > snp.decOpen)
		AND dtdate  = (select MAX(dtdate) from snp500_test)

*/

select snp.strtick,
		snp.dtdate,
		(snp.Sell/snp.Buy) AS PNL,
		snp.SWING,
		snp.PrevLow,
		snp.atr14,
		snp.buy,
		snp.PrevLow-snp.ATR14 as StopLoss,
		snp.Buy+((snp.Buy-(snp.PrevLow-snp.ATR14))*2) as Limit,
		StopMin20,
		LimitMax20,
		AltSell/snp.buy as AltSell
FROM (
select	np.*,atr.atr14,
		LAG(np.decOpen,1) OVER (partition by np.strtick order by np.dtdate) as PrevOpen,
		LAG(np.decLow,1) OVER (partition by np.strtick order by np.dtdate) as PrevLow,
		LAG(np.decClose,1) OVER (partition by np.strtick order by np.dtdate) as PrevClose,
		LAG(np.intVol,1) OVER (partition by np.strtick order by np.dtdate) as PrevVol,
		LEAD(np.decOpen,1) OVER (partition by np.strtick order by np.dtdate) as Buy,
		LEAD(np.decClose,1) OVER (partition by np.strtick order by np.dtdate) as Sell,
		MIN(np.decLow) OVER (partition by np.strtick order by np.dtdate ROWS BETWEEN 12 PRECEDING AND 2 PRECEDING) as SWING,
		MIN(np.decLow) OVER (partition by np.strtick order by np.dtdate ROWS BETWEEN 1 FOLLOWING and 11 FOLLOWING) as StopMin20,
		MAX(np.decHigh) OVER (partition by np.strtick order by np.dtdate ROWS BETWEEN 1 FOLLOWING and 11 FOLLOWING) as LimitMax20,
		LEAD(np.decClose,11) OVER (partition by np.strtick order by np.dtdate) as AltSell
from snp500_test np
JOIN ATR atr ON atr.dtDate = np.dtDate and atr.strTick = np.strTick
) snp
where (snp.decOpen < snp.PrevClose and snp.decClose > snp.PrevOpen and snp.PrevClose < snp.PrevOpen and snp.decClose > snp.decOpen)
		AND dtdate  >= '01/01/2000' --excluding below 61% success over 10 days 1.6% avg gain
		AND PrevLow < SWING  
		AND decHigh/decClose <1.005