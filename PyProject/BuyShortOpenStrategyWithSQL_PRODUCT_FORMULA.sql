/*
select strTick,dtdate,decOpen,decHigh,decLow,decAdjClose from snp500_test
where strTick in ('AAPL')
order by dtdate asc
*/
/* ORIGINAL*/
/*
select bb.strtick,DATEPART(Year,dtdate) as Year,POWER(EXP(SUM(LOG(COALESCE(bb.PNLOrderPlaced, 1)))),1/cast(count(*) as float)) AS DailyAvgPNL
FROM
(SELECT strTick,dtdate,decOpen,decHigh,decLow,decAdjClose,LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC) as PrevClose,
		CASE
/*11*/		WHEN decOpen >= LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC)
				AND LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) >= LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN '11'
/*10*/		WHEN decOpen >= LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC)
				AND LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) < LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN '10'
/*01*/		WHEN decOpen < LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC)
				AND LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) >= LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN '01'
/*00*/		WHEN decOpen < LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC)
				AND LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) < LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN '00'
/*NA*/		ELSE 'NA'
			END AS OrderPlaced,
		CASE
/*1*/		WHEN LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) >= LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN decOpen/LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC)
/*0*/		WHEN LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) < LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC)/decOpen
/*NA*/		ELSE NULL
			END AS PNLOrderPlaced
FROM snp500_test
where decAdjClose <> 0 AND decOpen <> 0
		--and strtick like 'TSLA'
		and dtdate between '2000-01-01' and '2021-01-01') bb
GROUP BY bb.strtick,DATEPART(Year,dtdate)
ORDER BY bb.strtick,DATEPART(Year,dtdate),POWER(EXP(SUM(LOG(COALESCE(bb.PNLOrderPlaced, 1)))),1/cast(count(*) as float)) desc
*/
/*
BEGIN TRAN
delete from snp500_test where strTick in ('HWM') and dtdate < '2020-04-01'
--commit
*/

select bb.strtick,dtdate,decAdjClose/LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC) as PNL,bb.PNLOrderPlaced AS DailyPNL,
		CASE
			WHEN bb.PNLOrderPlaced >= 1 THEN 1
			ELSE 0
		END as Success
FROM
(SELECT strTick,dtdate,decOpen,decHigh,decLow,decAdjClose,LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC) as PrevClose,
		CASE
/*11*/		WHEN decOpen >= LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC)
				AND LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) >= LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN '11'
/*10*/		WHEN decOpen >= LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC)
				AND LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) < LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN '10'
/*01*/		WHEN decOpen < LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC)
				AND LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) >= LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN '01'
/*00*/		WHEN decOpen < LAG(decAdjClose,1) OVER (partition by strTick order by dtdate ASC)
				AND LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) < LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN '00'
/*NA*/		ELSE 'NA'
			END AS OrderPlaced,
		CASE
/*1*/		WHEN LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) >= LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN decOpen/LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC)
/*0*/		WHEN LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC) < LAG(decAdjClose,2) OVER (partition by strTick order by dtdate ASC)
			THEN LAG(decOpen,1) OVER (partition by strTick order by dtdate ASC)/decOpen
/*NA*/		ELSE NULL
			END AS PNLOrderPlaced
FROM snp500_test
where decAdjClose <> 0 AND decOpen <> 0
		and strtick like 'TSLA'
		and dtdate between '2000-01-01' and '2021-01-01') bb
ORDER BY dtdate