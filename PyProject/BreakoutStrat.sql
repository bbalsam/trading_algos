Declare @highlow as decimal(4,3)
Declare @highopen as decimal(4,3)
SET @highlow = 1.015--1.015
SET @highopen = 1.002--1.005

SELECT /*strtick,
		dtdate,*/
		POWER(EXP(SUM(LOG(COALESCE(calc.PNL1Week, 1)))),1/cast(count(*) as float)) AS DailyAvgPNL,
		count(*) as NumberofRecords
			--PNL1Week,PNL2Week,PNL1Month--,decOpen,decHigh,decLow,decAdjClose
FROM
(select strtick,dtDate,decOpen,decHigh,decLow,decClose,decAdjClose,intVol,
		CASE
			WHEN	(decHigh/decLow) >= @highlow
				AND	(decHigh/decOpen) <= @highopen
				AND	decAdjClose > decOpen
				AND decAdjClose > LAG(decAdjClose,4) OVER (partition by strTick order by dtdate ASC)
			THEN 1
			ELSE 0

		END AS BULLISHBREAKOUT,
		LEAD(decAdjClose,5) OVER (partition by strTick order by dtdate ASC)/LEAD(decOpen,1) OVER (partition by strTick order by dtdate ASC)
		AS PNL1Week,
		LEAD(decAdjClose,10) OVER (partition by strTick order by dtdate ASC)/LEAD(decOpen,1) OVER (partition by strTick order by dtdate ASC)
		AS PNL2Week,
		LEAD(decAdjClose,21) OVER (partition by strTick order by dtdate ASC)/LEAD(decOpen,1) OVER (partition by strTick order by dtdate ASC)
		AS PNL1Month
from snp500_test
where dtdate > '2015-01-01' --and strtick in ('UAL')
) Calc
where BULLISHBREAKOUT = 1
		AND (PNL1Week is not null AND PNL2Week is not null AND PNL1Month is not null)
/*GROUP BY strtick,dtdate
order by POWER(EXP(SUM(LOG(COALESCE(calc.PNL1Week, 1)))),1/cast(count(*) as float)),strtick,
dtDate asc*/

