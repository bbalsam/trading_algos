/*
select	strTick,
		dtDate,
		decClose,
		left(decClose,1) as Benford
from snp500_test
where strTick like '^%'
		--AND dtdate between '01/01/1930' and '01/01/1970'
ORDER BY dtDate asc
*/
use stockdb
DECLARE @begdate as date, @enddate as date, @minAmt as float, @maxAmt as float,@stock as varchar(32)
/*SET @begdate = '01/01/2010'
SET @enddate = dateadd(day,-1,dateadd(year,10,@begdate))*/
SET @stock = '^%'
SET @enddate = '01/03/2001'
SET @begdate = dateadd(year,-6,@enddate)


SET @minAmt = (SELECT MIN(decClose) from snp500_test where strTick like @stock AND dtdate between @begdate and @enddate GROUP BY strTick)
select @minAmt
SET @maxAmt = (SELECT decClose from snp500_test where strTick like @stock AND dtdate = @enddate)--(select max(dtdate) from snp500_test))--MAX(decClose) from snp500_test where strTick like @stock AND dtdate between @begdate and @enddate GROUP BY strTick)
select @maxAmt

select --LEFT(RIGHT(Year(ben.dtdate),2),1)+'0' as Decade,
		--@pres as President,
		ben.NormPriceLeft1,count(*) CountOfPrice
FROM
(
SELECT	strtick,
		dtDate,
		decClose,
		@minAmt as minamt,
		@maxAmt as maxamt,
		ROUND((decClose-@minAmt)/(@maxAmt/10000),2)*100 as NormalizedPrice,
		LEFT(ROUND((decClose-@minAmt)/(@maxAmt/10000),2)*100,1) AS NormPriceLeft1
FROM	snp500_test
WHERE	dtdate between @begdate and @enddate and strtick like @stock
) ben
WHERE ben.NormPriceLeft1 <> 0
GROUP BY ben.NormPriceLeft1--,LEFT(RIGHT(Year(ben.dtdate),2),1)+'0'
ORDER BY ben.NormPriceLeft1 asc
/*
select dtdate,decclose
FROM snp500_test
where dtdate > '1/1/2018' and strtick like @stock
order by decclose asc*/