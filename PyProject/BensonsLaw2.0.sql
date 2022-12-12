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
/*
use stockdb

select comb.strtick,comb.NormPriceLeft1,count(*)
FROM
(
SELECT	snp.strTick,
		LEFT(ROUND((decClose-MinVal)/(CurVal/10000),2)*100,1) AS NormPriceLeft1
FROM snp500_test snp
JOIN (
SELECT	snp.strTick,
		MIN(decClose) AS MinVal,
		CurVal,
		count(*) AS TotalVals
FROM	snp500_test	snp
JOIN	(select strtick,decClose as CurVal from snp500_test where dtdate = '11/14/2019'--(select max(dtdate) from snp500_test)
		)			mv
			ON	mv.strTick = snp.strTick
GROUP BY	snp.strTick,
			mv.CurVal
) mmv
ON mmv.strTick = snp.strTick
) comb
GROUP BY comb.strtick,comb.NormPriceLeft1
*/


DECLARE @begdate as date,
		@enddate as date
SET		@enddate = '1974-10-03'
SET 	@begdate = (select max(dtdate) from snp500_test)
/*
select en.strtick, bg.endClose/en.begclose as PNL
FROM
(select strtick,decClose as begClose from snp500_test where dtDate = @enddate) en
JOIN
(select strtick,decClose as endClose from snp500_test where dtDate = @begdate) bg
ON bg.strTick = en.strTick
*/
SET 	@begdate = dateadd(year,-100,@enddate)



IF OBJECT_ID('tempdb..#priceBucket') IS NOT NULL DROP TABLE #priceBucket

select strtick, '1' as Bucket INTO #priceBucket from snp500_test group by strtick
insert into #priceBucket select strtick, '2' as Bucket  from snp500_test group by strtick
insert into #priceBucket select strtick, '3' as Bucket  from snp500_test group by strtick
insert into #priceBucket select strtick, '4' as Bucket  from snp500_test group by strtick
insert into #priceBucket select strtick, '5' as Bucket  from snp500_test group by strtick
insert into #priceBucket select strtick, '6' as Bucket  from snp500_test group by strtick
insert into #priceBucket select strtick, '7' as Bucket  from snp500_test group by strtick
insert into #priceBucket select strtick, '8' as Bucket  from snp500_test group by strtick
insert into #priceBucket select strtick, '9' as Bucket  from snp500_test group by strtick

select	pb.strtick,
		Bucket,
		count(ben.strTick) CountOfInstances
from #priceBucket pb
LEFT JOIN (

		SELECT	snp.strtick,
				dtDate,
				decClose,
				ROUND((decClose-minamt)/(maxamt/10000000),2)*100 as NormalizedPrice,
				LEFT(ROUND((decClose-minamt)/(maxamt/10000000),2)*100,1) AS NormPriceLeft1
		FROM	snp500_test snp
		JOIN	(SELECT strtick,
						min(decclose) as minamt,
						max(decclose) as maxamt
				FROM snp500_test
				WHERE dtdate between @begdate AND @enddate
				GROUP BY strtick
				) mma ON mma.strtick = snp.strtick
		WHERE	dtdate between @begdate and @enddate --and strtick like @stock
	) ben
		ON ben.strTick = pb.strTick AND ben.NormPriceLeft1 = pb.Bucket
where pb.strTick like '^%'
GROUP BY pb.strtick,
		Bucket
ORDER BY pb.strTick,bucket

/**/
/*
select dtdate,decclose
FROM snp500_test
where dtdate > '1/1/2018' and strtick like @stock
order by decclose asc
*/

/*
FIND PNL for backtest
*/
/*
declare @maxdate date, @startdate date
set @maxdate = (select max(dtdate) from snp500_test)
set @startdate = DATEADD(year,-1,@maxdate)

select	snp.strtick,
		begdate,
		snp.dtdate,
		begclose,
		snp.decClose,
		snp.decClose/begclose as PNL
from snp500_test snp
JOIN (select dtdate as begdate,strtick,decclose as begclose from snp500_test where dtdate = @startdate) beg
	ON beg.strTick = snp.strtick
where snp.dtdate = @maxdate
		--and	snp.strtick in ('NWS','NWSA','SYF','MET','AIV','COF','DOW','CBOE','AMCR','WU','PSX','TSLA','NCLH','HII','GM','ED','APTV','AJG','JPM')
 */
 /**/



select  top 1 dtdate,min(decclose) from snp500_test
where strTick like '^%' and dtdate between '01/01/1974' and '01/01/1975'
GROUP BY dtdate
ORDER BY min(decclose) asc