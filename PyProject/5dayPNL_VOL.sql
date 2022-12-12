use stockdb

/*
PNLOO
PNLHH
PNLHL
PNLLH
PNLLL
PNLCC
*/
/*

SELECT COMB,PNL_OO_FUTURE,COUNT(*)
FROM(
SELECT *,CAST(PNL_OOHHHLLHLLCCVV + PNL_OO_FUTURE AS VARCHAR) COMB
FROM(

select  strtick,dtdate,decOpen,decHigh,decLow,decClose,
		CAST(
		CASE
		WHEN lag(decOpen,5) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(decOpen,4) OVER (partition by strtick order by dtdate)
			>lag(decOpen,5) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(decOpen,4) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(decOpen,3) OVER (partition by strtick order by dtdate)
			>lag(decOpen,4) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(decOpen,3) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(decOpen,2) OVER (partition by strtick order by dtdate)
			>lag(decOpen,3) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(decOpen,2) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(decOpen,1) OVER (partition by strtick order by dtdate)
			>lag(decOpen,2) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(decOpen,1) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN decOpen
			>lag(decOpen,1) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(intVol,5) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(intVol,4) OVER (partition by strtick order by dtdate)
			>lag(intVol,5) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(intVol,4) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(intVol,3) OVER (partition by strtick order by dtdate)
			>lag(intVol,4) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(intVol,3) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(intVol,2) OVER (partition by strtick order by dtdate)
			>lag(intVol,3) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(intVol,2) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(intVol,1) OVER (partition by strtick order by dtdate)
			>lag(intVol,2) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(intVol,1) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN intVol
			>lag(intVol,1) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		AS VARCHAR)
		AS PNL_OOHHHLLHLLCCVV,
		CAST(
		CASE
		WHEN LEAD(decOpen,1) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN LEAD(decOpen,5) OVER (partition by strtick order by dtdate)>LEAD(decOpen,1) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END
		/*+
		CASE
		WHEN LEAD(decOpen,1) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN LEAD(decOpen,2) OVER (partition by strtick order by dtdate)>LEAD(decOpen,1) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --Future OO*/
		AS VARCHAR)
		AS PNL_OO_FUTURE
FROM snp500_test
WHERE dtDate >= '01/01/2018'
) fst
WHERE PNL_OO_FUTURE not like '%N%' and PNL_OOHHHLLHLLCCVV not like '%N%'
--ORDER BY fst.strTick,fst.dtdate
) snd
GROUP BY COMB,PNL_OO_FUTURE
*/

/*
FIND SAMPLE
*/
use stockdb

select thrd.dtDate,avg(leadOpen/thrd.decOpen) AS PNL,COUNT(*) NumStocks,sp.decOpen
FROM(

SELECT *,CAST(PNL_OOHHHLLHLLCCVV + PNL_OO_FUTURE AS VARCHAR) COMB
FROM(

select  strtick,dtdate,decOpen,decHigh,decLow,decClose,LEAD(decOpen,5) OVER (partition by strtick order by dtdate) as leadOpen,
		CAST(
		CASE
		WHEN lag(decOpen,5) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(decOpen,4) OVER (partition by strtick order by dtdate)
			>lag(decOpen,5) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(decOpen,4) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(decOpen,3) OVER (partition by strtick order by dtdate)
			>lag(decOpen,4) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(decOpen,3) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(decOpen,2) OVER (partition by strtick order by dtdate)
			>lag(decOpen,3) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(decOpen,2) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(decOpen,1) OVER (partition by strtick order by dtdate)
			>lag(decOpen,2) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(decOpen,1) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN decOpen
			>lag(decOpen,1) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(intVol,5) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(intVol,4) OVER (partition by strtick order by dtdate)
			>lag(intVol,5) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(intVol,4) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(intVol,3) OVER (partition by strtick order by dtdate)
			>lag(intVol,4) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(intVol,3) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(intVol,2) OVER (partition by strtick order by dtdate)
			>lag(intVol,3) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(intVol,2) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN lag(intVol,1) OVER (partition by strtick order by dtdate)
			>lag(intVol,2) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		+
		CASE
		WHEN lag(intVol,1) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN intVol
			>lag(intVol,1) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --OO
		AS VARCHAR)
		AS PNL_OOHHHLLHLLCCVV,
		CAST(
		CASE
		WHEN LEAD(decOpen,1) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN LEAD(decOpen,5) OVER (partition by strtick order by dtdate)>LEAD(decOpen,1) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END
		/*+
		CASE
		WHEN LEAD(decOpen,1) OVER (partition by strtick order by dtdate) IS NULL THEN 'N'
		WHEN LEAD(decOpen,2) OVER (partition by strtick order by dtdate)>LEAD(decOpen,1) OVER (partition by strtick order by dtdate)
		THEN '1'
		ELSE '0'
		END --Future OO*/
		AS VARCHAR)
		AS PNL_OO_FUTURE
FROM snp500_test
WHERE dtDate between '01/01/2015' and '01/01/2018'
) fst
WHERE PNL_OO_FUTURE not like '%N%' and PNL_OOHHHLLHLLCCVV not like '%N%'
		and PNL_OOHHHLLHLLCCVV in ('1110011110'
,'0100111101'
,'0101001001'
,'1100101101'
,'0101011011'
,'0000100101'
,'0011000000'
,'1101011110'
,'1100111110'
,'0110111111'
,'1010111110'
,'0101011001'
,'1000011110'
,'1000001010'
,'0110011111'
,'1110001111'
,'1001011110'
,'0101111100'
,'1000011010'
,'0100101101'
,'1110111111'
)
) thrd
RIGHT JOIN (SELECT dtdate,strtick,decOpen FROM snp500_test WHERE dtDate between '01/01/2015' and '01/01/2018' AND strtick in ('spy')) sp ON sp.dtdate = thrd.dtDate
GROUP BY thrd.dtDate,sp.decOpen
ORDER BY thrd.dtdate