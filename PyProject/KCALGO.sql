use stockdb
SELECT dtDate,AVG(futPNL),AVG(KC),COUNT(*)
FROM
(
SELECT *, row_number() over (partition by dtdate order by PstPNL asc) as RowNum
FROM(
SELECT *,
		/*=IF(100*(((Ratio*PctWinRate)-(1-PctWinRate)/Ratio))<=0,0,100*(((Ratio*PctWinRate)-(1-PctWinRate)/Ratio)))*/
		CASE 
			WHEN 100*(((Ratio*PctWinRate)-(1-PctWinRate)/Ratio))<=0
			THEN 0
			ELSE 100*(((Ratio*PctWinRate)-(1-PctWinRate)/Ratio))
		END AS KC
FROM(
SELECT *,
		/*=IFERROR(((SUMPRODUCT(I14:I23,G14:G23)/SUM(G14:G23))-1)/(1-(SUMPRODUCT(J14:J23,H14:H23)/SUM(H14:H23))),100)*/
		((SUM(PosPNL*PosintVol) OVER (PARTITION BY strTick ORDER BY dtDate ROWS 20 PRECEDING)
		/SUM(PosintVol) OVER (PARTITION BY strTick ORDER BY dtDate ROWS 20 PRECEDING))-1)
		/
		(1-(SUM(NegPNL*NegintVol) OVER (PARTITION BY strTick ORDER BY dtDate ROWS 20 PRECEDING)
		/SUM(NegintVol) OVER (PARTITION BY strTick ORDER BY dtDate ROWS 20 PRECEDING)))
		AS Ratio,
		/*=COUNT(I3:I23)/COUNT(I4:J23)*/
		CASE
			WHEN (count(PosPNL) OVER (PARTITION BY strTick ORDER BY dtDate ROWS 20 PRECEDING) +
				count(NegPNL) OVER (PARTITION BY strTick ORDER BY dtDate ROWS 20 PRECEDING)) = 0
			THEN NULL
			ELSE (count(PosPNL) OVER (PARTITION BY strTick ORDER BY dtDate ROWS 20 PRECEDING)*1.0)/
				((count(PosPNL) OVER (PARTITION BY strTick ORDER BY dtDate ROWS 20 PRECEDING)*1.0) +
				(count(NegPNL) OVER (PARTITION BY strTick ORDER BY dtDate ROWS 20 PRECEDING)*1.0))
		END AS PctWinRate
FROM
(
select	strtick,dtdate,decOpen,decClose, intVol,
		CASE
			WHEN LEAD(decOpen,1) OVER (PARTITION BY strTick ORDER BY dtDate) IS NULL
			THEN NULL
			ELSE LEAD(decOpen,2) OVER (PARTITION BY strTick ORDER BY dtDate)/LEAD(decOpen,1) OVER (PARTITION BY strTick ORDER BY dtDate)
		END as FutPNL,
		CASE
			WHEN LAG(decOpen,2) OVER (PARTITION BY strTick ORDER BY dtDate) IS NULL
			THEN NULL
			ELSE LAG(decOpen,1) OVER (PARTITION BY strTick ORDER BY dtDate)/LAG(decOpen,2) OVER (PARTITION BY strTick ORDER BY dtDate)
		END as PstPNL,
		CASE
			WHEN decClose/LAG(decClose,1) OVER (PARTITION BY strTick ORDER BY dtDate) > 1
			THEN decClose/LAG(decClose,1) OVER (PARTITION BY strTick ORDER BY dtDate)
			ELSE NULL
		END as PosPNL,
		CASE
			WHEN decClose/LAG(decClose,1) OVER (PARTITION BY strTick ORDER BY dtDate) < 1
			THEN decClose/LAG(decClose,1) OVER (PARTITION BY strTick ORDER BY dtDate)
			ELSE NULL
		END as NegPNL,
		CASE
			WHEN decClose/LAG(decClose,1) OVER (PARTITION BY strTick ORDER BY dtDate) > 1
			THEN intVol
			ELSE NULL
		END as PosintVol,
		CASE
			WHEN decClose/LAG(decClose,1) OVER (PARTITION BY strTick ORDER BY dtDate) < 1
			THEN intVol
			ELSE NULL
		END as NegintVol
FROM snp500_test
where dtdate >= '01/01/2015' --and strtick in ('TSLA','ILMN','MU','AAL','INCY','FANG','FB','LUV','SWKS','DAL','DXCM','AVGO','UAA','NFLX','URI','HBI','HII','ETFC','STZ','SPXL')
) fst
) snd
) trd
) frth
WHERE RowNum <=10 
GROUP BY dtDate
ORDER BY dtdate

/*
FIND y/y performance average
*/
/*
select Strtick,avg(PNL)
FROM(
select strTick,LEAD(decOpen,252) OVER (partition by strtick order by dtdate)/decOpen as PNL
FROM snp500_test
where dtdate between '01/01/2013' and '01/01/2015'
group by strTick,dtdate,decOpen
) fst
group by strTick
ORDER BY avg(PNL) desc
*/