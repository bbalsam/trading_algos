/*===========================
RSI Relative Strength Index

RSI step 1 = 100-(100/(1+(average gain/average loss)))


===========================*/
use stockdb

select	strtick,
		dtdate, /*=100-(100/(1-(AVERAGE(D3:D16)/AVERAGE(E3:E16))))*/
		avg(GAIN) OVER (partition by strtick ORDER BY dtdate asc ROWS 13 PRECEDING ) as GAINPct,
		avg(LOSS)  OVER (partition by strtick ORDER BY dtdate asc ROWS 13 PRECEDING) as LOSSPct,
		CASE WHEN avg(LOSS)  OVER (partition by strtick ORDER BY dtdate asc ROWS 13 PRECEDING) = 0
		THEN 50
		ELSE 100-(100/(1+
		(avg(GAIN) OVER (partition by strtick ORDER BY dtdate asc ROWS 13 PRECEDING )
		/avg(LOSS)  OVER (partition by strtick ORDER BY dtdate asc ROWS 13 PRECEDING))))
		END RSI
FROM(
select	dtDate,
		strTick,
		decAdjClose,
		CASE WHEN 
		(decAdjClose/lag(decadjclose,1) OVER (partition by strtick order by dtdate asc))-1 >= 0
		THEN
		(decAdjClose/lag(decadjclose,1) OVER (partition by strtick order by dtdate asc))-1
		ELSE 0
		END as GAIN,
		CASE WHEN 
		(decAdjClose/lag(decadjclose,1) OVER (partition by strtick order by dtdate asc))-1 < 0
		THEN
		ABS((decAdjClose/lag(decadjclose,1) OVER (partition by strtick order by dtdate asc))-1)
		ELSE 0
		END as LOSS
from snp500_test
where dtdate >= '2019-01-01') GL
GROUP BY dtdate,strtick,GAIN,LOSS