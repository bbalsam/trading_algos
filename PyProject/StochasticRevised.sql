
/*======================================================
STOCHASTIC FAST INDICATOR %K

How the Stochastic Momentum Oscillator Works:
Developed in the late 1950s, the stochastic momentum oscillator 
is used to compare where a security's price closed relative to 
its price range over a given period of time—usually 14 days. It 
is calculated using the following formula:
	  
%K= (100∗(CP−L14)) / (H14−L14)
​	 
where:
CP	= Most recent closing price
L14	= Low of the 14 previous trading sessions
H14	= Highest price traded during the same 14-day period
​	
A %K result of 80 is interpreted to mean that the price of the 
security closed above 80% of all prior closing prices that have 
occurred over the past 14 days. The main assumption is that a 
security's price will trade at the top of the range in a major 
uptrend. A three-period moving average of the %K called %D is 
usually included to act as a signal line. Transaction signals 
are usually made when the %K crosses through the %D.

Generally, a period of 14 days is used in the above calculation, 
but this period is often modified by traders to make this 
indicator more or less sensitive to movements in the price of 
the underlying asset.

The result obtained from applying the formula above is known as 
the fast stochastic. Some traders find that this indicator is too 
responsive to price changes, which ultimately leads to being taken 
out of positions prematurely. To solve this problem, the slow 
stochastic was invented by applying a three-period moving average 
to the %K of the fast calculation. Taking a three-period moving 
average of the fast stochastic's %K has proved to be an effective 
way to increase the quality of transaction signals; it also reduces 
the number of false crossovers. After the first moving average is 
applied to the fast stochastic's %K, an additional three-period 
moving average is then applied—making what is known as the slow 
stochastic's %D. Close inspection will reveal that the %K of the 
slow stochastic is the same as the %D (signal line) on the fast 
stochastic.

The Bottom Line

An easy way to remember the difference between the two technical 
indicators is to think of the fast stochastic as a sports car and 
the slow stochastic as a limousine. Like a sports car, the fast 
stochastic is agile and changes direction very quickly in response 
to sudden changes. The slow stochastic takes a little more time to 
change direction but promises a very smooth ride.

Mathematically, the two oscillators are nearly the same except that 
the slow stochastic's %K is created by taking a three-period average 
of the fast stochastic's %K. Taking a three-period moving average of 
each %K will result in the line that is used for a signal.

======================================================*/
use stockdb

/*======================================================
FAST STOCHASTIC FINAL
======================================================*/

Select *,AVG(k.DFAST) OVER (partition by strtick order by dtdate asc ROWS 2 PRECEDING) DSLOW
FROM
(select *,
	CASE
		WHEN (MAX(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING) = MIN(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING))
		THEN 50
		ELSE (100 * (decadjClose - MIN(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING)))
				/ (MAX(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING) - MIN(decadjclose) OVER (partition by strtick order by dtdate asc ROWS 13 PRECEDING))
	END as DFAST--%K= (100∗(CP−L14)) / (H14−L14)
from snp500_test
where dtdate > '10/01/2020'
) k
where strtick = 'tsla'
order by dtdate desc