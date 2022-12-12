/*
Create table Ichimoku2 (
    ID int IDENTITY(1,1) PRIMARY KEY,
	strTick varchar(12),
    dtBuyDate date,
    decBuyCost DECIMAL(10, 6),
	intShares int,
	dtSellDate date,
    decSellCost DECIMAL(10, 6),
	bitOpenPosition bit
);

drop table Ichimoku2
*/
/*
select * from Ichimoku 
where strtick = 'tsla'
order by dtbuydate

*/

Create table Ichimoku3 (
    ID int IDENTITY(1,1) PRIMARY KEY,
	strTick varchar(12),
    dtBuyDate date,
    decBuyCost DECIMAL(10, 6),
	intShares int,
	dtSellDate date,
    decSellCost DECIMAL(10, 6),
	bitOpenPosition bit,
	BUY1 int,
	BUY2 int,
	BUY3 int,
	BUY4 int,
	BUY5 int,
	BUY6 int,
	SELL1 int,
	SELL2 int,
	SELL3 int,
	SELL4 int,
	SELL5 int,
	SELL6 int,
);

drop table Ichimoku3


