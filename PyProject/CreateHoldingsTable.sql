/*
Create table Holdings (
    ID int IDENTITY(1,1) PRIMARY KEY,
	strTick varchar(12),
    dtBuyDate date,
    decCostBasis DECIMAL(10, 6),
	intShares int,
	bitOpenPosition bit
	dtSaleDate date,
	decSalePrice DECIMAL(10, 6)
);

drop table Holdings
*/
/*
select * from Holdings

begin tran
ALTER TABLE Holdings
ADD dtSaleDate date
ALTER TABLE Holdings
ADD decSalePrice DECIMAL(10, 6)

select * from Holdings
commit
*/

/*ALTER TABLE Holdings RENAME COLUMN binOpenPosition TO bitOpenPosition*/