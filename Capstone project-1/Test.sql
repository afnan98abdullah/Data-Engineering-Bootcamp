-- Customer dimention
-- customer id is not null
select count(*)=0 from TPCDS.ANALYTICS.CUSTOMER_DIM
where c_customer_sk is null;

-- Weekly sales inventory
-- warehouse_sk,item_sk,sold_wk_sk is unique
select count(*)=0 from
(select warehouse_sk,item_sk,sold_wk_sk 
from TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY
group by 1,2,3
having count(*)>1);

-- Relationship test
select count(*)=0 from
(select dim.i_item_sk
from TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY fact
left join TPCDS.ANALYTICS.ITEM_DIM dim
on dim.i_item_sk=fact.item_sk
where dim.i_item_sk is null);

-- Accepted value testing
select count(*)=0 from TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY
where warehouse_sk not in (1,2,3,4,5,6);

-- Adhoc testing
select count(*)=0 from
(select c_current_cdemo_sk,cd.cd_demo_sk
from TPCDS.RAW.CUSTOMER c
left join TPCDS.RAW.CUSTOMER_DEMOGRAPHICS cd
on c.c_current_cdemo_sk=cd.cd_demo_sk
where c_current_cdemo_sk is not null and cd.cd_demo_sk is null);


