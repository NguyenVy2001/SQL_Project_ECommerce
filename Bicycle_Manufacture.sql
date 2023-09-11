--1.Calc Quantity of items, Sales value & Order quantity by each Subcategory in L12M
SELECT distinct FORMAT_DATE('%b %Y', a.ModifiedDate) as period,
       c.Name,
       sum(a.OrderQty) as items,
       sum(a.LineTotal) as sale_value,
       count(distinct a.SalesOrderID) as sub_order
FROM `adventureworks2019.Sales.SalesOrderDetail` a 
JOIN `adventureworks2019.Production.Product` b using (ProductID)
JOIN `adventureworks2019.Production.ProductSubcategory` c ON cast(b.ProductSubcategoryID as int) = c.ProductSubcategoryID
where date(a.ModifiedDate) between (date_sub(date(a.ModifiedDate), INTERVAL 12 month)) and '2014-06-30'
group by period, c.Name
order by period desc;

--2.Calc % YoY growth rate by SubCategory & release top 3 cat with highest grow rate. Can use metric: quantity_item. Round results to 2 decimal
with current_year as (
          SELECT distinct FORMAT_DATE('%Y', a.ModifiedDate) as year,
                c.Name as product_name,
                sum(a.OrderQty) as current_qty_items
          FROM `adventureworks2019.Sales.SalesOrderDetail` a 
          JOIN `adventureworks2019.Production.Product` b using (ProductID)
          JOIN `adventureworks2019.Production.ProductSubcategory` c ON cast(b.ProductSubcategoryID as int) = c.ProductSubcategoryID
          group by year, product_name
          order by product_name, year desc)

, previous_year AS (
          select *,
                lead(current_year.current_qty_items) over(partition by current_year.product_name order by current_year.year desc) as pre_year_items
          from current_year
          order by 2, 1 desc)

select product_name,
       current_qty_items,
       pre_year_items,
       (current_qty_items - pre_year_items)/ pre_year_items AS year_diff
from previous_year
order by year_diff desc
limit 3;

--3.Ranking Top 3 TeritoryID with biggest Order quantity of every year. If there's TerritoryID with same quantity in a year, do not skip the rank number
with pre_ranking as (
       SELECT distinct FORMAT_DATE('%Y', a.ModifiedDate) as year, 
              b.TerritoryID,
              sum(a.OrderQty) as order_cnt
       FROM `adventureworks2019.Sales.SalesOrderDetail` a 
       JOIN `adventureworks2019.Sales.SalesOrderHeader` b 
       ON a.SalesOrderID = b.SalesOrderID
       group by 1, 2
       order by year DESC, order_cnt desc)

   , pre_data as (
       select *,
              dense_rank() over(partition by pre_ranking.year order by pre_ranking.order_cnt desc) as ranking
       from pre_ranking
       order by pre_ranking.year desc, pre_ranking.order_cnt desc)

select *
from pre_data
where pre_data.ranking <=3
order by year desc, pre_data.order_cnt desc;

--4.Calc Total Discount Cost belongs to Seasonal Discount for each SubCategory
with pre_data as (
SELECT distinct FORMAT_DATE('%Y', a.ModifiedDate) as year,
       c.Name as product,
       sum(a.OrderQty) as qty_items,
       a.UnitPrice as unit_price,
       d.DiscountPct as discount
FROM `adventureworks2019.Sales.SalesOrderDetail` a 
LEFT JOIN `adventureworks2019.Production.Product` b using (ProductID)
LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c ON cast(b.ProductSubcategoryID as int) = c.ProductSubcategoryID
LEFT JOIN `adventureworks2019.Sales.SpecialOffer` d ON a.SpecialOfferID = d.SpecialOfferID
where d.Type = 'Seasonal Discount'
group by year, product, unit_price, discount
order by product)

select pre_data.year,
       pre_data.product,
       pre_data.discount * pre_data.unit_price * pre_data.qty_items as total_discount_cost
from pre_data;

--5.Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
WITH all_customers_by_month AS (
      SELECT  extract(month from ModifiedDate) as all_month,
            CustomerID,
            COUNT(DISTINCT SalesOrderID) as num_order_id
      FROM `adventureworks2019.Sales.SalesOrderHeader` 
      WHERE status = 5 and 
            extract(year from ModifiedDate) = 2014
      group by all_month, CustomerID),

     pre_customer_first_month_shipped AS (
      SELECT *,
             row_number() over(partition by CustomerID order by all_customers_by_month.all_month) as row_num
      FROM all_customers_by_month ),

      first_ord as (
       select all_month as month_join, CustomerID
       from pre_customer_first_month_shipped
       where row_num = 1
      ) 

      select distinct b.month_join,
             CONCAT('M-',a.all_month - b.month_join) as month_diff,
             count(distinct a.CustomerID) customer_count
      from all_customers_by_month a
      join first_ord b using(CustomerID)
      group by 1,2
      order by 1,2;

--6.Trend of Stock level & MoM diff % by all product in 2011. If %gr rate is null then 0. Round to 1 decimal
with current_stock_qty as(
      SELECT a.Name as product_name,
            extract(month from b.ModifiedDate) as month,
            extract(year from b.ModifiedDate) as year,
            sum(b.StockedQty) as current_stock
      FROM `adventureworks2019.Production.Product` a 
      JOIN `adventureworks2019.Production.WorkOrder` b ON a.ProductID = b.ProductID
      where extract(year from b.ModifiedDate) = 2011
      group by product_name, month, year
      order by product_name, month DESC),

     pre_stock as (
      select *,
             lead(current_stock_qty.current_stock) over(partition by product_name order by month desc) as previous_stock
      from current_stock_qty),

     diff_percent as (
      select *,
            round((pre_stock.current_stock - pre_stock.previous_stock)/ pre_stock.previous_stock * 100, 1) as diff_pct
      from pre_stock
      order by product_name, month desc)

select diff_percent.product_name,
       diff_percent.month,
       year,
       current_stock,
       previous_stock,
       case when diff_pct is null then 0 else diff_pct end
from diff_percent;

--7."Calc MoM Ratio of Stock / Sales in 2011 by product name. Order results by month desc, ratio desc. Round Ratio to 1 decimal
--Order results by month desc, ratio desc. Round Ratio to 1 decimal"
WITH sale_info as (
      SELECT extract(month from a.ModifiedDate) as month,
                  extract(year from a.ModifiedDate) as year,
                  a.ProductID as product_id,
                  b.Name as product_name,
                  sum(a.OrderQty) as sale_count
      FROM `adventureworks2019.Sales.SalesOrderDetail` a 
      LEFT JOIN `adventureworks2019.Production.Product` b on a.ProductID = b.ProductID
      where extract(year from a.ModifiedDate) = 2011
      group by 1,2,3,4
      having count(distinct a.SalesOrderID) > 0
      order by month desc),

     stock_info as (
      SELECT ProductID as product_id, 
             COALESCE(COUNT(DISTINCT StockedQty), 0) as stock_count
      FROM `adventureworks2019.Production.WorkOrder`
      group by product_id)
 
SELECT a.product_id,
       a.month,
       a.year,
       a.product_name,
       a.sale_count,
       COALESCE(b.stock_count, 0) as stock_count,
       CASE WHEN a.sale_count = 0 THEN 0 
       ELSE ROUND(COALESCE(b.stock_count, 0) / a.sale_count, 1)
       END as ratio
FROM sale_info a
LEFT JOIN stock_info b USING (product_id)
ORDER BY month DESC, product_name;

--8.No of order and value at Pending status in 2014
SELECT  extract(year from ModifiedDate) as year,
        Status,
        count(distinct PurchaseOrderID) as order_Cnt,
        sum(TotalDue) as value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader` 
where extract(year from ModifiedDate) = 2014 and Status = 1
group by year, Status;

       
