/*1. Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)*/
SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
       sum(totals.visits) as visit,  
       sum(totals.pageviews) as pageview,
       sum(totals.transactions) as transaction
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0101' and '0331'
group by month
order by month;

/*2.Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)*/
SELECT trafficSource.source as sources,
       sum(totals.visits) as visit,
       sum(totals.bounces) as total_num_of_bounce,
       sum(totals.bounces)/ sum(totals.visits) * 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
group by sources
order by visit DESC;

/*3.Revenue by traffic source by week, by month in June 2017*/
SELECT 'Month' as time_type,
      FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS time,
      trafficSource.source as sources,
      round(sum(product.productRevenue) /1000000, 4) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE product.productRevenue is not null
group by time, sources
UNION ALL
SELECT 'Week' as time_type,
      FORMAT_DATE('%Y%W', PARSE_DATE('%Y%m%d', date)) AS time,
      trafficSource.source as sources,
      round(sum(product.productRevenue) /1000000, 4) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE product.productRevenue is not null
group by time, sources
order by revenue DESC;

/*4.Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017*/
With purchases AS (
            SELECT DISTINCT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
                  sum(totals.pageviews)/ COUNT(DISTINCT fullVisitorId) as avg_pageviews_purchase,
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
            UNNEST (hits) hits,
            UNNEST (hits.product) product
            where _table_suffix between '0601' and '0731'and
                    (totals.transactions >=1 and product.productRevenue is not null)
            group by month)

, non_purchase AS (
            SELECT DISTINCT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
                  sum(totals.pageviews)/ COUNT(DISTINCT fullVisitorId) as avg_pageviews_non_purchase,
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
            UNNEST (hits) hits,
            UNNEST (hits.product) product
            where _table_suffix between '0601' and '0731'and
                  (totals.transactions IS NULL and  product.productRevenue is null )
            group by month) 

SELECT *
FROM purchases
FULL JOIN non_purchase using(month)
order by month; 

/*5.Average number of transactions per user that made a purchase in July 2017*/
SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS Month,
       ROUND(SUM(totals.transactions)/ COUNT(DISTINCT fullVisitorId), 9) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
where totals.transactions >=1 and product.productRevenue is not null
group by Month;

/*6.Average amount of money spent per session. Only include purchaser data in July 2017*/
SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS Month,
       (sum(product.productRevenue)/ 1000000)/ sum(totals.visits) AS avg_spend_per_session
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
where totals.transactions IS NOT NULL and product.productRevenue is not null
group by Month;

/*7.Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered*/
WITH youtube_product AS ( 
          SELECT fullVisitorId, 
                product.v2ProductName AS purchased_product,
                SUM(product.productQuantity) AS quantity
          FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
          UNNEST(hits) AS hits,
          UNNEST(hits.product) AS product
          WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
                AND product.productRevenue IS NOT NULL
          group by fullVisitorId, purchased_product
          order by quantity DESC)

SELECT product.v2ProductName AS other_purchased_product,
       SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) AS hits,
UNNEST(hits.product) AS product
WHERE fullVisitorId in (SELECT fullVisitorId FROM youtube_product)
      AND product.v2ProductName <> "YouTube Men's Vintage Henley"
      AND product.productRevenue IS NOT NULL
group by other_purchased_product
order by quantity DESC;

/*8: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.*/
WITH cohort_map AS (
      SELECT DISTINCT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
            SUM(CASE WHEN hits.eCommerceAction.action_type = '2' THEN 1 ELSE 0 END) AS number_of_product_view,
            SUM(CASE WHEN hits.eCommerceAction.action_type = '3' THEN 1 ELSE 0 END) AS number_of_add_to_cart,
            SUM(CASE WHEN hits.eCommerceAction.action_type = '6' AND product.productRevenue is not null THEN 1 ELSE 0 END) AS number_of_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE _table_suffix between '0101' and '0331'
      group by month
      order by month)

SELECT *,
       round((number_of_add_to_cart/ number_of_product_view) * 100, 2) AS add_to_cart_rate,
       round((number_of_purchase/ number_of_product_view) * 100, 2) AS purchase_rate
FROM cohort_map
order by cohort_map.month;


