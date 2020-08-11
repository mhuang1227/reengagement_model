WITH cus_table AS (
                      SELECT
                          c.id cid
                          , MIN(o.created_at) first_order_date
                          , MAX(o.created_at) last_order_date
                          // calculate the number of orders users have placed since they signed up
                          , COUNT(DISTINCT o.id) total_orders_lifetime 
                          // calculate how many orders users have placed within past 90 to 30 days
                          , SUM(CASE WHEN o.created_at::date BETWEEN DATEADD(month, -3, GETDATE()) AND DATEADD(month, -1, GETDATE()) THEN 1 ELSE 0 END) recency 
                      FROM
                          OG_VIEWS.CUSTOMERS c
                          ,OG_VIEWS.ORDERS o
                          ,DATA_SCIENCE.SUBSCRIPTION_BASE sb 
                      WHERE
                          c.created_at::date BETWEEN '2019-01-01' AND '2020-03-01'
                          AND c.deleted_at IS NULL
                          AND c.stripe_customer_id LIKE '%cus_%'
                          AND o.status = 'delivered'
                          AND o.delivered_at IS NOT NULL
                          AND c.id=o.customer_id
                          AND c.id=sb.customer_id
                          AND sb.origination IN ('marketplace', 'platform', 'whitelabel')
                          AND sb.deleted_at IS NULL
                          AND sb.trial_abandon_time IS NULL
                          AND sb.churn_payment_time IS NULL
                          // exclude business customers
                          AND c.id NOT IN (
                                       SELECT
                                            DISTINCT c.id
                                        FROM
                                            OG_VIEWS.CUSTOMERS c
                                        WHERE
                                            LOWER(c.name) = 'business customer'
                                            OR LOWER(c.name) = 'actionlink vendor'
                                            OR LOWER(c.customer_type) = 'business'
                                      )
                      GROUP BY c.id
                      ),
t1 AS(
        SELECT
              DISTINCT cid
              , total_orders_lifetime
              , first_order_date
              , last_order_date
              //caluclate monthly order count
              , SUM(CASE WHEN DATEADD(day, -30, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -30, GETDATE()) AND GETDATE() THEN 1 
                       ELSE 0 END) past_30days_order_count 
              , SUM(CASE WHEN DATEADD(day, -60, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -60, GETDATE()) AND DATEADD(day, -30, GETDATE()) THEN 1 
                       ELSE 0 END) past_31_60days_order_count
              , SUM(CASE WHEN DATEADD(day, -90, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -90, GETDATE()) AND DATEADD(day, -60, GETDATE()) THEN 1 
                       ELSE 0 END) past_61_90days_order_count
              , SUM(CASE WHEN DATEADD(day, -120, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -120, GETDATE()) AND DATEADD(day, -90, GETDATE()) THEN 1 
                       ELSE 0 END) past_91_120days_order_count
              , SUM(CASE WHEN DATEADD(day, -150, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -150, GETDATE()) AND DATEADD(day, -120, GETDATE()) THEN 1 
                       ELSE 0 END) past_121_150days_order_count
              , SUM(CASE WHEN DATEADD(day, -180, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -180, GETDATE()) AND DATEADD(day, -150, GETDATE()) THEN 1 
                       ELSE 0 END) past_151_180days_order_count
        FROM
            cus_table ct
            ,OG_VIEWS.ORDERS o
        WHERE    
            o.customer_id=ct.cid
            // only consider delivered orders
            AND o.status = 'delivered'
            AND o.delivered_at IS NOT NULL
            //users have to place at least 5 orders to be considered regular users
            AND ct.total_orders_lifetime >= 5
            //users have to place at least 1 order within past 30 to 90 days to justify they are using Shipt recently
            AND ct.recency >=1
            //the first order date has to be at least 4 month from today. becasue we set past 30 days as the observation window and we need at least other 3 months data to calculate their average monthly order rate
            AND first_order_date < DATEADD(month, -4, GETDATE())
        GROUP BY 1,2,3,4
        ),
t2 AS(
        SELECT
            cid
            , first_order_date
            , last_order_date
            // group users into casual/core/power cohorts based on their average monthly order rate
            ,(ifnull(past_31_60days_order_count,0) + ifnull(past_61_90days_order_count,0) + ifnull(past_91_120days_order_count,0) + ifnull(past_121_150days_order_count,0) + ifnull(past_151_180days_order_count,0))/
          ((CASE WHEN past_31_60days_order_count is not null THEN 1 ELSE 0 END) + (CASE WHEN past_91_120days_order_count is not null THEN 1 ELSE 0 END) + (CASE WHEN past_121_150days_order_count is not null THEN 1 ELSE 0 END) + (CASE WHEN past_151_180days_order_count is not null THEN 1 ELSE 0 END) + (CASE WHEN past_61_90days_order_count is not null THEN 1 ELSE 0 END)) as monthly_order_count_average
            , CASE WHEN monthly_order_count_average >= 1 AND monthly_order_count_average < 2.5 THEN 'casual' 
                   WHEN monthly_order_count_average >= 2.5 AND monthly_order_count_average < 4.5 THEN 'core'
                   ELSE 'power'
              END cohort
            // days since users' last order
            , (IFNULL(ABS(past_30days_order_count-past_31_60days_order_count),0)+IFNULL(ABS(past_31_60days_order_count-past_61_90days_order_count),0)+IFNULL(ABS(past_61_90days_order_count-past_91_120days_order_count),0)+IFNULL(ABS(past_91_120days_order_count-past_121_150days_order_count),0)+IFNULL(ABS(past_121_150days_order_count-past_151_180days_order_count),0))/monthly_order_count_average AS magnitude_index
            , DATEDIFF(day, last_order_date, GETDATE()) days_since_last_transaction
            // avg days between orders
            , 30/monthly_order_count_average avg_days_between_orders
            , monthly_order_count_average - past_30days_order_count past_30days_drop_from_avg
            , past_30days_order_count
            , past_31_60days_order_count
            , past_61_90days_order_count
            , past_91_120days_order_count
            , past_121_150days_order_count
            , past_151_180days_order_count
         FROM
            t1
        )
SELECT 
    // today's date, at what date user is in the corresponding sub group
    GETDATE()::date date
    // users' id
    , t2.cid AS customer_id
    // users' metro
    , m.name AS metro
    // divide users into sub_groups, for the post test analysis
    , CASE WHEN monthly_order_count_average < 2.5 AND days_since_last_transaction >= avg_days_between_orders*1.5 THEN 'Casual Stopper'
                   WHEN monthly_order_count_average >= 2.5 AND monthly_order_count_average < 4.5 AND days_since_last_transaction >= avg_days_between_orders*2.25 THEN 'Core Stopper'
                   WHEN monthly_order_count_average >= 4.5 AND days_since_last_transaction >= avg_days_between_orders*2.5 THEN 'Power Stopper'
                   WHEN monthly_order_count_average < 2.5 AND days_since_last_transaction < avg_days_between_orders*1.5 THEN 'Casual Decreaser'
                   WHEN monthly_order_count_average >= 2.5 AND monthly_order_count_average < 4.5 AND magnitude_index <= 3 AND days_since_last_transaction < avg_days_between_orders*2.25 THEN 'Regular Core Decreaser'
                   WHEN monthly_order_count_average >= 2.5 AND monthly_order_count_average < 4.5 AND magnitude_index > 3 AND days_since_last_transaction < avg_days_between_orders*2.25 THEN 'Erratic Core Decreaser'
                   WHEN monthly_order_count_average >= 4.5 AND magnitude_index <= 2 AND days_since_last_transaction < avg_days_between_orders*2.5 THEN 'Regular Power Decreaser'
                   WHEN monthly_order_count_average >= 4.5 AND magnitude_index > 2 AND days_since_last_transaction < avg_days_between_orders*2.5 THEN 'Erratic Power Decreaser'
                   ELSE 'Other' 
       END sub_group
FROM 
    t2
    , OG_VIEWS.customers c
    , OG_VIEWS.metros m
WHERE
    // monthly order count average have to be at least 1
    monthly_order_count_average>=1
    // we define a user's order rate is decreased when his order count in the past 30 days is less than 75% of his/her average monthly order rate
    AND monthly_order_count_average*0.75 > past_30days_order_count
    AND t2.cid=c.id
    AND c.metro_id=m.id;
