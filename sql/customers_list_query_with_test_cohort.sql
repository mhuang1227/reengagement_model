WITH cus_order_count AS (
                      SELECT
                          c.id cid,
                          MIN(o.created_at) first_order_date,
                          MAX(o.created_at) last_order_date,
                          COUNT(DISTINCT o.id) total_orders_lifetime,
                          SUM(CASE WHEN o.created_at::date BETWEEN DATEADD(month, -3, GETDATE()) AND DATEADD(month, -1, GETDATE()) THEN 1 ELSE 0 END) recency
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
tmp AS(
        SELECT
              DISTINCT cid,
              total_orders_lifetime,
              first_order_date,
              last_order_date,
              SUM(CASE WHEN DATEADD(day, -30, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -30, GETDATE()) AND GETDATE() THEN 1 
                       ELSE 0 END) past_30days_order_count, 
              SUM(CASE WHEN DATEADD(day, -60, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -60, GETDATE()) AND DATEADD(day, -30, GETDATE()) THEN 1 
                       ELSE 0 END) past_31_60days_order_count,
              SUM(CASE WHEN DATEADD(day, -90, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -90, GETDATE()) AND DATEADD(day, -60, GETDATE()) THEN 1 
                       ELSE 0 END) past_61_90days_order_count,
              SUM(CASE WHEN DATEADD(day, -120, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -120, GETDATE()) AND DATEADD(day, -90, GETDATE()) THEN 1 
                       ELSE 0 END) past_91_120days_order_count,
              SUM(CASE WHEN DATEADD(day, -150, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -150, GETDATE()) AND DATEADD(day, -120, GETDATE()) THEN 1 
                       ELSE 0 END) past_121_150days_order_count,
              SUM(CASE WHEN DATEADD(day, -180, GETDATE()) < first_order_date THEN NULL
                       WHEN o.created_at::date BETWEEN DATEADD(day, -180, GETDATE()) AND DATEADD(day, -150, GETDATE()) THEN 1 
                       ELSE 0 END) past_151_180days_order_count
        FROM
            cus_order_count coc
            ,OG_VIEWS.ORDERS o
        WHERE    
            o.customer_id=coc.cid
            AND o.status = 'delivered'
            AND o.delivered_at IS NOT NULL
            AND coc.total_orders_lifetime >= 5
            AND coc.recency >=1
            AND first_order_date < DATEADD(month, -4, GETDATE())
        GROUP BY 1,2,3,4
        ),
mean AS(
        SELECT
            cid
            , first_order_date
            , last_order_date
            ,(ifnull(past_31_60days_order_count,0) + ifnull(past_61_90days_order_count,0) + ifnull(past_91_120days_order_count,0) + ifnull(past_121_150days_order_count,0) + ifnull(past_151_180days_order_count,0))/
          ((CASE WHEN past_31_60days_order_count is not null THEN 1 ELSE 0 END) + (CASE WHEN past_91_120days_order_count is not null THEN 1 ELSE 0 END) + (CASE WHEN past_121_150days_order_count is not null THEN 1 ELSE 0 END) + (CASE WHEN past_151_180days_order_count is not null THEN 1 ELSE 0 END) + (CASE WHEN past_61_90days_order_count is not null THEN 1 ELSE 0 END)) as monthly_order_count_average
            , CASE WHEN monthly_order_count_average >= 1 AND monthly_order_count_average < 2.5 THEN 'casual' 
                   WHEN monthly_order_count_average >= 2.5 AND monthly_order_count_average < 4.5 THEN 'core'
                   ELSE 'power'
              END cohort
            , (IFNULL(ABS(past_30days_order_count-past_31_60days_order_count),0)+IFNULL(ABS(past_31_60days_order_count-past_61_90days_order_count),0)+IFNULL(ABS(past_61_90days_order_count-past_91_120days_order_count),0)+IFNULL(ABS(past_91_120days_order_count-past_121_150days_order_count),0)+IFNULL(ABS(past_121_150days_order_count-past_151_180days_order_count),0))/monthly_order_count_average AS magnitude_index
            , DATEDIFF(day, last_order_date, GETDATE()) days_since_last_transaction
            , 30/monthly_order_count_average avg_days_between_orders
            , monthly_order_count_average - past_30days_order_count past_30days_drop_from_avg
            , past_30days_order_count
            , past_31_60days_order_count
            , past_61_90days_order_count
            , past_91_120days_order_count
            , past_121_150days_order_count
            , past_151_180days_order_count
         FROM
            tmp
        )
SELECT 
    GETDATE()::date date
    , m.cid AS customer_id
    , CASE WHEN monthly_order_count_average < 2.5 AND days_since_last_transaction > avg_days_between_orders*1.5 THEN 'Casual Stopper'
           WHEN monthly_order_count_average >= 2.5 AND monthly_order_count_average < 4.5 AND days_since_last_transaction > avg_days_between_orders*2.25 THEN 'Core Stopper'
           WHEN monthly_order_count_average >= 4.5 AND days_since_last_transaction > avg_days_between_orders*2.5 THEN 'Power Stopper'
           WHEN monthly_order_count_average >= 2.5 AND monthly_order_count_average < 4.5 AND (past_30days_order_count = 1 OR past_30days_order_count =2) AND magnitude_index <= 3 AND days_since_last_transaction < avg_days_between_orders*2.25 THEN 'Regular Core Decreaser'
           WHEN monthly_order_count_average >= 2.5 AND monthly_order_count_average < 4.5 AND (past_30days_order_count = 1 OR past_30days_order_count =2) AND magnitude_index > 3 AND days_since_last_transaction < avg_days_between_orders*2.25 THEN 'Erratic Core Decreaser'
           WHEN monthly_order_count_average >= 4.5 AND (past_30days_order_count = 1 OR past_30days_order_count =2 OR past_30days_order_count = 3 OR past_30days_order_count = 4) AND magnitude_index <= 2 AND days_since_last_transaction < avg_days_between_orders*2.5 THEN 'Regular Power Decreaser'
           WHEN monthly_order_count_average >= 4.5 AND (past_30days_order_count = 1 OR past_30days_order_count =2 OR past_30days_order_count = 3 OR past_30days_order_count = 4) AND magnitude_index > 2 AND days_since_last_transaction < avg_days_between_orders*2.5 THEN 'Erratic Power Decreaser'
           WHEN monthly_order_count_average < 2.5 AND days_since_last_transaction < avg_days_between_orders*1.5 THEN 'Casual Decreaser'
           ELSE 'Other' 
       END sub_group
     , CASE WHEN c.bucket_number BETWEEN 1 AND 50 THEN '20200814_20200914_ReEngagementIncentiveTest1_CohortB_Control'
            WHEN c.bucket_number BETWEEN 51 AND 100 THEN '20200814_20200914_ReEngagementIncentiveTest1_CohortA_Incentive10off3orders'
       END test_cohort
FROM 
    mean m
    ,OG_VIEWS.CUSTOMERS c
WHERE
    m.cid = c.id
    AND monthly_order_count_average>=1
    AND monthly_order_count_average*0.75 > past_30days_order_count;
