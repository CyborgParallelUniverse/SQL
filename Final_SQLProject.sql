create database customers;
update customer_info set Gender = NULL where Gender = "";
update customer_info set Age = NULL where Age = "";
alter table customer_info modify Age INT Null;

select * from customer_info;
#1 
#Cписок клиентов с непрерывной историей за год, то есть каждый месяц на регулярной основе без пропусков за указанный годовой период, средний чек за период с 01.06.2015 по 01.06.2016
create table transactions(
data_new DATE,
Id_check INT,
ID_client INT,
Count_products DECIMAL (10,3),
Sum_payment DECIMAL(10,2)
);

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TRANSACTIONS.csv"
INTO TABLE transactions
Fields Terminated by ','
lines terminated by '\n'
ignore 1 rows;

select * from transactions;
#1
#список клиентов с непрерывной историей за год, то есть каждый месяц на регулярной основе без пропусков за указанный годовой период
WITH months AS (
    SELECT '2015-06-01' AS month_start UNION ALL
    SELECT '2015-07-01' UNION ALL
    SELECT '2015-08-01' UNION ALL
    SELECT '2015-09-01' UNION ALL
    SELECT '2015-10-01' UNION ALL
    SELECT '2015-11-01' UNION ALL
    SELECT '2015-12-01' UNION ALL
    SELECT '2016-01-01' UNION ALL
    SELECT '2016-02-01' UNION ALL
    SELECT '2016-03-01' UNION ALL
    SELECT '2016-04-01' UNION ALL
    SELECT '2016-05-01'
),
client_months AS (
    SELECT 
        t.ID_client, 
        DATE_FORMAT(t.data_new, '%Y-%m-01') AS month_start
    FROM transactions t
    WHERE t.data_new >= '2015-06-01' AND t.data_new < '2016-06-01'
    GROUP BY t.ID_client, DATE_FORMAT(t.data_new, '%Y-%m-01')
),
monthly_counts AS (
    SELECT 
        cm.ID_client,
        COUNT(DISTINCT cm.month_start) AS active_months
    FROM client_months cm
    GROUP BY cm.ID_client
)
SELECT 
    ci.Id_client
FROM monthly_counts mc
JOIN customer_info ci ON ci.Id_client = mc.ID_client
WHERE mc.active_months = 12;

#1 средний чек за период с 01.06.2015 по 01.06.2016
SELECT 
    SUM(t.Sum_payment) / COUNT(DISTINCT t.Id_check) AS avg_check
FROM transactions t
WHERE t.data_new >= '2015-06-01' AND t.data_new < '2016-06-01';
# 1 средняя сумма покупок за месяц
SELECT 
    DATE_FORMAT(t.data_new, '%Y-%m') AS month,
    AVG(t.Sum_payment) AS avg_check
FROM transactions t
GROUP BY DATE_FORMAT(t.data_new, '%Y-%m')
ORDER BY month;
# 1 количество всех операций (транзакций) по каждому клиенту за весь период
select 
	t.ID_client,
	count(*) as total_t
from transactions t
Group by t.ID_client
order by total_t desc;

#2a
SELECT 
    DATE_FORMAT(t.data_new, '%Y-%m') AS month,
    sum(t.sum_payment)/count(distinct t.id_check) as avg_paycheck
FROM transactions t
GROUP BY DATE_FORMAT(t.data_new, '%Y-%m')
ORDER BY avg_paycheck;

#2b 
SELECT 
    DATE_FORMAT(t.data_new, '%Y-%m') AS month,
    COUNT(*) AS total_operations
FROM transactions t
GROUP BY DATE_FORMAT(t.data_new, '%Y-%m')
ORDER BY month;

#2c
SELECT 
    DATE_FORMAT(t.data_new, '%Y-%m') AS month,
    COUNT(distinct t.ID_client) AS total_ids
FROM transactions t
GROUP BY DATE_FORMAT(t.data_new, '%Y-%m')
ORDER BY month;

#2e
WITH base AS (
    SELECT 
        DATE_FORMAT(t.data_new, '%Y-%m') AS month,
        t.ID_client,
        t.Sum_payment,
        c.Gender
    FROM transactions t
    JOIN customer_info c ON t.ID_client = c.Id_client
),
month_totals AS (
    SELECT 
        month,
        COUNT(DISTINCT ID_client) AS total_clients,
        SUM(Sum_payment) AS total_payments
    FROM base
    GROUP BY month
)

SELECT 
    b.month,
    b.Gender,
    COUNT(DISTINCT b.ID_client) AS num_clients,
    ROUND(100 * COUNT(DISTINCT b.ID_client) / mt.total_clients, 2) AS pct_clients,
    ROUND(SUM(b.Sum_payment), 2) AS total_payment,
    ROUND(100 * SUM(b.Sum_payment) / mt.total_payments, 2) AS pct_payment
FROM base b
JOIN month_totals mt ON b.month = mt.month
GROUP BY b.month, b.Gender, mt.total_clients, mt.total_payments
ORDER BY b.month, b.Gender;

#3
WITH base AS (
    SELECT 
        t.ID_client,
        c.Age,
        CASE
            WHEN c.Age IS NULL THEN 'NA'
            WHEN c.Age < 10 THEN '00–09'
            WHEN c.Age < 20 THEN '10–19'
            WHEN c.Age < 30 THEN '20–29'
            WHEN c.Age < 40 THEN '30–39'
            WHEN c.Age < 50 THEN '40–49'
            WHEN c.Age < 60 THEN '50–59'
            WHEN c.Age < 70 THEN '60–69'
            ELSE '70+'
        END AS age_group,
        t.sum_payment,
        t.data_new
    FROM transactions t
    JOIN customer_info c ON t.ID_client = c.Id_client
)
SELECT 
    age_group,
    COUNT(*) AS total_operations,
    SUM(sum_payment) AS total_payment
FROM base
GROUP BY age_group
ORDER BY age_group;  # весь период 

# Поквартально
WITH base AS (
    SELECT 
        t.ID_client,
        c.Age,
        CASE
            WHEN c.Age IS NULL THEN 'NA'
            WHEN c.Age < 10 THEN '00–09'
            WHEN c.Age < 20 THEN '10–19'
            WHEN c.Age < 30 THEN '20–29'
            WHEN c.Age < 40 THEN '30–39'
            WHEN c.Age < 50 THEN '40–49'
            WHEN c.Age < 60 THEN '50–59'
            WHEN c.Age < 70 THEN '60–69'
            ELSE '70+'
        END AS age_group,
        t.sum_payment,
        t.data_new,
        CONCAT(YEAR(t.data_new), '-Q', QUARTER(t.data_new)) AS quarter
    FROM transactions t
    JOIN customer_info c ON t.ID_client = c.Id_client
),
quarter_totals AS (
    SELECT 
        quarter,
        COUNT(*) AS total_ops,
        SUM(sum_payment) AS total_sum
    FROM base
    GROUP BY quarter
)

SELECT 
    b.quarter,
    b.age_group,
    COUNT(*) AS operations,
    ROUND(AVG(b.sum_payment), 2) AS avg_payment,
    ROUND(100 * COUNT(*) / qt.total_ops, 2) AS pct_operations,
    ROUND(100 * SUM(b.sum_payment) / qt.total_sum, 2) AS pct_payment
FROM base b
JOIN quarter_totals qt ON b.quarter = qt.quarter
GROUP BY b.quarter, b.age_group, qt.total_ops, qt.total_sum
ORDER BY b.quarter, b.age_group;

