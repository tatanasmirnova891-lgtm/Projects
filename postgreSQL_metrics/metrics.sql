select * from marketing_metrics mm;
select * from users;
select * from userentry u ;

/*
 * Агрегированные показатели маркетинга по пользователям
 * Суммарные показы (impressions), клики (clicks), лиды (leads), покупки (purchases), расходы (spent) и выручку (revenue) 
 * для каждого пользователя
*/

SELECT 
    user_id,
    SUM(impressions) AS total_impressions, --количество показов
    SUM(clicks) AS total_clicks, --количество кликов
    SUM(leads) AS total_leads, --количество заявок
    SUM(purchases) AS total_purchases, --сумма покупки
    SUM(spent) AS total_spent, --потраченный бюджет
    SUM(revenue) AS total_revenue --общая выручка
FROM marketing_metrics mm join users u on mm.user_id = u.id
WHERE 1=1
AND u.id >= 94 --исключаем тестовых пользователей
and u.is_active = 1 --рассматриваем только активных пользователей
GROUP BY user_id
order by user_id;

/*
 * Конверсии
*/
with sum_metrics as (
	SELECT 
	    to_char(u.date_joined, 'YYYY-MM') as cohort,
	    SUM(impressions) AS total_impressions, --количество показов
	    SUM(clicks) AS total_clicks, --количество кликов
	    SUM(leads) AS total_leads, --количество заявок
	    SUM(valid_leads) as total_valid_leads, --количество квалифицированных заявок
	    SUM(purchases) AS total_purchases, --сумма покупки
	    SUM(spent) AS total_spent, --потраченный бюджет
	    SUM(revenue) AS total_revenue --общая выручка
	FROM marketing_metrics mm join users u on mm.user_id = u.id
	WHERE 1=1
	AND u.id >= 94 --исключаем тестовых пользователей
	and u.is_active = 1 --рассматриваем только активных пользователей
	GROUP BY cohort
	order by cohort
)
SELECT 
	cohort,
--CTR (%) (click through rate) - показывает коэффициент кликабельности, т.е. как показ ковертируется в клик.
	ROUND((total_clicks::numeric / total_impressions) * 100,2) as "CTR (%)",
--CR1 (%) - конверсия из клика в целевое действие
	ROUND((total_leads::numeric / total_clicks) * 100,2) as "CR1 (%)",
--purchases_per_click_percent - конверсия из клика в покупку
	ROUND((total_purchases::numeric / total_clicks) * 100,2) as "purchases_per_click_percent (%)",
--vCR (%) - конверсия из лида в квалифицированного лида
	ROUND((total_valid_leads::numeric / total_leads) * 100,2) as "vCR (%)",
--purchases_per_lead_percent - конверсия из лида в покупку
	ROUND((total_purchases::numeric / total_leads) * 100,2) as "purchases_per_lead_percent (%)",	
--vleads_per_click_percent - конверсия из клика в квалифицированного лида
	ROUND((total_valid_leads::numeric / total_clicks) * 100,2) as "vleads_per_click_percent (%)",	
--CR2 (%) - конверсия из квалифицированного лида в покупку
	ROUND((total_purchases::numeric / total_valid_leads) * 100,2) as "CR2 (%)"
FROM sum_metrics;


/*
 * Время на сайте и поведенческие метрики
 * Среднее время на сайте , среднее количество страниц за сессию, 
 * показатель отказов .
*/
SELECT 
	to_char(u.date_joined, 'YYYY-MM') as cohort,
	to_char(
	    interval '1 second' * ROUND(AVG(time_on_site_sec)),
	    'HH24:MI:SS'
	) as avg_time_on_site, --формат в читабельный вид "часы:минуты:секунды"
	ROUND(AVG(pages_per_session),2) as avg_page_session,
	ROUND(AVG(bounce_rate_percent), 2) as avg_bounce_rate --средневзвешенный процент отказов
FROM marketing_metrics mm join users u on mm.user_id = u.id
WHERE 1=1
AND u.id >= 94 --исключаем тестовых пользователей
and u.is_active = 1 --рассматриваем только активных пользователей
GROUP BY cohort
order by cohort;



/*
 * Активность пользователей
 * Kоличество входов (userentry) за период, среднее количество сессий на пользователя.
*/
SELECT 
	to_char(u.date_joined, 'YYYY-MM') as cohort,
-- Количество входов (сессий) - показывает, насколько часто пользователи возвращаются на сайт или в приложение.
	COUNT(*) AS total_sessions,  -- общее число сессий
	COUNT(DISTINCT user_id) AS unique_users,  -- уникальные пользователи
-- Среднее количество сессий на пользователя - показывает, насколько регулярно каждый пользователь взаимодействует с продуктом.
	ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_sessions_per_user
FROM userentry ue join users u on ue.user_id = u.id
WHERE 1=1
AND u.id >= 94 --исключаем тестовых пользователей
and u.is_active = 1 --рассматриваем только активных пользователей
GROUP BY cohort
order by cohort;



/*
 * Сегментация: разбивка пользователей по активности на основе количества кликов для анализа поведения разных групп.
*/
WITH user_clicks AS (
  SELECT 
    user_id,
    SUM(clicks) AS total_clicks
  FROM marketing_metrics
  GROUP BY user_id
),
clicks_quartiles AS (
  SELECT
    user_id,
    total_clicks,
    ntile(4) OVER (ORDER BY total_clicks) AS click_quartile
  FROM user_clicks
),
activites_users as (
	SELECT
	  user_id,
	  total_clicks,
	  click_quartile,
	  CASE
	    WHEN total_clicks <= 290 THEN 'низкая активность'
	    WHEN total_clicks <=535 THEN 'средняя активность'
	    ELSE 'высокая активность'
	  END AS activity_segment
	FROM clicks_quartiles
	ORDER BY total_clicks)
select 
	to_char(u.date_joined, 'YYYY-MM') as cohort,
	activity_segment,
	case 
		when activity_segment = 'низкая активность' then count(u.id)
		when activity_segment = 'средняя активность' then count(u.id)
		when activity_segment = 'высокая активность' then count(u.id)
	end as cnt_users_in_segments
from users u join activites_users au on u.id = au.user_id
WHERE 1=1
AND u.id >= 94 --исключаем тестовых пользователей
and u.is_active = 1 --рассматриваем только активных пользователей
GROUP BY cohort, activity_segment
order by cohort;


/*
Когорты: анализ пользователей по дате регистрации (date_joined), удержание и поведение в динамике.
*/
WITH cohorts AS (
    SELECT 
        id AS user_id, 
        DATE_TRUNC('month', date_joined) AS cohort_month,
        COUNT(u.id) OVER(partition by DATE_TRUNC('month', date_joined)) AS cohort_size
    FROM users u
	WHERE 1=1
	AND u.id >= 94 --исключаем тестовых пользователей
	and u.is_active = 1 --рассматриваем только активных пользователей
),
activity AS (
    SELECT 
        user_id, 
        DATE_TRUNC('month', entry_at) AS activity_month
    FROM userentry
),
retention AS (
    SELECT 
        c.cohort_month,
        a.activity_month,
        COUNT(DISTINCT a.user_id) AS active_users,
        c.cohort_size
    FROM cohorts c
    LEFT JOIN activity a ON c.user_id = a.user_id 
       AND a.activity_month >= c.cohort_month
    GROUP BY c.cohort_month, a.activity_month, c.cohort_size
)
SELECT 
    r.cohort_month,
    r.activity_month,
    r.active_users,
    r.cohort_size,
    ROUND((r.active_users::decimal / r.cohort_size) * 100, 2) AS retention_percent
FROM retention r
ORDER BY r.cohort_month, r.activity_month;



/*
Сводки для управления данными
 */
--Общее количество активных и неактивных пользователей
SELECT 
	case 
		when is_active = 1 then 'Активные пользователи'
	else 
		'Неактивные пользователи'
	end as activities,
	COUNT(id) AS user_count,
	ROUND(COUNT(id) * 100.0 / (SELECT COUNT(*) FROM users WHERE id >= 94), 2) AS percent
FROM users
WHERE 1=1
AND id >= 94 --исключаем тестовых пользователей
GROUP BY activities

	
	--Количество новых пользователей по месяцам
SELECT 
    DATE_TRUNC('month', date_joined) AS month,
    COUNT(*) AS cnt_new_users
FROM users
WHERE 1=1
AND id >= 94 --исключаем тестовых пользователей
GROUP BY month
ORDER BY month;

--ROI и средний чек

SELECT
    CASE WHEN SUM(purchases) = 0 THEN 0 ELSE ROUND(SUM(revenue) / SUM(purchases), 2) END AS "AOV",
    CASE WHEN SUM(spent) = 0 THEN 0 ELSE ROUND(SUM(revenue) / SUM(spent), 2) END AS "ROI"
FROM marketing_metrics;
