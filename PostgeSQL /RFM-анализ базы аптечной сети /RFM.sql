with agg_info as (
	select distinct 
		b.card, 
		count(b.doc_id) over(partition by b.card) as cnt_purchuases, --кол-во покупок
		extract(day from max(b.datetime) over() - max(b.datetime) over(partition by b.card)) + 1 as days_diff, --кол-во дней с момента последней покупки клиента
		sum(b.summ_with_disc) over(partition by b.card) as sum, --сумма покупки без учета бонусов (взяла это поле для выявления фактических денег поступивших за счет продажи), 
		round(sum(b.summ_with_disc) over(partition by b.card) * 1.0 / count(b.doc_id) over(partition by b.card), 2) as avg_sum
	from bonuscheques b
	where substring(b.card from '^[\d]+$') notnull --отбираем только валидные номера карт
	order by card 
),
abc_clients as (
	select 
		a.*, 
		(case 
			when sum(a.sum) over(order by a.sum desc) / sum(a.sum) over() <= 0.8 then 'A'  
			when sum(a.sum) over(order by a.sum desc) / sum(a.sum) over() <= 0.95 then 'B'
			else 'C'
		end
	) as abc
	from agg_info a
),
check_percentile as (
	select 
		abc,
		percentile, 
		percentile_disc(percentile) within group (order by sum) perc_value_sum, 
		percentile_disc(percentile) within group (order by cnt_purchuases) perc_value_cnt, 
		percentile_disc(percentile) within group (order by cnt_purchuases) perc_value_days
	from abc_clients, generate_series(0.0, 1.0, 0.105) AS percentile
	group by abc, percentile 
), 
thresholds as (
	select 
		abc, 
		percentile_disc(0.38) within group (order by days_diff) as r1, 
		percentile_disc(0.75) within group (order by days_diff) as r2, 
		percentile_disc(1) within group (order by days_diff) as r3,
		percentile_disc(1) within group (order by cnt_purchuases) as f1,
		percentile_disc(0.68) within group (order by cnt_purchuases) as f2,
		percentile_disc(0.45) within group (order by cnt_purchuases) as f3,
		percentile_disc(1) within group (order by sum) as m1,
		percentile_disc(0.66) within group (order by sum) as m2, 
		percentile_disc(0.33) within group (order by sum) as m3
	from abc_clients
	group by abc 
), 
rfm_result as (
	select 
		a.*, 
		t.f3, 
		t.f2, 
		(
		case 
			when a.days_diff <= t.r1 then 1
			when a.days_diff <= t.r2 then 2
			else 3
		end
		) as recency,
		(
		case
			when a.cnt_purchuases <= t.f3 then 3 
			when a.cnt_purchuases <= t.f2 then 2
			else 1
		end
		) as frequency, 
		(
		case
			when a.sum <= t.m3 then 3 
			when a.sum <= t.m2 then 2
			else 1
		end
		) as monetary, 
		(case when a.days_diff <= t.r1 then 1 when a.days_diff <= t.r2 then 2 else 3 end)*100 +
		(case when a.cnt_purchuases <= t.f3 then 3 when a.cnt_purchuases <= t.f2 then 2 else 1 end)*10 + 
		(case when a.sum <= t.m3 then 3 when a.sum <= t.m2 then 2 else 1 end) as rfm 
	from abc_clients a
	join thresholds t
	on a.abc = t.abc
)
select 
	rfm, 
	count(card) as cnt 
from rfm_result
group by rfm 
order by rfm
