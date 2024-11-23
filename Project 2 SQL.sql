with fb_ggl_data as (
select
	fabd.ad_date,
	coalesce(LOWER(substring(fabd.url_parameters, 'utm_campaign=([^#!&]+)')) ,
	' ') utm_campaign,
	coalesce(fabd.spend,
	0)spend,
	coalesce(fabd.impressions,
	0)impressions,
	coalesce(fabd.reach,
	0)reach,
	coalesce(fabd.clicks ,
	0)clicks,
	coalesce (fabd.leads,
	0)leads,
	coalesce(fabd.value,
	0)value
from
	facebook_ads_basic_daily fabd
union all
select
	gabd.ad_date,
	coalesce(LOWER(substring(gabd.url_parameters, 'utm_campaign=([^#!&]+)')) ,
	' ') utm_campaign,
	coalesce(gabd.spend,
	0)spend,
	coalesce(gabd.impressions,
	0)impressions,
	coalesce(gabd.reach,
	0)reach,
	coalesce(gabd.clicks,
	0)clicks,
	coalesce(gabd.leads,
	0)leads,
	coalesce(gabd.value,
	0)value
from
	google_ads_basic_daily gabd ),
monthly_stats as (
select
	date_trunc('month',
	ad_date)as "ad_month",
	case
		when utm_campaign = 'nan' then null
		else utm_campaign
	end,
	sum(spend::float) as "total_cost",
	sum(impressions::float) as "total_impressions",
	sum(reach::float) as "total_reach",
	sum(clicks::float) as "total_clicks",
	sum(leads::float) as "total_leads",
	sum(value::float) as "total_value",
	case
		when sum(impressions::float)= 0 then 0
		else (sum(clicks::float)/ sum(impressions::float))* 100
	end as ctr,
	case
		when sum(clicks::float)= 0 then 0
		else sum(spend::float)/ sum(clicks::float)
	end as cpc,
	case
		when sum(impressions::float)= 0 then 0
		else (sum(spend::float)/ sum(impressions::float))* 1000
	end as cpm,
	case
		when sum(spend::float)= 0 then 0
		else ((sum(value::float)-sum(spend::float))/ sum(spend::float))* 100
	end as romi
from
	fb_ggl_data
group by
	ad_month,
	utm_campaign),
	monthly_stats_with_changes as (
select
	ad_month,
	utm_campaign,
	total_cost,
	total_impressions,
	total_clicks,
	total_value,
	ctr,
	cpc,
	cpm,
	romi,
	lag(cpm) over(partition by utm_campaign
order by
	ad_month desc) as previous_month_cpm,
	lag(ctr) over(partition by utm_campaign
order by
	ad_month desc) as previous_month_ctr,
	lag(romi) over(partition by utm_campaign
order by
	ad_month desc) as previous_month_romi
from
	monthly_stats)
select
	ad_month,
	utm_campaign,
	total_cost,
	total_impressions,
	total_clicks,
	total_value,
	ctr,
	cpc,
	cpm,
	romi,
	case
		when previous_month_cpm>0 then cpm::numeric / previous_month_cpm-1
		when previous_month_cpm = 0
		and cpm>0 then 1
	end as cpm_change,
		case
			when previous_month_ctr>0 then ctr::numeric / previous_month_ctr-1
		when previous_month_ctr = 0
		and ctr>0 then 1
	end as ctr_change,
		case
			when previous_month_romi>0 then romi::numeric / previous_month_romi-1
		when previous_month_romi = 0
		and romi>0 then 1
	end as romi_change
from
		monthly_stats_with_changes
group by
		ad_month,
		utm_campaign,
		total_cost,
		total_impressions,
		total_clicks,
		total_value,
		previous_month_cpm,
		previous_month_ctr,
		previous_month_romi,
		ctr,
		cpc,
		cpm,
		romi
order by
	ad_month,
	utm_campaign