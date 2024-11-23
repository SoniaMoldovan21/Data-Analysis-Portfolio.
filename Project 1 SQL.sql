
	--Temporary Function--
	create or replace
	function pg_temp.utm_function(url_parameters varchar) 
	returns varchar as $$
	begin 
		return coalesce(LOWER(substring(url_parameters, 'utm_campaign=([^#!&]+)')) ,
	' ');
end ;

$$
	language plpgsql
	
		
	with fb_ggl_data as (
select
	fabd.ad_date,
	pg_temp.utm_function(fabd.url_parameters) as "utm_campaign", 
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
	pg_temp.utm_function(gabd.url_parameters) as "utm_campaign",
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
	google_ads_basic_daily gabd )
	select
	ad_date,
	 case
		when "utm_campaign" = 'nan' then null
		else "utm_campaign"
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
	end as cmp,
	case
		when sum(spend::float)= 0 then 0
		else ((sum(value::float)-sum(spend::float))/ sum(spend::float))* 100
	end as romi
from
	fb_ggl_data fgd
group by
	ad_date,
	"utm_campaign"
