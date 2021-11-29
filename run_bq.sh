#Create dataset
bq --location=us mk -d ecommerce

bq query --use_legacy_sql=false \
'CREATE TABLE ecommerce.data PARTITION BY
   event_date_partition AS
SELECT event_date, PARSE_DATE("%Y%m%d", event_date) as event_date_partition, event_timestamp, event_name, (SELECT ev.value.string_value FROM UNNEST(event_params) as ev where ev.key="page_title") as event_title, (SELECT ev.value.string_value FROM UNNEST(event_params) as ev where ev.key="page_location") as event_page_location,user_pseudo_id, device.category, device.mobile_brand_name, user_first_touch_timestamp, TIMESTAMP_MICROS(user_first_touch_timestamp) as user_first_touch_at, user_ltv.revenue as user_ltv_revenue, geo.continent as geo_continent, stream_id,traffic_source.medium as traffic_source  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`  WHERE user_ltv.revenue>0 and geo.continent="Asia" and traffic_source.medium="organic"'

