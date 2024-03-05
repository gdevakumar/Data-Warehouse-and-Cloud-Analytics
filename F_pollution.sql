CREATE TABLE `air_pollution.F_pollution` AS 
SELECT 
o3_mean, o3_max_value, o3_max_hour, o3_aqi, co_mean, co_max_value, co_max_hour, co_aqi, 
so2_mean, so2_max_value, so2_max_hour, so2_aqi, no2_mean, no2_max_value, no2_max_hour, no2_aqi,
date_id, location_id  
FROM `staging_dataset.pollution`;