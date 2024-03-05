ALTER TABLE `us-pollution-data.staging_dataset.dates` 
ADD COLUMN day_of_week STRING;

UPDATE `us-pollution-data.staging_dataset.dates`
SET day_of_week = FORMAT_DATE('%A', date)
WHERE month < 15;

ALTER TABLE `us-pollution-data.staging_dataset.dates` 
ADD COLUMN quarter INTEGER;

UPDATE `us-pollution-data.staging_dataset.dates` 
SET quarter = DIV(month + 2, 3)
WHERE month < 15;
