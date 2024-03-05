DELETE FROM `us-pollution-data.staging_dataset.pollution` 
WHERE
    o3_mean IS NULL 
    OR co_mean IS NULL
    OR so2_mean IS NULL
    OR no2_mean IS NULL;