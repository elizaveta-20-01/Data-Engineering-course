SELECT 
    g.state, 
    COUNT(*) as tv_count
FROM "elizaveta-data_database"."gold_user_profiles_enriched" g
JOIN "elizaveta-data_database"."sales_silver" s ON g.client_id = s.client_id
WHERE s.product_name = 'TV' 
  AND g.age BETWEEN 20 AND 30
GROUP BY g.state
ORDER BY tv_count DESC
LIMIT 1;