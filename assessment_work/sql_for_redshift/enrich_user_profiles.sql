CREATE TABLE "elizaveta-data_database"."gold_user_profiles_enriched" AS
SELECT 
    c.client_id,
    COALESCE(NULLIF(c.first_name, ''), split_part(u.full_name, ' ', 1)) as first_name,
    COALESCE(NULLIF(c.last_name, ''), split_part(u.full_name, ' ', 2)) as last_name,
    c.email,
    c.registration_date,
    COALESCE(NULLIF(c.state, ''), u.state) as state,
    u.phone_number,
    (year(current_date) - year(cast(u.birth_date as date))) as age
FROM "elizaveta-data_database"."customers_silver" c
LEFT JOIN "elizaveta-data_database"."user_profiles_silver" u ON c.email = u.email;
