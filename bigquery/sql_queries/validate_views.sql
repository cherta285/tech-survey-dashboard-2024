-- Проверка всех созданных views
-- Запустите этот скрипт, чтобы убедиться, что все views работают

-- 1. Топ-10 языков (Have)
SELECT 'top10_languages_haveworked' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.top10_languages_haveworked`
UNION ALL

-- 2. Топ-10 языков (Want)
SELECT 'top10_languages_wanttowork' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.top10_languages_wanttowork`
UNION ALL

-- 3. Топ-10 БД (Have)
SELECT 'top10_databases_haveworked' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.top10_databases_haveworked`
UNION ALL

-- 4. Топ-10 БД (Want)
SELECT 'top10_databases_wanttowork' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.top10_databases_wanttowork`
UNION ALL

-- 5. Все платформы (Have)
SELECT 'all_platforms_haveworked' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.all_platforms_haveworked`
UNION ALL

-- 6. Все платформы (Want)
SELECT 'all_platforms_wanttowork' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.all_platforms_wanttowork`
UNION ALL

-- 7. Топ-10 фреймворков (Have)
SELECT 'top10_webframes_haveworked' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.top10_webframes_haveworked`
UNION ALL

-- 8. Топ-10 фреймворков (Want)
SELECT 'top10_webframes_wanttowork' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.top10_webframes_wanttowork`
UNION ALL

-- 9. Сравнение Have vs Want
SELECT 'languages_have_vs_want' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.languages_have_vs_want`
UNION ALL

-- 10. Демография по странам
SELECT 'demographics_by_country' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.demographics_by_country`
UNION ALL

-- 11. Демография по возрасту
SELECT 'demographics_by_age' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.demographics_by_age`
UNION ALL

-- 12. Демография по образованию
SELECT 'demographics_by_education' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.demographics_by_education`
UNION ALL

-- 13. Общая статистика
SELECT 'overall_tech_stats' as view_name, COUNT(*) as row_count
FROM `surveydata-478616.tech_survey_data.overall_tech_stats`

ORDER BY view_name;