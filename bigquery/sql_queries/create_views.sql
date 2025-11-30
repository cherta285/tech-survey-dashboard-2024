-- ============================================================================
-- SQL VIEWS ДЛЯ LOOKER STUDIO DASHBOARD
-- Проект: surveydata-478616
-- Dataset: tech_survey_data
-- ============================================================================

-- ============================================================================
-- СТРАНИЦА 1: ТЕКУЩЕЕ ИСПОЛЬЗОВАНИЕ ТЕХНОЛОГИЙ (HAVE WORKED WITH)
-- ============================================================================

-- VIEW 1: Топ-10 языков программирования (Have Worked)
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.top10_languages_haveworked` AS
SELECT 
  Technology,
  COUNT(DISTINCT ResponseId) as RespondentCount,
  ROUND(COUNT(DISTINCT ResponseId) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.language_haveworked`
GROUP BY Technology
ORDER BY RespondentCount DESC
LIMIT 10;

-- VIEW 2: Топ-10 баз данных (Have Worked)
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.top10_databases_haveworked` AS
SELECT 
  Technology,
  COUNT(DISTINCT ResponseId) as RespondentCount,
  ROUND(COUNT(DISTINCT ResponseId) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.database_haveworked`
GROUP BY Technology
ORDER BY RespondentCount DESC
LIMIT 10;

-- VIEW 3: Все платформы (Have Worked) - без лимита, для Tree Map
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.all_platforms_haveworked` AS
SELECT 
  Technology,
  COUNT(DISTINCT ResponseId) as RespondentCount,
  ROUND(COUNT(DISTINCT ResponseId) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.platform_haveworked`
GROUP BY Technology
ORDER BY RespondentCount DESC;

-- VIEW 4: Топ-10 веб-фреймворков (Have Worked)
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.top10_webframes_haveworked` AS
SELECT 
  Technology,
  COUNT(DISTINCT ResponseId) as RespondentCount,
  ROUND(COUNT(DISTINCT ResponseId) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.webframe_haveworked`
GROUP BY Technology
ORDER BY RespondentCount DESC
LIMIT 10;

-- ============================================================================
-- СТРАНИЦА 2: БУДУЩИЕ ТЕХНОЛОГИЧЕСКИЕ ТРЕНДЫ (WANT TO WORK WITH)
-- ============================================================================

-- VIEW 5: Топ-10 языков программирования (Want to Work)
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.top10_languages_wanttowork` AS
SELECT 
  Technology,
  COUNT(DISTINCT ResponseId) as RespondentCount,
  ROUND(COUNT(DISTINCT ResponseId) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.language_wanttowork`
GROUP BY Technology
ORDER BY RespondentCount DESC
LIMIT 10;

-- VIEW 6: Топ-10 баз данных (Want to Work)
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.top10_databases_wanttowork` AS
SELECT 
  Technology,
  COUNT(DISTINCT ResponseId) as RespondentCount,
  ROUND(COUNT(DISTINCT ResponseId) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.database_wanttowork`
GROUP BY Technology
ORDER BY RespondentCount DESC
LIMIT 10;

-- VIEW 7: Все платформы (Want to Work)
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.all_platforms_wanttowork` AS
SELECT 
  Technology,
  COUNT(DISTINCT ResponseId) as RespondentCount,
  ROUND(COUNT(DISTINCT ResponseId) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.platform_wanttowork`
GROUP BY Technology
ORDER BY RespondentCount DESC;

-- VIEW 8: Топ-10 веб-фреймворков (Want to Work)
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.top10_webframes_wanttowork` AS
SELECT 
  Technology,
  COUNT(DISTINCT ResponseId) as RespondentCount,
  ROUND(COUNT(DISTINCT ResponseId) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.webframe_wanttowork`
GROUP BY Technology
ORDER BY RespondentCount DESC
LIMIT 10;

-- ============================================================================
-- СРАВНИТЕЛЬНЫЕ VIEWS (HAVE VS WANT)
-- ============================================================================

-- VIEW 9: Сравнение языков (Have vs Want) - для Combo Chart
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.languages_have_vs_want` AS
WITH have AS (
  SELECT 
    Technology,
    COUNT(DISTINCT ResponseId) as HaveCount
  FROM `surveydata-478616.tech_survey_data.language_haveworked`
  GROUP BY Technology
),
want AS (
  SELECT 
    Technology,
    COUNT(DISTINCT ResponseId) as WantCount
  FROM `surveydata-478616.tech_survey_data.language_wanttowork`
  GROUP BY Technology
),
top_have AS (
  SELECT Technology
  FROM have
  ORDER BY HaveCount DESC
  LIMIT 10
)
SELECT 
  h.Technology,
  COALESCE(h.HaveCount, 0) as HaveWorkedCount,
  COALESCE(w.WantCount, 0) as WantToWorkCount,
  COALESCE(w.WantCount, 0) - COALESCE(h.HaveCount, 0) as Difference,
  CASE 
    WHEN h.HaveCount > 0 THEN ROUND((COALESCE(w.WantCount, 0) - COALESCE(h.HaveCount, 0)) / h.HaveCount * 100, 1)
    ELSE 0
  END as GrowthPercent
FROM have h
LEFT JOIN want w USING (Technology)
WHERE h.Technology IN (SELECT Technology FROM top_have)
ORDER BY h.HaveCount DESC;

-- ============================================================================
-- СТРАНИЦА 3: ДЕМОГРАФИЯ
-- ============================================================================

-- VIEW 10: Респонденты по странам
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.demographics_by_country` AS
SELECT 
  Country,
  COUNT(*) as RespondentCount,
  ROUND(COUNT(*) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.demographics`
WHERE Country != 'Not Specified'
  AND Country_IsValid = TRUE
GROUP BY Country
ORDER BY RespondentCount DESC;

-- VIEW 11: Респонденты по возрасту
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.demographics_by_age` AS
SELECT 
  Age,
  COUNT(*) as RespondentCount,
  ROUND(COUNT(*) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.demographics`
WHERE Age != 'Not Specified'
  AND Age_IsValid = TRUE
GROUP BY Age
ORDER BY 
  CASE Age
    WHEN 'Under 18 years old' THEN 1
    WHEN '18-24 years old' THEN 2
    WHEN '25-34 years old' THEN 3
    WHEN '35-44 years old' THEN 4
    WHEN '45-54 years old' THEN 5
    WHEN '55-64 years old' THEN 6
    WHEN '65 years or older' THEN 7
    ELSE 8
  END;

-- VIEW 12: Респонденты по уровню образования
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.demographics_by_education` AS
SELECT 
  EdLevel,
  COUNT(*) as RespondentCount,
  ROUND(COUNT(*) / (SELECT COUNT(*) FROM `surveydata-478616.tech_survey_data.demographics`) * 100, 2) as Percentage
FROM `surveydata-478616.tech_survey_data.demographics`
WHERE EdLevel != 'Not Specified'
  AND EdLevel_IsValid = TRUE
GROUP BY EdLevel
ORDER BY RespondentCount DESC;

-- ============================================================================
-- ВСПОМОГАТЕЛЬНЫЕ VIEWS
-- ============================================================================

-- VIEW 13: Общая статистика по всем технологиям
CREATE OR REPLACE VIEW `surveydata-478616.tech_survey_data.overall_tech_stats` AS
SELECT 
  'Languages' as TechCategory,
  'Have Worked' as Status,
  COUNT(DISTINCT Technology) as UniqueTechnologies,
  COUNT(*) as TotalMentions,
  COUNT(DISTINCT ResponseId) as UniqueRespondents
FROM `surveydata-478616.tech_survey_data.language_haveworked`
UNION ALL
SELECT 
  'Languages' as TechCategory,
  'Want to Work' as Status,
  COUNT(DISTINCT Technology) as UniqueTechnologies,
  COUNT(*) as TotalMentions,
  COUNT(DISTINCT ResponseId) as UniqueRespondents
FROM `surveydata-478616.tech_survey_data.language_wanttowork`
UNION ALL
SELECT 
  'Databases' as TechCategory,
  'Have Worked' as Status,
  COUNT(DISTINCT Technology) as UniqueTechnologies,
  COUNT(*) as TotalMentions,
  COUNT(DISTINCT ResponseId) as UniqueRespondents
FROM `surveydata-478616.tech_survey_data.database_haveworked`
UNION ALL
SELECT 
  'Databases' as TechCategory,
  'Want to Work' as Status,
  COUNT(DISTINCT Technology) as UniqueTechnologies,
  COUNT(*) as TotalMentions,
  COUNT(DISTINCT ResponseId) as UniqueRespondents
FROM `surveydata-478616.tech_survey_data.database_wanttowork`
UNION ALL
SELECT 
  'Platforms' as TechCategory,
  'Have Worked' as Status,
  COUNT(DISTINCT Technology) as UniqueTechnologies,
  COUNT(*) as TotalMentions,
  COUNT(DISTINCT ResponseId) as UniqueRespondents
FROM `surveydata-478616.tech_survey_data.platform_haveworked`
UNION ALL
SELECT 
  'Platforms' as TechCategory,
  'Want to Work' as Status,
  COUNT(DISTINCT Technology) as UniqueTechnologies,
  COUNT(*) as TotalMentions,
  COUNT(DISTINCT ResponseId) as UniqueRespondents
FROM `surveydata-478616.tech_survey_data.platform_wanttowork`
UNION ALL
SELECT 
  'Web Frameworks' as TechCategory,
  'Have Worked' as Status,
  COUNT(DISTINCT Technology) as UniqueTechnologies,
  COUNT(*) as TotalMentions,
  COUNT(DISTINCT ResponseId) as UniqueRespondents
FROM `surveydata-478616.tech_survey_data.webframe_haveworked`
UNION ALL
SELECT 
  'Web Frameworks' as TechCategory,
  'Want to Work' as Status,
  COUNT(DISTINCT Technology) as UniqueTechnologies,
  COUNT(*) as TotalMentions,
  COUNT(DISTINCT ResponseId) as UniqueRespondents
FROM `surveydata-478616.tech_survey_data.webframe_wanttowork`;

-- ============================================================================
-- ПРОВЕРКА СОЗДАННЫХ VIEWS
-- ============================================================================

-- Проверьте, что все views созданы успешно:
-- SELECT table_name 
-- FROM `surveydata-478616.tech_survey_data.INFORMATION_SCHEMA.TABLES`
-- WHERE table_type = 'VIEW'
-- ORDER BY table_name;