-- Топ-10 языков (Have)
SELECT Technology, RespondentCount, Percentage
FROM `surveydata-478616.tech_survey_data.top10_languages_haveworked`
ORDER BY RespondentCount DESC;

-- Топ-10 языков (Want)
SELECT Technology, RespondentCount, Percentage
FROM `surveydata-478616.tech_survey_data.top10_languages_wanttowork`
ORDER BY RespondentCount DESC;

-- Сравнение Have vs Want
SELECT Technology, HaveWorkedCount, WantToWorkCount, GrowthPercent
FROM `surveydata-478616.tech_survey_data.languages_have_vs_want`
ORDER BY GrowthPercent DESC;

-- Демография по странам (Топ-10)
SELECT Country, RespondentCount, Percentage
FROM `surveydata-478616.tech_survey_data.demographics_by_country`
ORDER BY RespondentCount DESC
LIMIT 10;