{{ config(
    materialized='table',
    partition_by={
      "field": "year_month",
      "data_type": "string"
    }
)}}

WITH tags_agg AS (
    SELECT
    tags AS tag
    ,DATE_TRUNC(q.creation_date, MONTH) AS year_month
    ,COUNT(q.id) AS total_questions
    ,SUM(IF(q.answer_accepted, 1, 0)) AS total_satisfied_questions
    ,SUM(IF(q.answered, 1, 0)) AS total_answered_questions
    ,SUM(IF(q.answered, 0, 1)) AS total_unanswered_questions
    ,SUM(q.view_count) AS total_views
    ,SUM(IF(q.answer_accepted, 0, q.view_count)) AS total_views_on_unsatisfied_questions
    ,SUM(IF(q.answered, 0, q.view_count)) AS total_views_on_unanswered_questions
    ,AVG(DATETIME_DIFF(IF(a.id = q.accepted_answer_id, a.creation_date, NULL), q.creation_date, MINUTE)) AS average_minutes_to_satisfy
    FROM 
        {{ ref('stack-overflow-questions') }} q
        ,UNNEST(SPLIT(q.tags, '|')) AS tags

    LEFT JOIN
        {{ ref('stack-overflow-answers') }} a
    ON
        a.parent_id = q.id

    WHERE
        EXTRACT(YEAR FROM q.creation_date) >= 2019 -- hard cutoff for perf reasons

    GROUP BY 
        1, 2
),

tags_rates AS (
    SELECT 
        *
        ,total_satisfied_questions / total_questions AS satisfied_question_rate
        ,total_unanswered_questions / total_questions AS unanswered_question_rate
        ,total_views_on_unsatisfied_questions / total_views AS unsatisfied_view_rate
        ,total_views_on_unanswered_questions / total_views AS unanswered_view_rate
        ,total_questions / SUM(total_questions) OVER(PARTITION BY year_month) AS monthly_tag_question_share
        ,SUM(total_questions) OVER(PARTITION BY tag) / SUM(total_questions) OVER() AS overall_tag_question_share -- Inefficient, dupe rows every month, could do with seperate query.
    FROM
        tags_agg
),

tags_agg_ranked AS (
    SELECT 
        *
        ,RANK() OVER(PARTITION BY year_month ORDER BY total_questions DESC) AS monthly_tag_question_rank
        ,RANK() OVER(PARTITION BY year_month ORDER BY total_views_on_unsatisfied_questions DESC) AS monthly_tag_unsatisfied_rank
    FROM
        tags_rates

)

SELECT  
    *
FROM
    tags_agg_ranked