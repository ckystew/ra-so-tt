SELECT
  q.id
  ,title
  ,q.creation_date
  ,view_count
  ,tags
  ,accepted_answer_id
  ,IF(accepted_answer_id IS NOT NULL, TRUE, FALSE) AS answer_accepted
  ,IF(answer_count > 0, TRUE, FALSE) AS answered
FROM 
    `bigquery-public-data.stackoverflow.posts_questions` q
