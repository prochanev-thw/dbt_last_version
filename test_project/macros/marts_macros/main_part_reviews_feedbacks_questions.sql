{% macro main_part_reviews_feedbacks_questions() %}
WITH raw_fb AS (SELECT create_dttm AS datetime,
       cast(create_dttm AS date) AS dt,
       EXTRACT ('month' FROM create_dttm) AS mon,
       to_char(create_dttm, 'YYYYMM') AS yyyymm,
       object_type_id,
       config_id,
       ugc_feedback_pk, -- JOIN на hub ugc_feedback_id
--        user_id,
--        (feedback_content::json ->> 'meta')::json ->> 'device_platform' AS os_name,
--        object_id AS store_code,
--        CAST((feedback_content::json ->> 'meta')::json ->> 'transaction_id' AS numeric) AS transaction_id,
--        CAST(((feedback_content::json ->> 'meta')::json ->> 'oprosso') IS NOT NULL AS int) AS is_oprosso,
       json_array_elements((feedback_content::json ->> 'answers')::json) AS json_answer,
       feedback_content::json AS fc,
       ROW_NUMBER() over (partition by ugc_feedback_pk order by effective_dttm desc) AS rn -- https://track.magnit.ru/browse/DWH-5917
FROM {{ ref('sat_ugc_feedback') }}
),
fb AS (SELECT raw_fb.datetime,
		raw_fb.dt,
		raw_fb.mon,
		raw_fb.yyyymm,
		raw_fb.object_type_id,
		raw_fb.config_id,
-- 		raw_fb.user_id,
-- 		raw_fb.os_name,
-- 		coalesce(raw_fb.store_code, cast(raw_fb.fc ->> 'meta' as json) ->> 'store_code') AS store_code,
-- 		raw_fb.transaction_id,
-- 		raw_fb.is_oprosso,
       COALESCE((fc ->> 'meta')::json ->> 'card_number', json_answer ->> 'virtual_card') AS loyalty_card_number,
       json_answer ->> 'answer' AS answer,
       json_answer ->> 'question_id' AS question_id,
       CASE WHEN (json_answer ->> 'answer') LIKE {% raw %}'{%}'{% endraw %} THEN  json_object_keys((json_answer ->> 'answer')::json)
       ELSE Null END AS subquestion_id,
       huf.ugc_feedback_id,
       raw_fb.ugc_feedback_pk
        ,case when object_type_id = 2 then (json_answer ->> 'product')::json ->> 'name'
           else null end as product_name
       ,case when object_type_id = 2 then (json_answer ->> 'product')::json ->> 'categories'
           else null end as product_categories
       ,case when object_type_id = 2 then (json_answer ->> 'product')::json ->> 'article'
           else null end as product_id
FROM raw_fb
JOIN {{ ref('hub_ugc_feedback') }} huf
ON raw_fb.ugc_feedback_pk = huf.ugc_feedback_pk
WHERE TRUE
AND rn = 1
AND NOT (((json_answer ->> 'answer') like '[%]') OR ((json_answer ->> 'answer') like '[]'))
),
fb_feedback_needed as (
    SELECT
        fb_feedback_needed_answers.datetime,
        fb_feedback_needed_answers.dt,
        fb_feedback_needed_answers.mon,
        fb_feedback_needed_answers.yyyymm,
        fb_feedback_needed_answers.object_type_id,
        fb_feedback_needed_answers.config_id,
--         fb_feedback_needed_answers.user_id,
--         fb_feedback_needed_answers.os_name,
--         fb_feedback_needed_answers.store_code,
--         fb_feedback_needed_answers.transaction_id,
--         fb_feedback_needed_answers.is_oprosso,
        fb_feedback_needed_answers.loyalty_card_number,
        cast(fb_feedback_needed_answers.single_question_param_json ->> 'title' as text) as answer,
        fb_feedback_needed_answers.question_id,
        fb_feedback_needed_answers.subquestion_id,
        fb_feedback_needed_answers.ugc_feedback_id,
        fb_feedback_needed_answers.ugc_feedback_pk,
                 fb_feedback_needed_answers.product_id,
                 fb_feedback_needed_answers.product_categories,
                 fb_feedback_needed_answers.product_name
    FROM (SELECT
              fb_with_answer_array.datetime,
                 fb_with_answer_array.dt,
                 fb_with_answer_array.mon,
                 fb_with_answer_array.yyyymm,
                 fb_with_answer_array.object_type_id,
                 fb_with_answer_array.config_id,
--                  fb_with_answer_array.user_id,
--                  fb_with_answer_array.os_name,
--                  fb_with_answer_array.store_code,
--                  fb_with_answer_array.transaction_id,
--                  fb_with_answer_array.is_oprosso,
                 fb_with_answer_array.loyalty_card_number,
                 fb_with_answer_array.answer,
                 fb_with_answer_array.question_id,
                 fb_with_answer_array.subquestion_id,
                 fb_with_answer_array.ugc_feedback_id,
                 fb_with_answer_array.ugc_feedback_pk,
                 fb_with_answer_array.product_id,
                 fb_with_answer_array.product_categories,
                 fb_with_answer_array.product_name,
                 suq.question_params,
                 CASE WHEN fb_with_answer_array.answer IS NOT Null AND suq.question_params IS NOT Null
                    THEN json_array_elements((suq.question_params::json ->> 'answers')::json)
                    END AS single_question_param_json,
                 CASE WHEN fb_with_answer_array.answer IS NOT Null AND suq.question_params IS NOT Null
                    THEN json_array_elements((suq.question_params::json ->> 'answers')::json) ->> 'id' = fb_with_answer_array.answer
                    END AS answer_id_match
          FROM (SELECT raw_fb.datetime
                     , raw_fb.dt
                     , raw_fb.mon
                     , raw_fb.yyyymm
                     , raw_fb.object_type_id
                     , raw_fb.config_id
--                      , raw_fb.user_id
--                      , raw_fb.os_name
--                      , raw_fb.store_code
--                      , raw_fb.transaction_id
--                      , raw_fb.is_oprosso
                     , COALESCE((fc ->> 'meta')::json ->> 'card_number', json_answer ->> 'virtual_card') AS loyalty_card_number
                     , CASE
                           WHEN (json_answer ->> 'answer') LIKE '[%]' THEN cast((json_answer ->> 'answer')::json->>0 as text)  -- Василий сказал, что можно забирать только первый элемент
                           WHEN (json_answer ->> 'answer') LIKE '[]' THEN Null
                            END as answer
                     , cast(json_answer ->> 'question_id' as text)           AS question_id
                     , cast(null as text)                                     AS subquestion_id
                     , huf.ugc_feedback_id
                     , raw_fb.ugc_feedback_pk
                ,case when object_type_id = 2 then (json_answer ->> 'product')::json ->> 'name'
                   else null end as product_name
               ,case when object_type_id = 2 then (json_answer ->> 'product')::json ->> 'categories'
                   else null end as product_categories
               ,case when object_type_id = 2 then (json_answer ->> 'product')::json ->> 'article'
                   else null end as product_id
                FROM raw_fb
                JOIN {{ ref('hub_ugc_feedback') }} huf
                ON raw_fb.ugc_feedback_pk = huf.ugc_feedback_pk
                WHERE TRUE
                  AND rn = 1
                  AND (((json_answer ->> 'answer') like '[%]') OR ((json_answer ->> 'answer') like '[]'))
                  AND NOT (json_answer ->> 'answer') like {% raw %}'{%'{% endraw %}
                ) as fb_with_answer_array
          LEFT JOIN (
                SELECT
                    suq_window.question_id,
                    suq_window.question_params,
                    suq_window.effective_dttm
                FROM (select h.config_id,
                                h.question_id,
                                s.question_params,
                                s.effective_dttm,
                                row_number() over (partition by s.ugc_question_pk order by s.effective_dttm desc) as rn
                         from dds.hub_ugc_question h
                                  join dds.sat_ugc_question s --убираем рефы, чтоб не ждать загрузку, это костыль
                                       using (ugc_question_pk)
                        WHERE 1 = 1
                        AND s.ugc_type = 'feedback_needed' -- Василий сказал, что пока его интересуют только feedback_needed answers
                ) AS suq_window
                WHERE suq_window.rn = 1) AS suq
          ON fb_with_answer_array.question_id = suq.question_id) AS fb_feedback_needed_answers
    WHERE 1=1
    AND fb_feedback_needed_answers.answer_id_match IS NOT False
),
raw_matrix_subqs AS (
SELECT raw.config_id,
       raw.question_id,
       raw.question_title,
	   raw.type,
	   ugc_type,
	   COALESCE(matrix_questions ->> '_id', matrix_questions ->> 'id') as subquestion_id,
	   -- фикс с coalesce answers+title, options+(_id, id)
	   COALESCE(matrix_questions ->> 'text', matrix_questions ->> 'title') AS subquestion_title
FROM (SELECT h.config_id,
		   h.question_id,
		   s.question_text AS question_title,
		   s.question_type AS type,
		   s.ugc_type,
		   CASE WHEN s.question_type = 'MATRIX' then
		   -- фикс с coalesce
            json_array_elements(COALESCE(s.question_params::json ->> 'answers', s.question_params::json ->> 'options')::json)
                else Null end matrix_questions, -- https://track.magnit.ru/browse/DWH-5828
		   ROW_NUMBER() over (partition by s.ugc_question_pk order by s.effective_dttm desc) AS rn
	FROM dds.hub_ugc_question h
    join dds.sat_ugc_question s --убираем рефы, чтоб не ждать загрузку, это костыль
    using (ugc_question_pk)
	WHERE s.question_type = 'MATRIX') raw
WHERE raw.rn = 1
),
raw_non_matrix_subqs AS (
SELECT config_id,
	   question_id,
	   question_title,
	   type,
	   ugc_type,
	   subquestion_id,
	   subquestion_title
FROM (SELECT h.config_id,
			   h.question_id,
			   s.question_text AS question_title,
			   s.question_type AS type,
			   s.ugc_type,
			   CAST(NULL AS varchar) AS subquestion_id,
			   CAST(NULL AS varchar) AS subquestion_title,
			   ROW_NUMBER() over (partition by s.ugc_question_pk order by s.effective_dttm desc) AS rn
		FROM dds.hub_ugc_question h
        join dds.sat_ugc_question s --убираем рефы, чтоб не ждать загрузку, это костыль
        using (ugc_question_pk)
		WHERE question_type != 'MATRIX') raw
WHERE raw.rn = 1
),
fb_subqs_non_matrix AS (
SELECT fb_union.datetime,
       fb_union.dt,
       fb_union.mon,
       fb_union.yyyymm,
--        fb_union.os_name,
--        fb_union.store_code,
       fb_union.object_type_id,
       fb_union.config_id,
       fb_union.id,
--        fb_union.transaction_id,
       fb_union.loyalty_card_number,
--        fb_union.user_id,
       fb_union.question_id,
       fb_union.subquestion_id,
	   nmq.question_title,
	   nmq.subquestion_title,
	   nmq.ugc_type,
	   fb_union.is_subquestion,
	   fb_union.answer,
-- 	   fb_union.is_oprosso
       fb_union.product_id,
       fb_union.product_categories,
       fb_union.product_name
FROM (
    SELECT fb.datetime,
           fb.dt,
           fb.mon,
           fb.yyyymm,
--            fb.os_name,
--            fb.store_code,
           fb.object_type_id,
           fb.config_id,
           fb.ugc_feedback_id AS id,
--            fb.transaction_id,
           fb.loyalty_card_number,
--            fb.user_id,
           fb.question_id,
           fb.subquestion_id,
           0 AS is_subquestion,
           fb.answer,
--            fb.is_oprosso
            fb.product_id,
            fb.product_categories,
            fb.product_name
    FROM fb
    UNION ALL
    SELECT fb_feedback_needed.datetime,
           fb_feedback_needed.dt,
           fb_feedback_needed.mon,
           fb_feedback_needed.yyyymm,
--            fb_feedback_needed.os_name,
--            fb_feedback_needed.store_code,
           fb_feedback_needed.object_type_id,
           fb_feedback_needed.config_id,
           fb_feedback_needed.ugc_feedback_id AS id,
--            fb_feedback_needed.transaction_id,
           fb_feedback_needed.loyalty_card_number,
--            fb_feedback_needed.user_id,
           fb_feedback_needed.question_id,
           fb_feedback_needed.subquestion_id,
           0 AS is_subquestion,
           fb_feedback_needed.answer,
--            fb_feedback_needed.is_oprosso
            fb_feedback_needed.product_id,
            fb_feedback_needed.product_categories,
            fb_feedback_needed.product_name
    FROM fb_feedback_needed
) as fb_union
INNER JOIN raw_non_matrix_subqs nmq
ON fb_union.question_id = nmq.question_id
AND fb_union.config_id = nmq.config_id
),
fb_subqs_matrix AS (
SELECT fb.datetime,
       fb.dt,
       fb.mon,
       fb.yyyymm,
--        fb.os_name,
--        fb.store_code,
       fb.object_type_id,
       fb.config_id,
       fb.ugc_feedback_id AS id,
--        fb.transaction_id,
       fb.loyalty_card_number,
--        fb.user_id,
       fb.question_id,
       mq.subquestion_id,
	   mq.question_title,
	   mq.subquestion_title,
	   mq.ugc_type,
	   CASE WHEN mq.subquestion_id IS NULL THEN 0 ELSE 1 END AS is_subquestion,
	   -- и обработка пустых ответов
	   case when not (answer like {% raw %}'{%}'{% endraw %}) then answer
	        when (answer::json) ->> mq.subquestion_id != '' then COALESCE(((answer::json) ->> mq.subquestion_id)::json ->> 'value', (answer::json) ->> mq.subquestion_id)
            else NULL end AS answer, -- https://track.magnit.ru/browse/DWH-5828
-- 	   fb.is_oprosso
       fb.product_id,
       fb.product_categories,
       fb.product_name
FROM fb
INNER JOIN raw_matrix_subqs mq
ON fb.question_id = mq.question_id
AND fb.subquestion_id = mq.subquestion_id
AND fb.config_id = mq.config_id
),
fb_subqs AS (
SELECT *
FROM fb_subqs_non_matrix
UNION ALL
SELECT *
FROM fb_subqs_matrix
)
SELECT f.datetime AS datetime,
       f.object_type_id,
       f.config_id,
       f.id,
       f.question_id,
       f.subquestion_id,
       f.question_title,
       f.subquestion_title,
       f.ugc_type,
       f.is_subquestion,
       f.answer,
       f.product_id,
       f.product_categories,
       f.product_name
FROM fb_subqs f
WHERE (f.ugc_type NOT IN ('contact_mail', 'contact_phone') or f.ugc_type is null)
{% endmacro %}