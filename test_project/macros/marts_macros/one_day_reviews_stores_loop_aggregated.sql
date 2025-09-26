{% macro one_day_reviews_stores_loop_aggregated(result) %}
select report_date
     , store_code
     , store_name
     , store_address
     , store_region
     , store_okrug
     , store_city
     , store_filial
     , store_format
     , sum(score * motivation_weight * relevance_weight)                              acc_score_weight_multiplication_sum
     , sum(motivation_weight * relevance_weight)                                      acc_weight_sum
     , sum(comment_is_not_null)                                                       acc_review_count
     , sum(score)                                                                     acc_scores_sum
     , count(score)                                                                   acc_scores_qnt
     , sum(case when score = 1 then 1 else 0 end)                                     acc_score1_count
     , sum(case when score = 2 then 1 else 0 end)                                     acc_score2_count
     , sum(case when score = 3 then 1 else 0 end)                                     acc_score3_count
     , sum(case when score = 4 then 1 else 0 end)                                     acc_score4_count
     , sum(case when score = 5 then 1 else 0 end)                                     acc_score5_count
     , sum(case
               when feedback_date = report_date then score * motivation_weight * relevance_weight
               else 0 end)                                                            score_weight_multiplication_sum
     , sum(case
               when feedback_date = report_date then motivation_weight * relevance_weight
               else 0 end)                                                            weight_sum
     , sum(case when feedback_date = report_date then comment_is_not_null else 0 end) review_count
     , sum(case when feedback_date = report_date then score else 0 end)               scores_sum
     , sum(case when feedback_date = report_date then 1 else 0 end)                   scores_qnt
     , sum(case when feedback_date = report_date and score = 1 then 1 else 0 end)     score1_count
     , sum(case when feedback_date = report_date and score = 2 then 1 else 0 end)     score2_count
     , sum(case when feedback_date = report_date and score = 3 then 1 else 0 end)     score3_count
     , sum(case when feedback_date = report_date and score = 4 then 1 else 0 end)     score4_count
     , sum(case when feedback_date = report_date and score = 5 then 1 else 0 end)     score5_count
from (
    select feedback_id
         , '{{ result }}'::date AS report_date
         , feedback_datetime
         , feedback_date
         , transaction_id
         , store_code
         , store_name
         , store_address
         , store_region
         , store_okrug
         , store_city
         , store_filial
         , store_format
         , score
         , comment
         , case when comment is not null and comment != '' then 1 else 0 end AS comment_is_not_null
         , 1 / sqrt(DATE_PART('day', '{{result }}'::timestamp - feedback_datetime) + 1) AS relevance_weight
         , case
               when score = 1 then 1.15
               when score = 2 then 0.9
               when score = 3 then 0.8
               when score = 4 then 0.7
               when score = 5 then 0.5
        end AS motivation_weight
         , ROW_NUMBER() over (partition by magnit_id order by feedback_datetime desc) AS rn  -- to select only the most fresh review for every user
    from {{ref('reviews_stores_loop')}} r
    where r.feedback_date between '{{result}}'::date - interval '60 day' and '{{result}}'::date
) reviews_loop_transformed
where rn = 1
group by report_date
       , store_code
       , store_name
       , store_address
       , store_region
       , store_okrug
       , store_city
       , store_filial
       , store_format
{% endmacro %}