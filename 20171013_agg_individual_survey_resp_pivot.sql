with tt1 (individual_id,survey_project_id,question_id,answer_id,answer_type,answer_length,response_value) as (


select '1203931733', 'p3079999153', '54901', '1286469', 'quantity', null, null union all
select '1203931733', 'p3079999153', '54902', '1286467', 'quantity', null, null union all
select '1203931733', 'p3079999153', '54904', '1286465', 'date', null, null union all
select '1203931733', 'p3079999153', '54905', '1286471', 'time', null, null union all
select '1203931733', 'p3079999153', '54908', '1286461', 'date', null, null union all
select '1203931733', 'p3079999153', '54909', '1286464', 'time', null, null union all
select '1203931733', 'p3079999153', '54912', '1286453', 'character', '60', '10002001058567100' union all
select '1203931733', 'p3079999153', '54914', '1286451', 'character', '60', 'THOMAS' union all
select '1203931733', 'p3079999153', '54916', '1286418', 'character', '60', 'MCCORMICK' union all
select '1203931733', 'p3079999153', '54919', '1286444', 'character', '60', 'tom@thomasmccormick.com' union all
select '1203931733', 'p3079999153', '54920', '1286425', 'character', '60', '13163537' union all
select '1203931733', 'p3079999153', '54921', '1286389', 'character', '60', 'BALLOT_2016' union all
select '1203931733', 'p3079999153', '54922', '1286423', 'character', '60', 'B' union all
select '1203931733', 'p3079999153', '54923', '1286438', 'character', '60', 'USA' union all
select '1203931733', 'p3079999153', '54924', '1286420', 'character', '60', '16B01B00' union all
select '1203931733', 'p3079999153', '54925', '1286719', 'single', null, 'IL' union all
select '1203931733', 'p3079999153', '54926', '1286449', 'character', '255', '1366442*7865992' union all
select '1203931733', 'p3079999153', '54927', '1286433', 'character', '2', '01' union all
select '1203931733', 'p3079999153', '54928', '1287306', 'single', null, '01' union all
select '1203931733', 'p3079999153', '54929', '1286673', 'single', null, 'B' union all
select '1203931733', 'p3079999153', '54930', '1286478', 'single', null, '1' union all
select '1203931733', 'p3079999153', '54931', '1286450', 'character', '60', 'img-1--r-1641208--s-YVJOIWYM--kc2-16B01B00I' union all
select '1203931733', 'p3079999153', '54932', '1286404', 'character', '60', 'img-1--r-1641208--s-YVJOIWYM--kc2-16B01B002' union all
select '1203931733', 'p3079999153', '54933', '1287366', 'single', null, '1' union all
select '1203931733', 'p3079999153', '54934', '1286523', 'single', null, '2' union all
select '1203931733', 'p3079999153', '54935', '1286381', 'character', '60', '1473026103803' union all
select '1203931733', 'p3079999153', '54941', '1286387', 'character', '4', '0904' union all
select '1203931733', 'p3079999153', '54942', '1286426', 'character', '4', '0918' union all
select '1203931733', 'p3079999153', '54955', '1286544', 'single', null, '1' union all
select '1203931733', 'p3079999153', '54960', '1286684', 'single', null, '2' union all
select '1203931733', 'p3079999153', '54960', '1286582', 'single', null, 'I' union all
select '1203931733', 'p3079999153', '54961', '1286953', 'multiple', null, '1' union all
select '1203931733', 'p3079999153', '54961', '1286630', 'multiple', null, '3' union all
select '1203931733', 'p3079999153', '54968', '1286430', 'character', '9', '16B01B00' union all
select '1203931733', 'p3079999153', '54969', '1286458', 'character', '2', '00' union all
select '1203931733', 'p3079999153', '54972', '1286448', 'character', '10', '16B01B002' union all
select '1203931733', 'p3079999153', '54977', '1286600', 'single', null, '0' union all
select '1203931733', 'p3079999153', '54978', '1286576', 'single', null, '1' union all
select '1203931733', 'p3079999153', '54979', '1286653', 'single', null, '0' union all
select '1203931733', 'p3079999153', '54980', '1286782', 'single', null, '0' union all
select '1203931733', 'p3079999153', '54981', '1286810', 'single', null, '0' union all
select '1203931733', 'p3079999153', '54982', '1286388', 'character', '11', '0|0' union all
select '1203931733', 'p3079999153', '54983', '1286406', 'character', '7', 'Desktop' union all
select '1203931733', 'p3079999153', '54984', '1286690', 'single', null, '2' )

select 
  individual_id
, survey_project_id
, question_id
, answer_id
, answer_length
, answer_type
, case when answer_type in ('character','single','multiple') then response_value else null end as response_text
, case when answer_type = 'date'                             then response_value else null end as answer_date
, case when answer_type = 'time'                             then response_value else null end as answer_time
, case when answer_type = 'quantity' then response_value else null end answer_length

from tt1

