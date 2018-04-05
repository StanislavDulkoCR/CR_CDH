
WITH cte_ddl AS (

    SELECT
        schemaname
        , tablename
        , replace(replace(ddl, '	,PRIMARY KEY (', ''), ')', '')  pk_column_list

    FROM cr_temp.v_sdtech_ddl
    WHERE 1 = 1
        AND ddl LIKE '%PRIMARY%'
        and schemaname = 'prod'
        and tablename not like 'agg%'
--         AND tablename = 'individual_address'
    limit 10
), cte_split AS (

    SELECT
        cte_ddl.schemaname
        , tablename
        , regexp_substr(pk_column_list,'(\\w+)') AS pk_column_name_1
        , regexp_substr(pk_column_list,'(\\,\\ \\w+){1}') AS pk_column_name_2
        , replace(regexp_substr(pk_column_list,'(\\,\\ \\w+){2}') ,regexp_substr(pk_column_list,'(\\,\\ \\w+){1}'),'') AS pk_column_name_3
        , replace(regexp_substr(pk_column_list,'(\\,\\ \\w+){3}') ,regexp_substr(pk_column_list,'(\\,\\ \\w+){2}'),'') AS pk_column_name_4
        , replace(regexp_substr(pk_column_list,'(\\,\\ \\w+){4}') ,regexp_substr(pk_column_list,'(\\,\\ \\w+){3}'),'') AS pk_column_name_5
        , replace(regexp_substr(pk_column_list,'(\\,\\ \\w+){5}') ,regexp_substr(pk_column_list,'(\\,\\ \\w+){4}'),'') AS pk_column_name_6
        , replace(regexp_substr(pk_column_list,'(\\,\\ \\w+){6}') ,regexp_substr(pk_column_list,'(\\,\\ \\w+){5}'),'') AS pk_column_name_7
        , replace(regexp_substr(pk_column_list,'(\\,\\ \\w+){7}') ,regexp_substr(pk_column_list,'(\\,\\ \\w+){6}'),'') AS pk_column_name_8
        , replace(regexp_substr(pk_column_list,'(\\,\\ \\w+){8}') ,regexp_substr(pk_column_list,'(\\,\\ \\w+){7}'),'') AS pk_column_name_9
        , replace(regexp_substr(pk_column_list,'(\\,\\ \\w+){9}') ,regexp_substr(pk_column_list,'(\\,\\ \\w+){8}'),'') AS pk_column_name_10

    FROM cte_ddl
)

SELECT
    cs.schemaname
    , cs.tablename
    , decode(cs.pk_column_name_1,  '', NULL, cs.pk_column_name_1)   AS pk_column_name_1
    , decode(cs.pk_column_name_2,  '', NULL, cs.pk_column_name_2)   AS pk_column_name_2
    , decode(cs.pk_column_name_3,  '', NULL, cs.pk_column_name_3)   AS pk_column_name_3
    , decode(cs.pk_column_name_4,  '', NULL, cs.pk_column_name_4)   AS pk_column_name_4
    , decode(cs.pk_column_name_5,  '', NULL, cs.pk_column_name_5)   AS pk_column_name_5
    , decode(cs.pk_column_name_6,  '', NULL, cs.pk_column_name_6)   AS pk_column_name_6
    , decode(cs.pk_column_name_7,  '', NULL, cs.pk_column_name_7)   AS pk_column_name_7
    , decode(cs.pk_column_name_8,  '', NULL, cs.pk_column_name_8)   AS pk_column_name_8
    , decode(cs.pk_column_name_9,  '', NULL, cs.pk_column_name_9)   AS pk_column_name_9
    , decode(cs.pk_column_name_10, '', NULL, cs.pk_column_name_10)  AS pk_column_name_10
FROM cte_split cs
;