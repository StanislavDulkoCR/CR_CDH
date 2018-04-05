CREATE TABLE cr_temp.email_response_encode (


individual_id BIGINT NOT NULL ENCODE DELTA
, email VARCHAR (255) ENCODE Text255
, member_id NUMERIC (10) NOT NULL ENCODE MOSTLY32
, cell_id VARCHAR (10) NOT NULL ENCODE BYTEDICT
, catalog_type CHAR (5) NOT NULL ENCODE BYTEDICT
, legacy_individual_id BIGINT ENCODE DELTA32K
, campaign_id BIGINT
, campaign_name VARCHAR (250)
, opened_flag NUMERIC (1)
, bounced_flag NUMERIC (1)
, clicked NUMERIC (1)
, cell_subject VARCHAR (256)
, keycode VARCHAR (30)
, list_id INTEGER
, list_desc VARCHAR (256)
, campaign_optout CHAR
, email_finder_num INTEGER
, message_id INTEGER
, create_date TIMESTAMP NOT NULL ENCODE DELTA32K
, create_feed_instance_id VARCHAR (64) NOT NULL ENCODE RUNLENGTH
, update_date TIMESTAMP
, update_feed_instance_id VARCHAR (64)

)
    DISTKEY (individual_id)
    INTERLEAVED SORTKEY  (individual_id, cell_id, keycode, list_id);