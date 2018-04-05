
create table audit.automation_gatekeeper
(
	primary_id integer distkey,
	secondary_id integer,
	gate_status varchar(64) DEFAULT 'closed',
	gate_opened_dt timestamp,
	gate_closed_dt timestamp,
	batch_id varchar(128),
	automation_name varchar(128),
    enabled BOOLEAN DEFAULT 'FALSE'
)
diststyle key
    SORTKEY (gate_opened_dt)
;