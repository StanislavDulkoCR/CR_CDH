with donor as (select * from prod.PRINT_TRANSACTION pt inner join prod.action_header ah on pt.hash_action_id = ah.hash_action_id where set_code in ('B','D') )
, rec as (select * from prod.PRINT_TRANSACTION pt inner join prod.action_header ah on pt.hash_action_id = ah.hash_action_id where set_code in ('C','E') )

select 

 donor.ACTION_TYPE  ,rec.ACTION_TYPE
,donor.set_code     ,rec.set_code
,donor.action_date  ,rec.action_date
,donor.action_amount, rec.action_amount
,donor.*, rec.*
from donor inner join rec on donor.addtl_action_id = rec.addtl_action_id
--left join prod.PRINT_TRANSACTION pt on donor.HASH_ACTION_ID = pt.HASH_ACTION_ID
--and rec.action_id = 1050950000059602015	
--and rec.addtl_action_id = 933800190018 
and donor.individual_id = 1218256586
--and donor.individual_id = 1218256586
;



Offline (Print)
(SOURCE_NAME = 'CDS', ACTION_TYPE = 'ORDER')
If PRINT_TRANSACTION.SET_CD is 'A' then ACTION_HEADER.ACTION_AMOUNT.
Else
If the PRINT_TRANSACTION.SET_CD is 'B' or 'D' (donor) 
sum the ACTION_HEADER.ACTION_AMOUNT plus all
TMP_PRINT_RECIPIENT_ORDER.SUM_ACTION_AMOUNT of the recipient records 

where ACTION_HEADER.HASH_ACTION_ID of 
the donor order equals PRINT_TRANSACTION.HASH_ACTION_ID of the recipient order 

and PRINT_TRANSACTION.SET_CD of the recipient is 'C' or 'E'.
Else set to zero."