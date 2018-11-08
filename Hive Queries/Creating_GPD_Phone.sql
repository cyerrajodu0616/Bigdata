add jar ${hivevar:AETPMC}/${hivevar:ONPA_OWNER}/ajax_dev/jars/West-0.0.1-SNAPSHOT.jar;
add file ${hivevar:AETPMC}/${hivevar:ONPA_OWNER}/ajax_dev/props/stdphoneexlude.dat;
create temporary function phone_std as 'hive_UDf.Phone_Std';


drop table if exists arbitrated_gpd_phones;

create table arbitrated_gpd_phones ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' as 
with ajax_phones as (select cnt_sit_mzp_id,
						  cnt_mzp_id,
						  concat_ws(',',cdb_phone,
						  cib_phone,
						  sec_telephone,
						  contact_obc_out.cbc_prospect_provided_phone,
						  equifax_site_attributes_out.seq_all_phone,
						  ctp_additional_phone_1,
						  ctp_additional_phone_2,
						  ctp_additional_phone_3,
						  ctp_business_phone,
						  crd_1_number,
						  crd_2_number,
						  crd_3_number,
						  crd_4_number,
						  crd_5_number,
						  crd_6_number,
						  crd_7_number,
						  crd_8_number,
						  crd_9_number,
						  crd_10_number) as phones 
from contact_out left join contact_attrib_dun_out on (cnt_mzp_id = cdb_cnt_mzp_id) 
				 left join contact_attrib_iusa_out on (cnt_mzp_id = cib_cnt_mzp_id) 
				 left join contact_attrib_radius_out on (cnt_mzp_id = crd_cnt_mzp_id) 
				 left join contact_obc_out on (cnt_mzp_id = cbc_cnt_mzp_id) 
				 left join contact_third_party_out on (cnt_mzp_id = ctp_cnt_mzp_id ) 
				 left join equifax_site_attributes_out on (cnt_sit_mzp_id = seq_sit_mzp_id) 
				 left join site_execureach_out on (cnt_sit_mzp_id = sec_sit_mzp_id )), 
Ajax_all_phones as (
			    select distinct sit_mzp_id,
				       split(concat_ws(',',sit_best_phone,phones),',') mzp_phones 
			      from site_out a left join 
				   ajax_Phones b on (sit_mzp_id = cnt_sit_mzp_id)
			           where (a.SIT_ABI_FLAG = 'Y' OR a.SIT_DMI_FLAG = 'Y' OR a.SIT_EQUIFAX_FLAG = 'Y')
				) 
select * from (
				SELECT distinct sit_mzp_id, 
				               phone_std(phone,'${hivevar:AETPMC}/${hivevar:ONPA_OWNER}/ajax_dev/props/stdphoneexlude.dat') as phone_std
				  FROM Ajax_all_phones 
				  LATERAL VIEW explode(mzp_phones) adTable AS phone
				) a 
where length(a.phone_std) > 0;
