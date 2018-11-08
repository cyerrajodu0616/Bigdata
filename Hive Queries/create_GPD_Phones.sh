YR=`date +'%Y'`
MM=`date +'%m'`
dbname=ajax_${YR}${MM}

echo $dbname

table_count=`hive -S -e"use ${dbname};show tables" | grep -i -w "arbitrated_gpd_phones" | wc -l`
if [[ ${table_count} -eq 1 ]]
then 
echo "Table is present please delete before re-creating. Flow skipped" 
exit 0
else 
echo "table is not present" 
required_table_count=`hive -S -e"use ${dbname};show tables" | grep -i -E -w "(contact_out|contact_attrib_dun_out|contact_attrib_iusa_out|contact_attrib_radius_out|contact_obc_out|contact_third_party_out|equifax_site_attributes_out|site_execureach_out|site_out)" | wc -l`
echo "Req table count: $required_table_count"
if [[ ${required_table_count} -eq 9 ]] 
then 
echo "all tables are present" 
cd ${AETPMC}/${ONPA_OWNER}/ajax_dev/sqls
hive -i compress.sql -f Creating_GPD_Phone.sql --hivevar db=${dbname} --hivevar AETPMC=${AETPMC} --hivevar ONPA_OWNER=${ONPA_OWNER}
ret=$?
if [ $ret -eq 0 ]
then
echo "Process completed sucessfully"
exit 0
fi
else
echo "Tables are missing. Exiting with return code zero to consolidate the complete process status"
exit 0 
fi
fi

