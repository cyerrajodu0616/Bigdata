echo "*********************"
echo "first parameter = table_name"
echo "second_parametr = output db name"
echo "third parameter = Input db name"
echo "*************************************************************************************"
Hive_Table_Name=$1
METICS_DB=$2
Helios_DB=$3
Metics_Table_Name=$4
creat_sql_path=$5

metics_hive_sql=${creat_sql_path}/${METICS_DB}_${Metics_Table_Name}.txt
metics_hive_url=jdbc:hive2://aeamxp00e0:10000/${METICS_DB}

rm -rf ${metics_hive_sql}
mkdir -p ${creat_sql_path}
touch ${metics_hive_sql}

echo "input table: "${Hive_Table_Name} 
echo "Metics DB: "${METICS_DB}
echo "Helios DB: "${Helios_DB}
echo "Script Path: "${creat_sql_path}
echo "create Script file name: "${metics_hive_sql}

hive -S -e "set hive.cli.print.header=true;use ${Helios_DB};show create table $Hive_Table_Name" >> ${metics_hive_sql}
sed -i 1,2d ${metics_hive_sql}
sed -i "1s|^|CREATE EXTERNAL TABLE IF NOT EXISTS ${METICS_DB}.${Metics_Table_Name}( \n|" ${metics_hive_sql}

echo "/opt/mapr/hive/hive/bin/beeline -u ${metics_hive_url} -n ${USER} -f ${metics_hive_sql}"
#${HIVE_HOME}/bin/beeline -u ${metics_hive_url} -n ${USER} -f ${metics_hive_sql}
/opt/mapr/hive/hive/bin/beeline -u ${metics_hive_url} -n ${USER} -f ${metics_hive_sql}
helios_ret_cde=$?

if [ $helios_ret_cde -ne 0 ];then
       echo `date +'\%Y\%m\%d'`."Metics mdb hive script failed with RC: $helios_ret_cde"
       exit $helios_ret_cde
else
       echo `date +'\%Y\%m\%d'`."Metics mdb hive script completed with RC: $helios_ret_cde"
       exit $helios_ret_cde
fi
echo "*************************************************************************************"
