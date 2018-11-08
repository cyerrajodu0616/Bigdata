Src_Hive_Table_Name=$1
Target_Helios_DB=$2
Src_Helios_DB=$3
Target_Table_Name=$4
creat_sql_path=$5

metics_hive_sql=${creat_sql_path}/${Target_Helios_DB}_${Target_Table_Name}.txt
metics_hive_url=jdbc:hive2://aeamxp00e0:10000/${Target_Helios_DB}

rm -rf ${metics_hive_sql}
mkdir -p ${creat_sql_path}
touch ${metics_hive_sql}

echo "input table: "${Src_Hive_Table_Name} 
echo "Metics DB: "${Target_Helios_DB}
echo "Helios DB: "${Src_Helios_DB}
echo "Script Path: "${creat_sql_path}
echo "create Script file name: "${metics_hive_sql}

hive -S -e "set hive.cli.print.header=true;use ${Src_Helios_DB};show create table $Src_Hive_Table_Name" > ${metics_hive_sql}
sed -i 1,2d ${metics_hive_sql}
sed -i "1s|^|CREATE EXTERNAL TABLE IF NOT EXISTS ${Target_Helios_DB}.${Target_Table_Name}( \n|" ${metics_hive_sql}

echo "/opt/mapr/hive/hive/bin/beeline -u ${metics_hive_url} -n ${USER} -f ${metics_hive_sql}"
#${HIVE_HOME}/bin/beeline -u ${metics_hive_url} -n ${USER} -f ${metics_hive_sql}
hive -S -e "drop table if exists ${Target_Helios_DB}.${Target_Table_Name};"
hive -f ${metics_hive_sql}
helios_ret_cde=$?

if [ $helios_ret_cde -ne 0 ];then
       echo `date +'\%Y\%m\%d'`."External table creation is failed: $helios_ret_cde"
       exit $helios_ret_cde
else
       echo `date +'\%Y\%m\%d'`."External table creation is completed with RC: $helios_ret_cde"
       exit $helios_ret_cde
fi
echo "*************************************************************************************"

