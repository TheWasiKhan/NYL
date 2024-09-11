#!/usr/bin/ksh

###############################################################################
#######
####### FILE: seqMasterPartyRefTableLoadRun.sh
#######
####### DESCRIPTION: looping the job for dim client comb client rel table
#######
####### USAGE: sh SSeqPzDimClientCombClientRel2Run.sh <hive_schema> <job_name> <temp_hdfs_dir> <table_name> <tgt_hive_schema> <warning_limit> <pz_hive_schema>
#######
####### EXAMPLE: sh SeqPzDimClientCombClientRel2Run.sh nyledm SeqPzDimClientCombClientRel2 /workspace/public/infosphere/edm/processed/public/nyledm/tmp dim_client_comb_client_rel edm_nyledm 0 procpub
#######
####### sh /nyl/data/projects/infosphere/edm/processed_party/scripts/SeqPzDimClientCombClientRel2Run.sh phase2 SeqPzDimClientCombClientRel2 s3://nyl-edm-dev-curated/edm/temp dim_client_comb_client_rel phase2 0 pdm  
#######
####### sh -x #$PROJECT_SCRIPT_DIR#/SeqPzDimClientCombClientRel2Run.sh #uvarInitializeVariable.uv_hive_schema# SeqPzDimClientCombClientRel2  s3://#$EDM_S3_BUCKET_NAME#/#$EDM_S3_TARGET_FILE_PATH# #uvarInitializeVariable.uv_TblNmDimClientCombClientRel# #uvarInitializeVariable.uv_hive_schema# 0 #$EDM_REDSHIFT_TARGET_FULL_SCHEMA#
#######
####### Date       Version   Developer      Description
####### ---------- --------- -------------- -----------------------------------
####### 2020-02-19 1.0       krishna         Initial release
#######
###############################################################################


source /nyl/data/cyberarkcreds/AwsCliProperties.sh

#check input parameters
if [ "$#" -lt 5 ]; then
     echo "Invalid parameter list"
        echo "Script needs 7 mandatory parameter"
        echo "Parameter 1: Schema Name"
        echo "Parameter 2: Job Name"
        echo "Parameter 3: S3 dir"
                    echo "Parameter 4: Table Name "
                    echo "Parameter 5: Full hive schema"
        echo "Parameter 6: Warning Limit"
        echo "Usage: $0 <schema_name> <job_name> <s3_dir> <warning_limit>"
                                echo "Parameter 7: HiveDBConnectionString"
        exit 1
fi

#set parameters
script_name=`basename $0 | cut -d'.' -f1`
HIVE_SCHEMA=`echo $1 | tr '[:upper:]' '[:lower:]'`
DS_JOBNAME="$2"
S3_DIR="$3"
TABLE_Name="$4"
FULL_HIVE_SCHEMA="$5"
WARN_LIMIT="-warn $6"
EDM_HUB_PROCESSED_HIVE_SCHEMA="$7"
shortSchema=`echo $1 | cut -d'_' -f1`

### Initializing Variables
if [[ `realpath $0 | grep "preprod" | wc -c` -eq 0 ]]; then
        . /nyl/edm/common_scripts/edh_all_env_vars_processed_party.sh X
else
        . /nyl/edm/common_scripts/edh_all_env_vars_processed_party.sh preprod
fi

# Creating log file
LOGFILE=${EDHPROCESSED_PARTY_LOG_DIR}/nyledm/${script_name}_${CURR_TIMESTAMP}.log
echo "Log File is ${LOGFILE}"

paramFile=${EDHPROCESSED_PARTY_TEMP_DIR}/nyledm/${script_name}_${DS_JOBNAME}.param

# Touch temp files
touch ${paramFile}

echo -en "\n\n##############EDM Processed Reference DataStage Job Run Log################\n\n"  > ${LOGFILE}
echo -en "Date: `date '+%C%y-%m-%d %H:%M:%S.%N'` \n\n" >> ${LOGFILE}
echo -en "DS Host: $(hostname) \n\n" >> ${LOGFILE}
echo -en "DS Project: ${EDHPROCESSED_PARTY_DS_PROJNAME} \n\n" >> ${LOGFILE}
echo -en "DS Sequence Name: ${DS_JOBNAME} \n\n" >> ${LOGFILE}
echo -en "Schema Name: ${HIVE_SCHEMA} \n\n" >> ${LOGFILE}


echo "jp_HIVE_SCHEMA=${HIVE_SCHEMA}" > ${paramFile}
echo "AuroraConfigDBCredentials=AURORACONFIGDB" >> ${paramFile}
echo "RedshiftDBCredentials=REDSHIFTDB" >> ${paramFile}
echo "S3Credentials=S3KEY" >> ${paramFile}


### Check the Matched file
#/workspace/public/infosphere/edm/processed/public/nyledm/tmp/procpub_edm_nyledm/dim_client_comb_client_rel_nyledm_matched.txt

#match_file = `aws s3 ls ${S3_DIR}/dim_client_comb_client_rel/dim_client_comb_client_rel_matched.txt | wc -c`
##chk_match_file=$?

#if [ ${match_file} -ne 0 ];
#then
#echo "File (dim_client_comb_client_rel_nyledm_matched.txt) is exist" | tee -a ${LOGFILE}
#else
#echo "File (dim_client_comb_client_rel_nyledm_matched.txt) is not exist" | tee -a ${LOGFILE}
#exit 1
#fi

if [[ `aws s3 ls ${S3_DIR}/${TABLE_Name}/${TABLE_Name}_matched.txt | wc -c` -gt 0 ]];
then
echo "File (dim_client_comb_client_rel_matched.txt) is exist" | tee -a ${LOGFILE}
else
echo "File (dim_client_comb_client_rel_matched.txt) is not exist" | tee -a ${LOGFILE}
exit 1
fi


### start the loop if count greate then 1
#while [ ${chk_match_file} -eq 0 ];
#do

### remove matched source file
### aws s3 rm ${S3_DIR}/${TABLE_Name}/dim_client_comb_client_rel_full.txt.0

### aws s3 rm ${S3_DIR}/${TABLE_Name}/dim_client_comb_client_rel_full.txt.1

### aws s3 rm ${S3_DIR}/${TABLE_Name}/dim_client_comb_client_rel_matched_trg.txt.1

### aws s3 rm ${S3_DIR}/${TABLE_Name}/dim_client_comb_client_rel_matched_trg.txt.2

### aws s3 rm ${S3_DIR}/${TABLE_Name}/dim_client_comb_client_rel_matched_trg.txt.0

### count the matched file
Count_match_file=`aws s3 ls ${S3_DIR}/${TABLE_Name}/${TABLE_Name}_matched.txt|wc -l`


while [ $Count_match_file -gt 0 ];
do

#echo "Matched file contains data " | tee -a ${LOGFILE}

echo "Running DataStage Job $DS_JOBNAME"

# Source dsenv file
source /opt/IBM/InformationServer/Server/DSEngine/dsenv
/opt/IBM/InformationServer/Server/DSEngine/bin/dsjob -run -jobstatus -param jpTableName=${TABLE_Name} -param jp_TGT_FULL_HIVE_SCHEMA=${HIVE_SCHEMA} -param jp_HIVE_SCHEMA=${HIVE_SCHEMA} -paramfile ${paramFile} edm_processed_party $DS_JOBNAME.${HIVE_SCHEMA} > ${LOGFILE}
return_code=$?

#echo -en "DataStage Job $DS_JOBNAME Run Started\n\n" | tee -a ${LOGFILE}

#run the datastage job and get the status of the job
#$DS_JOB -run -jobstatus ${WARN_LIMIT} $EDHPROCESSED_PARTY_DS_PROJNAME $DS_JOBNAME.${HIVE_SCHEMA}

#$DS_JOB -run -jobstatus -param jpTableName=${TABLE_Name} -param jp_TGT_FULL_HIVE_SCHEMA=${HIVE_SCHEMA} -param jp_HIVE_SCHEMA=${HIVE_SCHEMA} $EDHPROCESSED_PARTY_DS_PROJNAME  $DS_JOBNAME.${HIVE_SCHEMA} > ${LOGFILE}
#return_code=$?


# Get job info from DataStage
echo -en "Get the job information for the job $DS_JOBNAME, Return String is..\n\n" | tee -a ${LOGFILE}
RETURN_STRING=`$DS_JOB -jobinfo $EDHPROCESSED_PARTY_DS_PROJNAME $DS_JOBNAME.${HIVE_SCHEMA}`
echo $RETURN_STRING | tee -a ${LOGFILE}

# check for empty string in RETURN STRING and check return code
if [ $return_code -ne 1 ]; then
        echo -en "\n\n ${DS_JOBNAME} Failed, please check the logs\n\n" | tee -a ${LOGFILE}
        echo "${DS_JOBNAME} Failed With Error Message"
        echo "DS Job Info RC  : $return_code"
        echo "DS Job Info"
        echo "-----------"
        exit 1
else
        echo -en "${DS_JOBNAME} Finished Successfully.\n\n"
        echo -en "\n\n ${DS_JOBNAME} Finished Successfully.\n\n" | tee -a ${LOGFILE}
fi


### remove matched source file
aws s3 rm ${S3_DIR}/${TABLE_Name}/${TABLE_Name}_matched.txt

### rename matched target file to matched source file
aws s3 mv ${S3_DIR}/${TABLE_Name}/${TABLE_Name}_matched_trg.txt ${S3_DIR}/${TABLE_Name}/${TABLE_Name}_matched.txt

Rename_count_match_file=`aws s3 cp ${S3_DIR}/${TABLE_Name}/${TABLE_Name}_matched.txt - |wc -l`
Count_match_file=$Rename_count_match_file

done

echo -en "\n\n##############END OF SCRIPT################################\n\n" | tee -a ${LOGFILE}

exit 0

