#!/bin/bash

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
DIRPARENT=$(dirname "$SCRIPTPATH")

source $DIRPARENT/conf

LOGFILE=$LOGPATH/`date +\%Y\%m\%d`/Job2_Bugfix0_`date +\%Y\%m\%d\%H\%M`.log
LOGSPARK=$LOGPATH/`date +\%Y\%m\%d`/spark/Job2_Bugfix0_`date +\%Y\%m\%d\%H\%M`.log
mkdir -p $LOGPATH/`date +\%Y\%m\%d`/spark

info(){
    echo "[$(date +%Y-%m-%d\ %H:%M:%S) (INFO)] $1" 2>&1 | tee --append $LOGFILE
}

error(){
    echo "[$(date +%Y-%m-%d\ %H:%M:%S) (ERROR)] $1" 2>&1 | tee --append $LOGFILE
}

kerberos(){
        info "kinit -R -kt ${KERBEROS_KEYTAB} ${KERBEROS_PRINCIPAL}@${KERBEROS_REALM}"
        kinit -R -kt ${KERBEROS_KEYTAB} ${KERBEROS_PRINCIPAL}@${KERBEROS_REALM} >> $LOGFILE 2>&1
}

kerberos &&
{

info "Iniciando BugfixStage1"

NUMEXECORES=4
NUMDRIVERCORES=4
EXECMEM=12G
DRIVERMEM=8G
EXECMEMOVER=4096
DRIVERMEMOVER=8192
INITIALEXEC=8
MAXEXEC=60
MINEXEC=4
MAXRESULTSIZE=3G
SQLPARTITIONS=2500
NETWORKTIMEOUT=3600
EXECHEARTBEATINT=1800


NAME=BugfixStage1
data=${1:-"20250128"}
contagem_max=${2:-"900"}

kerberos &&
{

    SPARK_PRINT_LAUNCH_COMMAND=true /usr/bin/spark-submit \
        --name $NAME \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory ${DRIVERMEM} \
        --executor-memory ${EXECMEM} \
        --conf spark.executor.cores=${NUMEXECORES} \
        --conf spark.driver.cores=${NUMDRIVERCORES} \
        --conf spark.driver.maxResultSize=${MAXRESULTSIZE} \
        --conf spark.driver.memoryOverhead=${DRIVERMEMOVER} \
        --conf spark.executor.memoryOverhead=${EXECMEMOVER} \
        --conf spark.yarn.maxAppAttemps=4 \
        --conf spark.task.maxFailures=4 \
        --conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.initialExecutors=${INITIALEXEC} \
        --conf spark.dynamicAllocation.maxExecutors=${MAXEXEC} \
        --conf spark.dynamicAllocation.initialExecutors=${MINEXEC} \
        --conf spark.shuffle.service.enabled=true \
	--conf spark.dynamicAllocation.shuffleTracking.enabled=true \
        --conf spark.sql.shuffle.partitions=${SQLPARTITIONS} \
        --conf spark.shuffle.service.port=7337 \
        --conf spark.ui.port=4040 \
        --conf spark.sql.shuffle.partitions=${SQLPARTITIONS} \
        --conf spark.default.parallelism=800 \
        --conf spark.kryoserializer.buffer.max=1024 \
        --conf spark.executor.extraJavaOptions="-XX:+UseCompressedOops" \
        --conf spark.network.timeout=${NETWORKTIMEOUT} \
        --conf spark.executor.heartbeatInterval=${EXECHEARTBEATINT} \
        --conf spark.executorEnv.DB_RAW=$DB_RAW \
        --conf spark.executorEnv.DB_RAW_HDFSPATH=$DB_RAW_HDFSPATH \
        --conf spark.executorEnv.DB_REPORT=$DB_REPORT \
        --conf spark.executorEnv.DB_REPORT_HDFSPATH=$DB_REPORT_HDFSPATH \
        --conf spark.executorEnv.DB_WORK=$DB_WORK \
        --conf spark.executorEnv.DB_WORK_HDFSPATH=$DB_WORK_HDFSPATH \
        --conf spark.executorEnv.LOGLEVEL=$LOGLEVEL \
        --conf spark.executorEnv.PYSPARK_PYTHON=/bin/python \
        --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python \
        --conf spark.executorEnv.PYTHONPATH=/lib/cert-elk-url.tgz:/lib/certifi-2021.10.8-py2.py3-none-any.whl:/lib/urllib3-1.26.15-py2.py3-none-any.whl:/lib/.all:/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.9-src.zip:/opt/cloudera/parcels/CDH/lib/spark/python:/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.9-src.zip:/opt/cloudera/parcels/CDH/lib/spark/python/lib/pyspark.zip:/lib/certifi-2021.10.8-py2.py3-none-any.whl \
        --py-files /lib/certifi-2021.10.8-py2.py3-none-any.whl,/lib/urllib3-1.26.15-py2.py3-none-any.whl \
        --driver-java-options "-XX:+UseCompressedOops " \
                /data01/GA/scripts/python/1b_repair-gigawords.py $data $contagem_max > $LOGSPARK 2>&1 &

		
	APP_ID=$(yarn application -list -appStates ACCEPTED,RUNNING | grep $NAME | awk '{print $1}');
    counter=0
    while [ "${APP_ID}" = "" ]
    do
        if [ "$counter" -gt 20  ]; then
            error "Job not accepted! Exiting!";
            exit 2
        else
            counter=$((counter+1))
            info "Not accepted ${counter}";
            APP_ID=$(yarn application -list -appStates ACCEPTED,RUNNING | grep $NAME | awk '{print $1}');
        fi
        sleep 5
    done

    APP_RESULT=$(yarn application -status ${APP_ID} | grep Final-State | awk '{print $3}');
    while [ "${APP_RESULT}" = "UNDEFINED" ]
    do
        info "Running job (${APP_ID})";
        APP_RESULT=$(yarn application -status ${APP_ID} | grep Final-State | awk '{print $3}');
        sleep 5
    done
    wait

	info "Atualizando metadados no Hive..."
	hive -e "MSCK REPAIR TABLE dwr_db.leituras_duplicadas_bkp;"

    ###############################
    ## RESULT                	 ##
    ###############################
    if [[ ( "${APP_RESULT}" = "SUCCEEDED" ) ]]; then
        info "$NAME=${APP_RESULT}"
        exit 0
    else
        error "$NAME=${APP_RESULT}"
        exit 1
    fi
} || {
    error "Kerberos Fail!"
    exit 2
		
}
}
