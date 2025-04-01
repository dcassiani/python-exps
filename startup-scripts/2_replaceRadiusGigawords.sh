#!/bin/bash

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
DIRPARENT=$(dirname "$SCRIPTPATH")

source $DIRPARENT/conf

LOGFILE=$LOGPATH/`date +\%Y\%m\%d`/Job2_Bugfix1_`date +\%Y\%m\%d\%H\%M`.log
LOGSPARK=$LOGPATH/`date +\%Y\%m\%d`/spark/Job2_Bugfix1_`date +\%Y\%m\%d\%H\%M`.log
mkdir -p $LOGPATH/`date +\%Y\%m\%d`/spark

info(){
    echo "[$(date +%Y-%m-%d\ %H:%M:%S) (INFO)] $1" 2>&1 | tee --append $LOGFILE
}

error(){
    echo "[$(date +%Y-%m-%d\ %H:%M:%S) (ERROR)] $1" 2>&1 | tee --append $LOGFILE
}


HIVE_TABLE="dwr_db.leituras_duplicadas"
HDFS_ORIGINAL_PATH="/dwr_db.db/leituras_duplicadas"
HDFS_CLONE_PATH="/backup/leituras_duplicadas"
HDFS_BACKUP_PATH="/backup/original"

data=${1:-"20250128"}

PARTITIONS=( $data )

# validacao de seguranca
for PARTITION in "${PARTITIONS[@]}"; do

    BACKUP_PATH="${HDFS_BACKUP_PATH}/partition=${PARTITION}"

	if hdfs dfs -test -d "$BACKUP_PATH"; then
		info "${BACKUP_PATH} path ja existe - suspeita de execucao duplicada - ABORTANDO"
		#se esse erro - apos checagem - limpeza com: 
		#hdfs dfs -rm -r -skipTrash /backup/original/partition=20241029
		exit 1
    fi
done


# Backup e substitution
for PARTITION in "${PARTITIONS[@]}"; do

    ORIGINAL_PATH="${HDFS_ORIGINAL_PATH}/partition=${PARTITION}"
    BACKUP_PATH="${HDFS_BACKUP_PATH}/partition=${PARTITION}"
	CLONE_PATH="${HDFS_CLONE_PATH}/partition=${PARTITION}"

    hdfs dfs -mkdir -p "$BACKUP_PATH"

    info "Movendo arquivos da partition ${PARTITION} para BACKUP"
    if hdfs dfs -test -d "$ORIGINAL_PATH"; then
		info "hdfs dfs -mv ${ORIGINAL_PATH}/* ${BACKUP_PATH}/"
        hdfs dfs -mv $ORIGINAL_PATH/* $BACKUP_PATH/.
#		hdfs dfs -mv /backup/original/partition=20241108/* /dwr_db.db/leituras_duplicadas/partition=20241108/.
        info "Partition ${PARTITION} movida com sucesso para o backup"
    else
        error "Partition ${PARTITION} não encontrada em ${ORIGINAL_PATH}"
    fi
	
	info "Movendo arquivos da partition ${PARTITION} para RADIUS"
    if hdfs dfs -test -d "$CLONE_PATH"; then
        hdfs dfs -cp $CLONE_PATH/* $ORIGINAL_PATH/.
        info "Partition ${PARTITION} movida com sucesso para o RADIUS"
    else
        error "Partition ${PARTITION} não encontrada em ${CLONE_PATH} - retornando arquivos originais"
		hdfs dfs -cp $BACKUP_PATH/* $ORIGINAL_PATH/.
        info "Partition ${PARTITION} movida com sucesso para o RADIUS"
    fi

done

info "Atualizando metadados no Hive..."
hive -e "MSCK REPAIR TABLE ${HIVE_TABLE};"

info "Processo concluido"
exit 0
