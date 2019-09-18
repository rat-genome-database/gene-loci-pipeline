#!/usr/bin/env bash
# default_db.xml must have a bean 'DataSource' configuring connection to RGD database
#   and 'CarpeDataSource' configuring connection to Rat CarpeNovo database
#
. /etc/profile

APPHOME=/home/rgddata/pipelines/GeneLociPipeline
cd $APPHOME

SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
if [ "$SERVER" = "REED" ]; then
  EMAIL_LIST=rgd.developers@mcw.edu
fi


# by default, run for all map keys
MAPKEY=0
# unless user specifies in cmdline for which map_key to run
if [[ "$1" ]]; then
  MAPKEY=$1
  #echo " running for MAP_KEY=$MAPKEY"
else
  #echo " running for all map keys"
fi

./_run.sh --mapKey=$MAPKEY

mailx -s "[$SERVER] GeneLoci pipeline" $EMAIL_LIST < $HOMEDIR/logs/summary.log
