#!/usr/bin/env bash
#

. /etc/profile
APPNAME=GeneLociPipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

ELIST=mtutaj@mcw.edu
if [ "$SERVER" == "REED" ]; then
    ELIST="mtutaj@mcw.edu"
fi

DB_OPTS="-Dspring.config=$APPDIR/../properties/default_db.xml"
LOG4J_OPTS="-Dlog4j.configuration=file://$APPDIR/properties/log4j.properties"
declare -x "GENE_LOCI_PIPELINE_OPTS=$DB_OPTS $LOG4J_OPTS"
bin/$APPNAME "$@" > cron.log 2>&1

