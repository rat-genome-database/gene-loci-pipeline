#!/usr/bin/env bash
#
# Clean duplicate rows from GENE_LOCI table.
# Usage:
#   ./cleanDuplicates.sh          -- clean duplicates for all map keys
#   ./cleanDuplicates.sh 380      -- clean duplicates for map_key=380
#
. /etc/profile

APPHOME=/home/rgddata/pipelines/gene-loci-pipeline
cd $APPHOME

SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
if [ "$SERVER" = "REED" ]; then
  EMAIL_LIST=rgd.devops@mcw.edu
fi

MAPKEY=all
if [[ "$1" ]]; then
  MAPKEY=$1
fi

./_run.sh --cleanDuplicates --mapKey=$MAPKEY

mailx -s "[$SERVER] GeneLoci cleanDuplicates" $EMAIL_LIST < $APPHOME/logs/summary.log
