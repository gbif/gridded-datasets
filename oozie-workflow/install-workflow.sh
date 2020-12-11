#!/usr/bin/env bash
set -e
set -o pipefail

ENV=$1
TOKEN=$2

echo "Installing gridded datasets workflow for $ENV"

echo "Get latest gridded-datasets config profiles from GitHub"
curl -Ss -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/gridded-datasets/$ENV/gridded.properties

START=$(date +%Y-%m-%d)T$(grep '^startHour=' gridded.properties | cut -d= -f 2)Z
FREQUENCY="$(grep '^frequency=' gridded.properties | cut -d= -f 2)"
OOZIE=$(grep '^oozie.url=' gridded.properties | cut -d= -f 2)

# Gets the Oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $OOZIE -jobtype coordinator -filter name=Gridded-Datasets | awk 'NR==3 {print $1}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  sudo -u hdfs oozie job -oozie $OOZIE -kill $WID
fi

echo "Assembling jar for $ENV"
# Oozie uses timezone UTC
mvn -Dgridded.frequency="$FREQUENCY" -Dgridded.start="$START" -DskipTests -Duser.timezone=UTC clean install -U

echo "Copy to Hadoop"
sudo -u hdfs hdfs dfs -rm -r /gridded-datasets-workflow/
sudo -u hdfs hdfs dfs -copyFromLocal target/gridded-datasets-workflow /
sudo -u hdfs hdfs dfs -copyFromLocal /etc/hive/conf/hive-site.xml /gridded-datasets-workflow/lib/

echo "Start Oozie gridded datasets job"
sudo -u hdfs oozie job --oozie $OOZIE -config gridded.properties -run
