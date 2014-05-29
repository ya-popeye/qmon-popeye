#!/bin/bash
#	curl -v -H "StatRobotUser: robot_mrproc" -H "StatRobotPassword: password_mrproc" -F "tskv_data=@publish-data.tsv"  -F "cube_config=<publish.yaml" -F "title=Строки из Кафки" -F "cmd=create_report" -F "_allow_change_job=1" -k "https://stat-beta.yandex-team.ru/Statface/Add/KPI/Kafka/Lines"

#exit 0

p=`pwd`
mkdir /tmp/publish-lines
cd /tmp/publish-lines
cp /etc/yandex/statbox-logbroker/publish-lines.yaml publish.yaml

echo starting
perl /usr/share/statbox-logbroker/export-lines.pl > tmp-export
echo "export lines done"
if [ $? == 0 ]
then
	env=`cat /etc/yandex/environment.type`

	server=
	if [ $env == "testing" ]
	then
		server=stat-beta.yandex-team.ru
	elif [ $env == "production" ] 
	then
		server=stat.yandex-team.ru
	fi

	cat tmp-export | perl /usr/share/statbox-logbroker/import-lines.pl > publish-data.tsv 
	echp "import lines done"

	curl -v -H "StatRobotUser: robot_mrproc" -H "StatRobotPassword: password_mrproc" -F "tskv_data=@publish-data.tsv"  -F "cube_config=<publish.yaml" -F "title=Строки из Кафки" -F "cmd=create_report" -F "_allow_change_job=1" -k "https://$server/Statface/Add/KPI/Kafka/Lines"

	echo "publishing done"

#	rm publish-data.tsv
#	rm publish.yaml

fi

cd $p

