#!/bin/bash


cp /etc/yandex/statbox-logbroker/publish-lags.yaml /tmp/publish.yaml

cd /tmp
logbroker-lagschecker.sh  --tsv --store --fielddateinterval 10 | grep -E '^sink' > publish_sh-data.tsv
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

	curl -v -H "StatRobotUser: robot_mrproc" -H "StatRobotPassword: password_mrproc" -F "tskv_data=@publish_sh-data.tsv"  -F "cube_config=<publish.yaml" -F "title=Отставание экспорта из Кафки" -F "cmd=create_report" -k "https://$server/Statface/Add/KPI/Kafka/Lags"

	#rm publish_sh-data.tsv  # commented to simplify debug
	rm publish.yaml
fi

