#!/bin/bash

export PATH=/usr/sbin:/usr/bin:/sbin:/bin
export LC_ALL=C
export LANG=C

pids="statbox-kafka"

if [ -f /var/lock/statbox-logbroker-watchdog ]
then 
	echo ok
else
	(
		flock -n 9 || exit 1
		for pid in $pids
		do
			cmd="restart"
			if [ $pid == "statbox-kafka" ]
			then
				(test -f /var/run/statbox/$pid.pid && test -f /proc/`cat /var/run/statbox/$pid.pid`/cmdline || (/usr/share/statbox-logbroker/reallocate.py /storage >> /var/log/statbox/reallocate.log || exit 1))
				cmd="force-restart"
			fi
			(test -f /var/run/statbox/$pid.pid && test -f /proc/`cat /var/run/statbox/$pid.pid`/cmdline || /etc/init.d/$pid $cmd)
		done
	) 9>/var/lock/statbox-logbroker-watchdog-running
fi

