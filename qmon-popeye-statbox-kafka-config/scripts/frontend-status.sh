#!/bin/bash

ans=`curl -m 60 -v http://localhost/ping 2>/tmp/monrun-err`

if [ $? == 0 ]
then
	code=`cat /tmp/monrun-err | grep '< HTTP/1.1'`
	echo $code | grep 200 2>&1 > /dev/null
	if [ $? == 0 ]
	then
		echo $ans
	else
		echo "2; $code "
	fi
else
	echo "2; ping failed (timeout)"
fi

rm /tmp/monrun-err

