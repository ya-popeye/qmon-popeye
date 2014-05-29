#!/bin/bash

(flock -n 9 || exit 0 && /usr/bin/logbroker-locked.sh --command /usr/share/statbox-logbroker/publish-lines.sh --lock "lock-publish-lines") 9>/var/lock/publish-lines.lock
