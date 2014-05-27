*/5 * * * * root (flock -n 9 || exit 0 && python /usr/share/statbox-logbroker/drop-logs.py) 9>/var/lock/drop-logs.lock
*/1 * * * * root bash /usr/share/statbox-logbroker/watchdog.sh
*/20 * * * * statbox (flock -n 9 || exit 0 && /usr/share/statbox-logbroker/kafka-cleaner.pl) 9>/var/lock/kafka-cleaner.lock
