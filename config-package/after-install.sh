

  ENV_TYPE=development
  if [ -f /etc/yandex/environment.type ]; then
      ENV_TYPE=`cat /etc/yandex/environment.type`
      echo "environment type is $ENV_TYPE"
  else
      echo 'yandex environment is not set, defaulting to development'
      ENV_TYPE=development
  fi

  for APP_NAME in 'qmon-popeye-slicer' 'qmon-popeye-pump' 'qmon-popeye-query' ; do
    QMON_CONF_DIR=/etc/yandex/$APP_NAME/$ENV_TYPE
    if [ ! -d $QMON_CONF_DIR ] ; then
      echo "no configuration for $APP_NAME $ENV_TYPE"
      exit 1
    fi
    ln -sfn $QMON_CONF_DIR /etc/yandex/$APP_NAME/conf
    chmod -R 655 $QMON_CONF_DIR 
  done
