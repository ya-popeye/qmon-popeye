fpm -s dir -t deb \
  -n qmon-popeye-config \
  -v 0.1.0-yandex1 \
  --directories /etc/yandex/qmon-popeye-slicer \
  --directories /etc/yandex/qmon-popeye-pump \
  --directories /etc/yandex/qmon-popeye-query \
  --after-install after-install.sh \
  yandex/qmon-popeye-pump=/etc/yandex \
  yandex/qmon-popeye-query=/etc/yandex \
  yandex/qmon-popeye-slicer=/etc/yandex 

