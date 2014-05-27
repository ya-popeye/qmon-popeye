#!/bin/sh

set +x
set +e

home=$(dirname $0)

group=$(cat ../build.sbt | grep organization | awk '{ print $5 }' | tr -d '"' )
version=$(cat ../build.sbt | grep version | awk '{ print $5 }' | tr -d '"' )

sed -i.bak -e "s/@GROUP@/$group/g" -e "s/@VERSION@/$version/g" debian/rules
$home/debian/make-app.sh slicer
$home/debian/make-app.sh pump
$home/debian/make-app.sh query
(cd $home && rm -f debian/changelog && \
/usr/bin/dch --create \
   --package qmon-popeye \
   --newversion 0.9.0.1 --distribution $(lsb_release -sc) "Autopackage"
)
