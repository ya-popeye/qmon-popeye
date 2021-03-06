#!/bin/sh

set +x
set +e

home=$(dirname $0)

group=$(cat $home/../build.sbt | grep organization | awk '{ print $5 }' | tr -d '"' )
version=$(cat $home/../build.sbt | grep version | awk '{ print $5 }' | tr -d '"' )

sed -i.bak -e "s/@GROUP@/$group/g" -e "s/@VERSION@/$version/g" $home/debian/rules
$home/debian/make-app.sh slicer
$home/debian/make-app.sh pump
$home/debian/make-app.sh query
(cd $home && rm -f $home/debian/changelog && \
/usr/bin/dch --create \
   --package qmon-popeye \
   --newversion $version --distribution $(lsb_release -sc) "Autopackage"
)
