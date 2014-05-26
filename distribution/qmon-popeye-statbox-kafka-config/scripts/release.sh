#!/bin/bash

git pull && dch -i && git commit -a && git tag tag-statbox-logbroker-`dpkg-parsechangelog | grep Version | cut -f 2 -d ' '` && git push && git push --tags && debuild -b
