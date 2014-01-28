#!/bin/sh

LAST_DIR=`ls -dt trending-* | head -1`

if [ -e "./$LAST_DIR/part-00000" ]; then
	cat ./$LAST_DIR/part-00000 | sed 's/[\"}]/ /g' | awk '{ print $6 " " $4 }' | sort -rn | head -20
else
	echo "no data avaialable"
fi