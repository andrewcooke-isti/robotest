#!/bin/bash

. /home/cats/.catsrc
# dump env
#set
cd /home/cats/cats/blackbox/robotest/test
${ROBOT} -v MASTER:${CATS_MASTER} -v DBCONNECT:${CATS_DBCONNECT_IDCX} dependent.robot
