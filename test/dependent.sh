#!/bin/bash

# run this by hand on the slave.  first run will create test targets
# on master.  if you then edit passwd1 on master all tests should fail
# for different reasons (including second skipped).

. /home/cats/.catsrc
# dump env
#set
cd /home/cats/cats/blackbox/robotest/test
${ROBOT} -v MASTER:${CATS_MASTER} -v DBCONNECT:${CATS_DBCONNECT_IDCX} dependent.robot
