#!/bin/bash

. /home/cats/.catsrc
cd /home/cats/cats/blackbox/robotest
${ROBOT} -v MASTER:${CATS_MASTER} -v DBCONNECT:${CATS_DBCONNECT_IDCX} dependent.robot
