#!/bin/bash
clear

echo "=============== Hey $USER ==============="
echo

echo " - > Your bash environment is initialize"
echo

conda init

echo
echo "* * * Enter your desired conda environment to activatation : "
read condaEnv
echo

conda activate $condaEnv

export PYTHONPATH=${PWD}

echo " - > Your \"PYTHONPATH\" variable is  $PYTHONPATH"
echo

echo " - > current time : `date `"
echo

echo "=============== Done ! ==============="
echo
