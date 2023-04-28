#!/bin/bash
PROCESSE=`ps -ef | grep monitor_main.py | grep -v grep | awk '{print $2}' | xargs kill -9`
echo "stop finish"
app_path=$(dirname "$PWD")
cd $app_path
source activate ml_app
nohup python monitor_main.py >> nohup.out 2>&1 &
echo "starting finish"