#!/bin/bash
app_path=$(dirname "$PWD")
cd $app_path
PROCESSE=`ps -ef | grep monitor_main.py | grep -v grep | awk '{print $2}' | xargs kill -9`
echo "stop finish"
