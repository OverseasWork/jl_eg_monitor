#!/bin/bash
app_path=$(dirname "$PWD")
cd $app_path
source activate ml_app
nohup python monitor_main.py >> nohup.out 2>&1 &
echo "starting finish"