#! /bin/bash
#printf "TIME\t\tMEMORY\t\tDisk\t\tCPU\n"
end=$((SECONDS+3600))
while [ $SECONDS -lt $end ]; do
now=$(date '+%d-%m-%Y %H:%M:%S')
MEMORY=$(ps -e -o %mem  |awk '{s+=$1} END {print s"\t\t"}')
DISK=$(df -h |awk '$NF=="/"{printf "%s\t\t", $5}')
CPU=$(ps -e -o %cpu | awk '{s+=$1} END {print s}')
echo -e "$now\t\t$MEMORY$DISK$CPU"
sleep 5
done