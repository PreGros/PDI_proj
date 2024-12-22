# !/bin/bash

# Check if input argument is in correct range
if [[ "$1" -lt 1 || "$1" -gt 6 ]]; then
  echo "Error: $1 is not between 1 and 6"
  exit 1  
fi

if [ -z "$2" ]; then
  waitAfterProducer=15
elif [[ "$2" =~ ^[0-9]+$ ]]; then
  waitAfterProducer=$2
else
  echo "Second argument must be a number!"
  exit 1
fi

if [ -z "$2" ]; then
  waitAfterSpark=45
elif [[ "$2" =~ ^[0-9]+$ ]]; then
  waitAfterSpark=$2
else
  echo "Third argument must be a number!"
  exit 1
fi

outputsPath="testOutputs/assigment$1"
outputPath="$outputsPath/output$1.txt"
croppedOutputPath="$outputsPath/croppedOutput$1.txt"
correctOutput="$outputsPath/correctOutput$1.txt"

if [[ -f "$outputPath" ]]; then
  rm "$outputPath"
fi

if [[ -f "$croppedOutputPath" ]]; then
  rm "$croppedOutputPath"
fi

echo "Running Docker to start spark-app and get output. This will take 60 seconds."

# Change env to set testing mode
echo -e "ARG1_VAL=$1\nMODE_VAL=local" > .env

sleep 1

docker-compose up python-api-producer -d

sleep $waitAfterProducer

docker-compose up spark-app -d

sleep $waitAfterSpark

docker-compose logs spark-app > ${outputPath}

docker-compose down

if [[ "$1" == "1" || "$1" == "2" ]]; then
  awk '/^pyspark-app  \| -------------------------------------------/,/^only showing top 20 rows/' ${outputPath} > ${croppedOutputPath}
elif [[ "$1" == "3" || "$1" == "4" ]]; then
  awk '/^pyspark-app  \| -------------------------------------------/,/^only showing top 5 rows/' ${outputPath} > ${croppedOutputPath}
elif [[ "$1" == "5" ]]; then
  awk '/^pyspark-app  \| -------------------------------------------/,/^+----------+-----+-------------------+-------------------+/' ${outputPath} > ${croppedOutputPath}
elif [[ "$1" == "6" ]]; then
  awk '/^pyspark-app  \| -------------------------------------------/,/^+----------+-------------------+-------------+/' ${outputPath} > ${croppedOutputPath}
fi

diff ${croppedOutputPath} ${correctOutput} > /dev/null

if [ $? -eq 0 ]; then
    echo -e "\e[1;32mAssigment $1 OK\e[0m"
else
    echo -e "\e[1;31mAssigment $1 FAIL\e[0m"
fi

echo -e "ARG1_VAL=$1\nMODE_VAL=api" > .env