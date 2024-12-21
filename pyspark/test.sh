# !/bin/bash

# Check if input argument is in correct range
if [[ "$1" -lt 1 || "$1" -gt 6 ]]; then
  echo "Error: $1 is not between 1 and 6"
  exit 1  # Exit with an error code
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

echo "Running Docker to start spark-app and get output. This will take 60 seconds. (60+ with container build & pull)"

docker-compose build

docker-compose pull

# Change env to set testing mode
echo -e "ARG1_VAL=$1\nMODE_VAL=local" > .env

sleep 1

# Run the Docker command and save output to a file
docker-compose up python-api-producer -d

sleep 15

docker-compose up spark-app -d

sleep 30

docker-compose logs spark-app > ${outputPath}

# Bring down the Docker containers
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





# Compare two files using diff
diff ${croppedOutputPath} ${correctOutput} > /dev/null

# Check the result of diff
if [ $? -eq 0 ]; then
    # No difference, print "Success" in green and bold
    echo -e "\e[1;32mAssigment $1 OK\e[0m"
else
    # There is a difference, print "Failure" in red and bold
    echo -e "\e[1;31mAssigment $1 FAIL\e[0m"
fi



echo -e "ARG1_VAL=$1\nMODE_VAL=api" > .env