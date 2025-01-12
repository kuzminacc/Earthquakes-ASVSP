docker exec -it namenode bash -c "chmod +x ./data/load.sh && ./data/load.sh"
docker exec -it spark-master bash -c "chmod +x ./batch/startspark.sh && ./batch/startspark.sh"