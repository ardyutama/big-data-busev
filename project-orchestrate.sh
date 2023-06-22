#! /bin/bash

cd bus-spark-processor && bash build_dependencies.sh && cd ..
sudo docker build -t stream_job -f bus-spark-processor/Dockerfile.streamjob ./bus-spark-processor
sudo docker run --name stream_job -e ENABLE_INIT_DAEMON=false -d --network big-data-2_busev stream_job
sudo docker container logs -f --tail 10 stream_job