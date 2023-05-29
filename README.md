# big-data-infrastructure

## How to run Big Data Infrastructure

1. Prepare the pipenv for python to install python environment and add it to requirements.txt
2. run sudo docker compose up -d to run the docker container
3. run mkdir hadoop/datanode and mkdir hadoop/namenode to build the checkpoint and data to store in hadoop
4. run sudo make makeHadoop to make hadoop file inside the container (*for the first time and if you want to format the namenode, just delete the hadoop folder that you make in step 3*)
5. run sudo make makeCassandra to prepare table Cassandra for the database (Note: if you wanted to reset the database, just run the command because its gonna drop all the table when there's existed)
6. In the path of ~/.big-data-infrastructure , run bash project-orchestrate.sh to run the pipeline 

To stop the pipeline, just run sudo docker container stop stream_job