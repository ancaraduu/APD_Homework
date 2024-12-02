#!/bin/bash

sudo docker-compose up -d --build

if [[ $? != 0 ]]
then
	echo "Nu s-a putut crea/porni containerul Docker"
	sudo docker-compose down
	cd ..
	exit
fi

sudo docker exec -w /apd/checker -it apd_container /apd/checker/checker.sh
sudo docker-compose down
cd ..