#!/bin/bash
if [ "$1" != "" ] || [$# -gt 1]; then
	echo "Creating layer compatible with python version $1" 
	#docker run -v "$PWD":/var/task "lambci/lambda:build-python3.8" /bin/sh -c "pip install -r requirements.txt -t libs; exit"
	#pip install -r requirements.txt -t python
	docker run -v "$PWD":/var/task "lambci/lambda:build-python3.8" /bin/sh -c "pip install -r requirements.txt -t python/lib/python3.8/site-packages/; exit"
	#zip -r layer.zip python
	#docker run -v "$PWD":/var/task "lambci/lambda:build-python$1" /bin/sh -c "pip install -r requirements.txt -t libs; exit"
	#pip install -r requirements.txt -t python
	zip -r layer.zip python
	rm -r python
	echo "Done creating layer!"
	#ls -lah layer.zip

else
	echo "Enter python version as argument - ./createlayer.sh 3.6"
fi
