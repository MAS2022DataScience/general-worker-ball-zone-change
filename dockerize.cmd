REM docker build -t general-worker-ball-zone-change:0.0.1 -t general-worker-ball-zone-change:latest .
docker rmi general-worker-ball-zone-change:latest -f
docker build -t general-worker-ball-zone-change:latest .

REM if it runs on localhost
REM docker run -it -p 8080:8080 general-worker-ball-zone-change:latest
REM if it runs on 192.168.1.100
REM docker run --env DATAPLATFORM_IP=192.168.1.100 -it -p 8080:8080 general-worker-ball-zone-change:latest