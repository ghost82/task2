docker build -t task2 .
docker run -it -p 8088:8088 -p 8042:8042 -p 4040:4040 -p 8082:8082 -p 7077:7077 --volume $(pwd):/data2 -h sandbox task2 bash
