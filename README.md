use the following command to build the image:

    docker build -t task2 .
    
To run, publish luigi's default web port ( 8082 )

    docker run -it -p 8082:8082 -p 8088:8088 -p 8042:8042 -p 4040:4040 -p 7077:7077 -h sandbox task2 bash
    
And visit:
 - http://LUIGI_HOST:8082/
 - http://LUIGI_HOST:8082/static/visualiser/index.html#
 - http://LUIGI_HOST:8082/history
