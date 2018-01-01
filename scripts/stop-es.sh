docker rm $(docker stop $(docker ps -a -q --filter ancestor=docker.elastic.co/elasticsearch/elasticsearch:6.0.1 --format="{{.ID}}"))
