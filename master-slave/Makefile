up:
	docker-compose up --build -d 
.PHONY: up 


cluster-info:
	curl http://127.0.0.1:8222/varz
	curl http://127.0.0.1:8222/routez
.PHONY: cluster-info

tail-master:
	docker logs master -f
tail-fileserver:
	docker logs fileserver -f 
tail-worker1:
	docker logs worker1 -f 
.PHONY: tail-master tail-fileserver tail-worker1 

stop:
	docker stop worker1
	docker stop master
start:
	docker start worker1
	docker start master
restart:
	docker restart worker1 
	docker restart master
.PHONY: stop start, restart 
