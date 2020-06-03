up:
	docker-compose up --build -d 
.PHONY: up 


cluster-info:
	curl http://127.0.0.1:8222/varz
	curl http://127.0.0.1:8222/routez
.PHONY: cluster-info

tail-dispatcher:
	docker logs dispatcher -f 
tail-receiver:
	docker logs receiver -f 
.PHONY: tail-dispatcher tail-receiver 
