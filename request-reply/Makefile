up:
	docker-compose up --build -d 
.PHONY: up 


cluster-info:
	curl http://127.0.0.1:8222/varz
	curl http://127.0.0.1:8222/routez
.PHONY: cluster-info

get:
	curl http://127.0.0.1:3100/2 -v
.PHONY: get

tail-req:
	docker logs requester -f
tail-user:
	docker logs provider1 -f
tail-time:
	docker logs provider2 -f
.PHONY: tail-req tail-prov1 tail-prov2

req1:
	curl http://localhost:3100/1

req2:
	curl http://localhost:3100/2

req3:
	curl http://localhost:3100/3