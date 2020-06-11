up:
	docker-compose up --build -d 
.PHONY: up 
 

test-kvstore:
	curl -X POST http://localhost:3000/set?key=1&value=desktop  
	curl http://localhost:3000/get?key=1  
	curl http://localhost:3000/list 
	curl -X DELETE http://localhost:3000/remove?key=1  
	curl http://localhost:3000/list  
.PHONY: tail-kvstore test-kvstore


tail-frontend:
	docker logs frontend -f
.PHONY: tail-frontend

tail-kvstore:
	docker logs kvstore -f
.PHONY: tail-kvstore

tail-db:
	docker logs database -f
.PHONY: tail-db

tail-storage:
	docker logs storage -f 
.PHONY: tail-storage 

tail-master:
	docker logs master -f 
.PHONY: tail-master 

tail-worker:
	docker logs worker -f
.PHONY: tail-worker

