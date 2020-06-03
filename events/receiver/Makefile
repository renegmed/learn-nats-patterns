 init-project:
	go mod init github.com/renegmed/nats-patterns-events/receiver
.PHONY: init-project

pb:
	protoc -I pb/ pb/*.proto --go_out=plugins=grpc:pb
.PHONY: pb