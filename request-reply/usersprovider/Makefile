 init-project:
	go mod init github.com/renegmed/nats-patterns-reqreply/usersprovider
.PHONY: init-project

pb:
	protoc -I pb/ pb/*.proto --go_out=plugins=grpc:pb
.PHONY: pb