FROM golang:1.14.0-alpine

WORKDIR src/fileserver

COPY . .
 
RUN go mod vendor

RUN go build -o fileserver .

RUN pwd
RUN ls -als
 
ENTRYPOINT ["./fileserver"]