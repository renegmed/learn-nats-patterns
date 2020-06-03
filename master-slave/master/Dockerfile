FROM golang:1.14.0-alpine

WORKDIR src/master
COPY . .
 
RUN go mod vendor

RUN go build -o master .

RUN pwd
RUN ls -als
 
ENTRYPOINT ["./master"]