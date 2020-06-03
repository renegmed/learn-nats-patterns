FROM golang:1.14.0-alpine

WORKDIR src/worker

COPY . .
 
RUN go mod vendor

RUN go build -o worker .

RUN pwd
RUN ls -als
 
ENTRYPOINT ["./worker"]