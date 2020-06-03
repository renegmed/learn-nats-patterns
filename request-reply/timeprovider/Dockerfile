FROM golang:1.14.0-alpine

WORKDIR src/timeprovider
COPY . .
 
RUN go mod vendor

RUN go build -o timeprovider .
 
ENTRYPOINT ["./timeprovider"]
