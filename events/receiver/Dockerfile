FROM golang:1.14.0-alpine

WORKDIR src/receiver

COPY . .
 
RUN go mod vendor
RUN go build -o receiver .

# RUN pwd
# RUN ls -als

ENTRYPOINT ["./receiver"]
