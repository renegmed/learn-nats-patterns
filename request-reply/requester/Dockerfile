FROM golang:1.14.0-alpine

WORKDIR src/requester

COPY . .
 
RUN go mod vendor
RUN go build -o requester .

# RUN pwd
# RUN ls -als

ENTRYPOINT ["./requester"]
