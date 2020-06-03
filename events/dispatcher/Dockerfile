FROM golang:1.14.0-alpine

WORKDIR src/dispatcher

COPY . .
 
RUN go mod vendor
RUN go build -o dispatcher .

# RUN pwd
# RUN ls -als

ENTRYPOINT ["./dispatcher"]
