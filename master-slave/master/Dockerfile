FROM golang:1.14.1-alpine as builder

WORKDIR /app

COPY . .
 
RUN go mod vendor
RUN go build -o master .
 
FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the Pre-built binary file from the previous stage. Observe we also copied the .env file
COPY --from=builder /app/master .
 

ENTRYPOINT ["./master"]
