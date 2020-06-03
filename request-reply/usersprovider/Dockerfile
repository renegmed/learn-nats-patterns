FROM golang:1.14.0-alpine

WORKDIR src/usersprovider

COPY . .

#RUN pwd
#RUN ls -als
 
RUN go mod vendor

RUN go build -o usersprovider .
 
ENTRYPOINT ["./usersprovider"]
