FROM ubuntu

# Install the C lib for kafka
RUN apt-get update
RUN apt-get install -y --no-install-recommends apt-utils wget gnupg software-properties-common
RUN apt-get install -y apt-transport-https ca-certificates
RUN wget -qO - https://packages.confluent.io/deb/5.3/archive.key | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.3 stable main"
RUN apt-get update
RUN apt-get install -y librdkafka-dev

# Install Go
RUN add-apt-repository ppa:longsleep/golang-backports
RUN apt-get update
RUN apt-get install -y golang-1.12-go

# build the library
WORKDIR /go/src/github.com/resulguldibi/consumer-dispatcher
ADD . /go/src/github.com/resulguldibi/consumer-dispatcher

RUN GOPATH=/go GOOS=linux /usr/lib/go-1.12/bin/go build -a -o main .

EXPOSE 8000

ENTRYPOINT ["./main"]