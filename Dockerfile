FROM golang:1.16

ADD .  /go/src/github.com/l2trace/gorm-vertica/
WORKDIR /go/src/github.com/l2trace/gorm-vertica/

RUN go build -v vertica/vertica.go