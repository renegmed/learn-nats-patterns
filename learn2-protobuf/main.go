package main

import (
	fmt "fmt"

	"github.com/golang/protobuf/proto"
	"github.com/renegmed/learn-proto/pb"
)

func main() {
	var text = []byte("hello")
	message := &pb.Message{
		Text: text,
	}

	data, err := proto.Marshal(message)
	if err != nil {
		panic(err)
	}

	fmt.Println(data) // [10 5 104 101 108 108 111]

	newMessage := &pb.Message{}
	err = proto.Unmarshal(data, newMessage)
	if err != nil {
		panic(err)
	}

	fmt.Println(newMessage.GetText()) // [104 101 108 108 111]
}
