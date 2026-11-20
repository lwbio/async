// Package proto defines the protobuf codec. Importing this package will
// register the codec.
package proto

import (
	"errors"

	"google.golang.org/protobuf/proto"
)

// Name is the name registered for the proto compressor.
const Name = "proto"

// codec is a Codec implementation with protobuf. It is the default codec for Transport.
type Codec struct{}

func (Codec) Marshal(v interface{}) ([]byte, error) {
	m, err := getProtoMessage(v)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(m)
}

func (Codec) Unmarshal(data []byte, v interface{}) error {
	pm, err := getProtoMessage(v)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, pm)
}

func (Codec) Name() string {
	return Name
}

func getProtoMessage(v interface{}) (proto.Message, error) {
	if msg, ok := v.(proto.Message); ok {
		if msg == nil || !msg.ProtoReflect().IsValid() {
			return nil, errors.New("nil proto message")
		}
		return msg, nil
	}
	return nil, errors.New("not proto message")
}
