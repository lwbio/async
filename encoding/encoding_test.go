package encoding

import (
	"google.golang.org/protobuf/types/known/wrapperspb"
	"testing"
)

func TestCodecsRejectBadInputsWithoutPanic(t *testing.T) {
	if _, err := GetCodec("proto").Marshal("not a proto"); err == nil {
		t.Fatal("accepted non-protobuf value")
	}
	var value *wrapperspb.StringValue
	if _, err := GetCodec("proto").Marshal(value); err == nil {
		t.Fatal("accepted nil protobuf")
	}
	if err := Decode("application/json", []byte(`{}`), nil); err == nil {
		t.Fatal("accepted nil destination")
	}
	if err := Decode("application/unknown", nil, new(string)); err == nil {
		t.Fatal("accepted unknown codec")
	}
}

func TestProtobufAndJSON(t *testing.T) {
	for _, name := range []string{"proto", "json"} {
		in := wrapperspb.String("hello")
		data, err := GetCodec(name).Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		out := new(wrapperspb.StringValue)
		if err = Decode("application/"+name, data, out); err != nil {
			t.Fatal(err)
		}
		if out.Value != "hello" {
			t.Fatal(out)
		}
	}
}
