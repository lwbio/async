package encoding

import (
	"fmt"
	"mime"
	"strings"
	"sync"

	"github.com/lwbio/async/encoding/json"
	"github.com/lwbio/async/encoding/proto"
)

func init() {
	RegisterCodec(json.Codec{})
	RegisterCodec(proto.Codec{})
}

// Codec defines the interface Transport uses to encode and decode messages.  Note
// that implementations of this interface must be thread safe; a Codec's
// methods can be called from concurrent goroutines.
type Codec interface {
	// Marshal returns the wire format of v.
	Marshal(v interface{}) ([]byte, error)
	// Unmarshal parses the wire format into v.
	Unmarshal(data []byte, v interface{}) error
	// Name returns the name of the Codec implementation. The returned string
	// will be used as part of content type in transmission.  The result must be
	// static; the result cannot change between calls.
	Name() string
}

var registeredCodecs = make(map[string]Codec)
var codecMu sync.RWMutex

// RegisterCodec registers the provided Codec for use with all Transport clients and
// servers.
func RegisterCodec(codec Codec) {
	if codec == nil {
		panic("cannot register a nil Codec")
	}
	if codec.Name() == "" {
		panic("cannot register Codec with empty string result for Name()")
	}
	contentSubtype := strings.ToLower(codec.Name())
	codecMu.Lock()
	defer codecMu.Unlock()
	registeredCodecs[contentSubtype] = codec
}

// GetCodec gets a registered Codec by content-subtype, or nil if no Codec is
// registered for the content-subtype.
//
// The content-subtype is expected to be lowercase.
func GetCodec(contentSubtype string) Codec {
	codecMu.RLock()
	defer codecMu.RUnlock()
	return registeredCodecs[strings.ToLower(contentSubtype)]
}

func ForContentType(contentType string) Codec {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.HasPrefix(mediaType, "application/") {
		return nil
	}
	return GetCodec(strings.TrimPrefix(mediaType, "application/"))
}

func Decode(contentType string, data []byte, value any) error {
	codec := ForContentType(contentType)
	if codec == nil {
		return fmt.Errorf("unsupported content type %q", contentType)
	}
	return codec.Unmarshal(data, value)
}
