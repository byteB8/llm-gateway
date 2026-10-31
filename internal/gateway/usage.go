package gateway

import (
	"bytes"
	"encoding/json"

	"github.com/byteB8/llm-gateway/internal/openai"
)

// usageTap watches a response as it is copied to the client and keeps the token usage the backend
// reports: the "usage" object of a streaming chunk, or of a whole JSON response.
type usageTap struct {
	stream  bool
	partial []byte // a streamed line not yet terminated by '\n'
	body    []byte // a non-streaming response, kept up to maxBody
	usage   *openai.Usage
}

const maxBody = 1 << 20

func (u *usageTap) Write(p []byte) {
	if !u.stream {
		if len(u.body)+len(p) <= maxBody {
			u.body = append(u.body, p...)
		}
		return
	}
	u.partial = append(u.partial, p...)
	for {
		i := bytes.IndexByte(u.partial, '\n')
		if i < 0 {
			break
		}
		u.line(u.partial[:i])
		u.partial = u.partial[i+1:]
	}
	if len(u.partial) > maxBody { // a runaway line; give up on it rather than grow forever
		u.partial = nil
	}
}

func (u *usageTap) line(l []byte) {
	data, ok := bytes.CutPrefix(bytes.TrimRight(l, "\r"), []byte("data: "))
	if !ok || !bytes.Contains(data, []byte(`"usage"`)) {
		return
	}
	var c struct {
		Usage *openai.Usage `json:"usage"`
	}
	if json.Unmarshal(data, &c) == nil && c.Usage != nil {
		u.usage = c.Usage
	}
}

// Usage returns the reported usage, or nil if the backend reported none.
func (u *usageTap) Usage() *openai.Usage {
	if !u.stream && u.usage == nil && len(u.body) > 0 {
		var c struct {
			Usage *openai.Usage `json:"usage"`
		}
		if json.Unmarshal(u.body, &c) == nil {
			u.usage = c.Usage
		}
	}
	return u.usage
}
