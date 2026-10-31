// Package openai holds the subset of the OpenAI chat-completions wire format the gateway needs.
package openai

import "strings"

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequest struct {
	Model     string    `json:"model"`
	Messages  []Message `json:"messages"`
	Stream    bool      `json:"stream"`
	MaxTokens int       `json:"max_tokens,omitempty"`
	// MaxCompletionTokens is OpenAI's newer name for the same cap.
	MaxCompletionTokens int    `json:"max_completion_tokens,omitempty"`
	User                string `json:"user,omitempty"`
}

type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Chunk is one server-sent event of a streaming response.
type Chunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	Usage   *Usage        `json:"usage,omitempty"`
}

type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

// Completion is a non-streaming response.
type Completion struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

// CountTokens approximates tokens as whitespace-separated words. Both the fake backend and the
// load generator use it, so the counts agree with each other, not with a real tokenizer.
func CountTokens(s string) int { return len(strings.Fields(s)) }

// PromptTokens counts the tokens of every message in the request.
func PromptTokens(msgs []Message) int {
	n := 0
	for _, m := range msgs {
		n += CountTokens(m.Content)
	}
	return n
}

// PrefixKey is the part of a request that an inference server can reuse from its prefix cache:
// the system prompt if there is one, otherwise the first message.
func PrefixKey(msgs []Message) string {
	for _, m := range msgs {
		if m.Role == "system" {
			return m.Content
		}
	}
	if len(msgs) > 0 {
		return msgs[0].Content
	}
	return ""
}
