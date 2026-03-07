package ndn

import (
	"encoding/json"
	"fmt"
	"time"

	enc "github.com/named-data/ndnd/std/encoding"
	ndnlib "github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/types/optional"
	"github.com/named-data/ndnd/std/utils"
)

// Client is an NDN-based KV store client (consumer).
// It sends Interests to a KV server and parses Data responses.
type Client struct {
	engine   ndnlib.Engine
	serverID string
	timeout  time.Duration
}

// NewClient creates a new NDN KV client targeting the given server.
func NewClient(engine ndnlib.Engine, serverID string) *Client {
	return &Client{
		engine:   engine,
		serverID: serverID,
		timeout:  4 * time.Second,
	}
}

// SetTimeout sets the Interest lifetime (how long to wait for a response).
func (c *Client) SetTimeout(d time.Duration) {
	c.timeout = d
}

// Get retrieves a value by key.
func (c *Client) Get(key string) (*Response, error) {
	name, err := GetName(c.serverID, key)
	if err != nil {
		return nil, fmt.Errorf("failed to build GET name: %w", err)
	}
	return c.express(name, nil)
}

// Put stores a key-value pair.
func (c *Client) Put(key, value string) (*Response, error) {
	name, err := PutName(c.serverID, key)
	if err != nil {
		return nil, fmt.Errorf("failed to build PUT name: %w", err)
	}
	return c.express(name, enc.Wire{[]byte(value)})
}

// Delete removes a key.
func (c *Client) Delete(key string) (*Response, error) {
	name, err := DeleteName(c.serverID, key)
	if err != nil {
		return nil, fmt.Errorf("failed to build DELETE name: %w", err)
	}
	return c.express(name, nil)
}

// Keyrange retrieves the keyrange metadata.
func (c *Client) Keyrange() (*Response, error) {
	name, err := KeyrangeName(c.serverID)
	if err != nil {
		return nil, fmt.Errorf("failed to build KEYRANGE name: %w", err)
	}
	return c.express(name, nil)
}

// KeyrangeRead retrieves the readable keyrange metadata.
func (c *Client) KeyrangeRead() (*Response, error) {
	name, err := KeyrangeReadName(c.serverID)
	if err != nil {
		return nil, fmt.Errorf("failed to build KEYRANGE_READ name: %w", err)
	}
	return c.express(name, nil)
}

// express sends an Interest and waits for a Data response.
func (c *Client) express(name enc.Name, appParam enc.Wire) (*Response, error) {
	intCfg := &ndnlib.InterestConfig{
		MustBeFresh: true,
		Lifetime:    optional.Some(c.timeout),
		Nonce:       utils.ConvertNonce(c.engine.Timer().Nonce()),
	}

	interest, err := c.engine.Spec().MakeInterest(name, intCfg, appParam, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Interest: %w", err)
	}

	type result struct {
		resp *Response
		err  error
	}

	ch := make(chan result, 1)

	err = c.engine.Express(interest, func(args ndnlib.ExpressCallbackArgs) {
		switch args.Result {
		case ndnlib.InterestResultData:
			content := args.Data.Content().Join()
			var resp Response
			if err := json.Unmarshal(content, &resp); err != nil {
				ch <- result{err: fmt.Errorf("failed to parse response: %w", err)}
				return
			}
			ch <- result{resp: &resp}

		case ndnlib.InterestResultNack:
			ch <- result{err: fmt.Errorf("interest NACKed (reason=%d)", args.NackReason)}

		case ndnlib.InterestResultTimeout:
			ch <- result{err: fmt.Errorf("interest timed out for %s", name)}

		case ndnlib.InterestCancelled:
			ch <- result{err: fmt.Errorf("interest cancelled for %s", name)}

		default:
			ch <- result{err: fmt.Errorf("unexpected result %d for %s", args.Result, name)}
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to express Interest: %w", err)
	}

	r := <-ch
	return r.resp, r.err
}
