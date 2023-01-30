package planetscale

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"

	"github.com/fastly/compute-sdk-go/fsthttp"
	"github.com/valyala/fastjson"
)

const (
	executorEndpoint = "/psdb.v1alpha1.Database/Execute"
	sessionEndpoint  = "/psdb.v1alpha1.Database/CreateSession"
	executorMethod   = "POST"
)

var unknownError = fmt.Errorf("unknown error")

type PsDriver struct{}

type PsConn struct {
	username string
	password string
	host     string
	backend  string
	session  []byte
}

func (d PsDriver) Open(dsn string) (driver.Conn, error) {
	m, err := url.ParseQuery(dsn)
	if err != nil {
		return nil, fmt.Errorf("error parsing dsn: %w", err)
	}

	var c PsConn
	c.username = m.Get("username")
	c.password = m.Get("password")
	c.host = m.Get("host")
	c.backend = m.Get("backend")

	return &c, nil
}

func (c *PsConn) Close() error {
	return nil
}

func (c *PsConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare method not implemented")
}

func (c *PsConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("Begin method not implemented")
}

func (c *PsConn) Rollback() (driver.Stmt, error) {
	return nil, fmt.Errorf("Rollback method not implemented")
}

func (c *PsConn) buildRequest(endpoint string, body []byte) (*fsthttp.Request, error) {
	u := fmt.Sprintf("https://%s:%s@%s%s", c.username, c.password, c.host, endpoint)

	req, err := fsthttp.NewRequest(executorMethod, u, nil)
	if err != nil {
		return nil, err
	}

	req.Body = io.NopCloser(bytes.NewReader(body))

	auth := base64.StdEncoding.EncodeToString([]byte(c.username + ":" + c.password))
	req.Header.Add("Host", c.host)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "database-go")
	req.Header.Add("Authorization", "Basic "+auth)

	return req, nil
}

func (c *PsConn) sendRequest(ctx context.Context, req *fsthttp.Request) ([]byte, error) {
	resp, err := req.Send(ctx, c.backend)
	if err != nil {
		return nil, err
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("planetscale API error reading response body: %s", err)
	}

	if resp.StatusCode != fsthttp.StatusOK {

		return nil, fmt.Errorf("planetscale API error: %d\n%s", resp.StatusCode, respBody)
	}

	return respBody, nil
}

func (c *PsConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, args)
}

func (c *PsConn) refreshSession(ctx context.Context) error {
	req, err := c.buildRequest(sessionEndpoint, []byte("{}"))
	if err != nil {
		return err
	}

	respBody, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}

	var p fastjson.Parser
	v, err := p.ParseBytes(respBody)

	c.session = []byte{}
	c.session = v.GetObject("session").MarshalTo(c.session)
	return nil
}

func (c *PsConn) QueryContext(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	if c.session == nil {
		if err := c.refreshSession(ctx); err != nil {
			return nil, err
		}
	}

	q, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	body := []byte(`{"query":`)
	body = append(body, q[:]...)
	body = append(body, []byte(`,"session":`)...)
	body = append(body, c.session[:]...)
	body = append(body, []byte(`}`)...)

	req, err := c.buildRequest(executorEndpoint, body)
	if err != nil {
		return nil, err
	}

	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	var p fastjson.Parser
	v, err := p.ParseBytes(resp)
	if err != nil {
		return nil, err
	}

	if session := v.GetObject("session"); session != nil {
		c.session = []byte{}
		c.session = session.MarshalTo(c.session)
	}

	if jsonErr := v.GetObject("error"); jsonErr != nil {
		if msg := jsonErr.Get("message"); msg != nil {
			return nil, fmt.Errorf("%s", msg.GetStringBytes())
		}
		return nil, unknownError
	}

	result := v.GetObject("result")
	if result == nil {
		return nil, fmt.Errorf("no result")
	}

	log.Printf("%v", result.String())
	return nil, nil
}

func init() {
	sql.Register("planetscale", &PsDriver{})
}
