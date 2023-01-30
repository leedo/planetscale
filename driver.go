package planetscale

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/fastly/compute-sdk-go/fsthttp"
)

const (
	executorEndpoint = "/psdb.v1alpha1.Database/Execute"
	executorMethod   = "POST"
)

type PsDriver struct{}

type PsConn struct {
	username string
	password string
	host     string
	backend  string
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

func (c *PsConn) buildApiReq(body string) (*fsthttp.Request, error) {
	b := strings.NewReader(body)
	u := fmt.Sprintf("https://%s:%s@%s%s", c.username, c.password, c.host, executorEndpoint)
	return fsthttp.NewRequest(executorMethod, u, b)
}

func (c *PsConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, args)
}

func (c *PsConn) QueryContext(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	req, err := c.buildApiReq(query)
	if err != nil {
		return nil, err
	}

	resp, err := req.Send(ctx, c.backend)
	if err != nil {
		return nil, err
	}

	log.Printf("%v+", resp)

	return nil, nil
}

func init() {
	sql.Register("planetscale", &PsDriver{})
}
