package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/fastly/compute-sdk-go/edgedict"
	"github.com/fastly/compute-sdk-go/fsthttp"
	_ "github.com/leedo/planetscale"
)

func main() {
	fsthttp.ServeFunc(func(ctx context.Context, w fsthttp.ResponseWriter, r *fsthttp.Request) {
		const query = `SELECT * FROM users`
		conf, err := edgedict.Open("planetscale_config")
		if err != nil {
			w.WriteHeader(fsthttp.StatusBadGateway)
			fmt.Fprintln(w, err)
			return
		}

		username, err := conf.Get("username")
		if err != nil && username != "" {
			w.WriteHeader(fsthttp.StatusBadGateway)
			fmt.Fprintln(w, err)
			return
		}

		password, err := conf.Get("password")
		if err != nil && password != "" {
			w.WriteHeader(fsthttp.StatusBadGateway)
			fmt.Fprintln(w, err)
			return
		}

		host, err := conf.Get("host")
		if err != nil && host != "" {
			w.WriteHeader(fsthttp.StatusBadGateway)
			fmt.Fprintln(w, err)
			return
		}

		backend, err := conf.Get("backend")
		if err != nil && backend != "" {
			w.WriteHeader(fsthttp.StatusBadGateway)
			fmt.Fprintln(w, err)
			return
		}

		dsn := fmt.Sprintf(
			"username=%s&password=%s&host=%s&backend=%s",
			username, password, host, backend,
		)

		db, err := sql.Open("planetscale", dsn)
		if err != nil {
			w.WriteHeader(fsthttp.StatusBadGateway)
			fmt.Fprintln(w, err)
			return
		}

		_, err = db.QueryContext(ctx, query)
		if err != nil {
			w.WriteHeader(fsthttp.StatusBadGateway)
			fmt.Fprintln(w, err)
			return
		}
	})
}
