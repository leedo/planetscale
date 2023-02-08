package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/fastly/compute-sdk-go/edgedict"
	"github.com/fastly/compute-sdk-go/fsthttp"
	_ "github.com/leedo/planetscale"
)

func internalError(w fsthttp.ResponseWriter, err error) {
	w.WriteHeader(fsthttp.StatusBadGateway)
	fmt.Fprintln(w, err)
}

func readDSN() (string, error) {
	conf, err := edgedict.Open("planetscale_config")
	if err != nil {
		return "", err
	}

	dsn, err := conf.Get("dsn")
	if err != nil && dsn != "" {
		return "", err
	}

	return dsn, nil
}

func main() {
	fsthttp.ServeFunc(func(ctx context.Context, w fsthttp.ResponseWriter, r *fsthttp.Request) {
		const query = `SELECT * FROM user`
		dsn, err := readDSN()
		if err != nil {
			internalError(w, err)
			return
		}

		db, err := sql.Open("planetscale", dsn)
		if err != nil {
			internalError(w, err)
			return
		}

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			internalError(w, err)
			return
		}

		for rows.Next() {
			var (
				id   int64
				name string
			)

			if err := rows.Scan(&id, &name); err != nil {
				internalError(w, err)
				return
			}

			fmt.Fprintf(w, "%d %s", id, name)
		}

		if err := rows.Err(); err != nil {
			internalError(w, err)
			return
		}
	})
}
