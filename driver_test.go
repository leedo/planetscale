package planetscale

import (
	"database/sql"
	"testing"
)

func TestDriverOpen(t *testing.T) {
	_, err := sql.Open("planetscale", "username=fart&password=balls&host=guh&backend=guh")
	if err != nil {
		t.Fatal(err)
	}
}
