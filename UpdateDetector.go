package main

const (
	dbname         = "updateadddb"
	collectionname = "items"
	dburl          = "mongodb://localhost:27017"
)

type Mongotemplate struct {
	id          string `json: "id"`
	competitors string `json: "competitors"`
	lastchecked string `json: "last_checked"`
	lastchange  string `json: "last_change"`
}

func main() {

}
