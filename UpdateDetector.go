package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	dbname         = "updateadddb"
	collectionname = "items"
	dburl          = "mongodb://localhost:27017"
)

type Skus struct {
	Products []Product `json:"skus"`
}
type Product struct {
	ID          string       `json:"oid"`
	Competitors []Competitor `json:"competitors"`
}

type Competitor struct {
	ID string `json:"oid"`
}

func Hash(ids []string) string {
	joined := strings.Join(ids, "")
	hash := sha256.Sum256([]byte(joined))
	return hex.EncodeToString(hash[:])
}

func main() {

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(dburl))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO())
	collection := client.Database(dbname).Collection(collectionname)

	res, err := http.Get("https://webapi.intelligencenode.com//breuninger?page=1&app_key=213c06c51ed7df6fDEB")
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	var skus Skus
	if err := json.Unmarshal(body, &skus); err != nil {
		log.Fatal(err)
	}

	for _, product := range skus.Products {
		var ids []string
		for _, comp := range product.Competitors {
			ids = append(ids, comp.ID)
		}
		hashedidstring := Hash(ids)

		_, err = collection.InsertOne(context.TODO(), bson.M{
			"oid":                     product.ID,
			"hashedcompetitorsstring": hashedidstring,
		})
		if err != nil {
			log.Fatalf("failed to insert into mongodb %v", err)
		}
		fmt.Printf("data for product %s inserver \n", product.ID)
	}
}
