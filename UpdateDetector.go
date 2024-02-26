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
	"time"

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

func GetPages(page int) ([]byte, error) {
	url := fmt.Sprintf("https://webapi.intelligencenode.com//breuninger?page=%d&app_key=213c06c51ed7df6fDEB", page)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("end of pages")
	}

	return ioutil.ReadAll(resp.Body)
}

func main() {

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(dburl))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO())
	collection := client.Database(dbname).Collection(collectionname)

	var result bson.M
	var currentTime = time.Now()
	var update bson.M
	page := 1
	for {
		body, err := GetPages(page)
		if err != nil {
			log.Fatalf("got all pages")
		}
		if len(body) == 0 {
			break
		}

		var skus Skus
		if err := json.Unmarshal(body, &skus); err != nil {
			break
		}

		for _, product := range skus.Products {
			var ids []string
			for _, comp := range product.Competitors {
				ids = append(ids, comp.ID)
			}
			hashedidstring := CalcHash(ids)

			filter := bson.M{"_id": product.ID}
			if err = collection.FindOne(context.TODO(), filter).Decode(&result); err == nil {
				//ako existva pravi slednoto
				filter = bson.M{"hashedcompetitorsstring": hashedidstring}

				if err = collection.FindOne(context.TODO(), filter).Decode(&result); err == nil {
					filter = bson.M{"_id": product.ID}
					update = bson.M{
						"$set": bson.M{
							"oid":                     product.ID,
							"hashedcompetitorsstring": hashedidstring,
							"last_checked":            currentTime,
						},
					}
					_, err = collection.UpdateByID(context.TODO(), filter, update)
					if err != nil {
						log.Fatalf("failed to insert new time into mongodb %v", err)
					}
					fmt.Printf("time checked updated for product %s", product.ID)

				} else if err = collection.FindOne(context.TODO(), filter).Decode(&result); err != nil {
					filter = bson.M{"_id": product.ID}
					update = bson.M{
						"set": bson.M{
							"oid":                     product.ID,
							"hashedcompetitorsstring": hashedidstring,
							"last_checked":            currentTime,
							"last_update":             currentTime,
						},
					}
					fmt.Printf("updated time for checked and last update  for product %s", product.ID)
				}
			} else {
				_, err = collection.InsertOne(context.TODO(), bson.M{
					"oid":                     product.ID,
					"hashedcompetitorsstring": hashedidstring,
					"last_checked":            currentTime,
					"last_update":             currentTime,
				})
				if err != nil {
					log.Fatalf("failed to insert into mongodb %v", err)
				}
				fmt.Printf("data for product %s inserver \n", product.ID)
			}

		}
		page++
	}

}

func CalcHash(ids []string) string {
	joined := strings.Join(ids, "")
	hash := sha256.Sum256([]byte(joined))
	return hex.EncodeToString(hash[:])
}
