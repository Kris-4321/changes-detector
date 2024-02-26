package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	dbname         = "updateadddb"
	collectionname = "items"
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

var ProductFromDb struct {
	ID           string    `bson:"_id"`
	HashedString string    `bson:"hashedcompetitorsstring"`
	Last_Checked time.Time `bson:"last_checked"`
	Last_Updated time.Time `bson:"last_update"`
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

	dburl := flag.String("mongo", "mongodb://localhost:27017", "Mongo connection string")
	flag.Parse()
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(*dburl))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO())
	collection := client.Database(dbname).Collection(collectionname)

	var updatedIDs []string
	var update bson.M
	var now time.Time
	page := 1
	for {
		body, err := GetPages(page)
		if err != nil {
			log.Fatalf("got all pages")
			continue
		}
		if len(body) == 0 {
			break
		}

		var skus Skus
		if err := json.Unmarshal(body, &skus); err != nil {
			break
		}

		for _, product := range skus.Products {

			productObjectID, err := primitive.ObjectIDFromHex(product.ID)
			if err != nil {
				log.Print(err)
				continue
			}

			var ids []string
			for _, comp := range product.Competitors {
				ids = append(ids, comp.ID)
			}
			hashedidstring := CalcHash(ids)

			now = time.Now()

			err = collection.FindOne(context.TODO(), bson.M{"_id": productObjectID}).Decode(&ProductFromDb)
			if err == nil {
				if hashedidstring == ProductFromDb.HashedString {
					update = bson.M{"$set": bson.M{"last_checked": now}}
				} else {
					update = bson.M{
						"$set": bson.M{
							"hashedcompetitorsstring": hashedidstring,
							"last_checked":            now,
							"last_update":             now,
						},
					}
					updatedIDs = append(updatedIDs, product.ID)
				}
				_, err = collection.UpdateByID(context.TODO(), productObjectID, update)
				if err != nil {
					log.Printf("failed to update product %s %v", product.ID, err)
				}
				fmt.Printf("updated product %s. \n", product.ID)

			} else if err == mongo.ErrNoDocuments {
				_, err = collection.InsertOne(context.TODO(), bson.M{
					"_id":                     productObjectID,
					"hashedcompetitorsstring": hashedidstring,
					"last_checked":            now,
					"last_update":             now,
				})
				if err != nil {
					log.Fatalf("failed to insert into mongodb %v", err)
				}
				fmt.Printf("data for product %s inserver \n", product.ID)

			}

		}
		page++
	}
	if len(updatedIDs) == 0 {
		fmt.Print("no updated items")
	} else {
		fmt.Print(strings.Join(updatedIDs, ", "))
	}

}

func CalcHash(ids []string) string {
	joined := strings.Join(ids, "")
	hash := sha256.Sum256([]byte(joined))
	return hex.EncodeToString(hash[:])
}
