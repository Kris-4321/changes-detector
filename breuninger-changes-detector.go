package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	collectionname = "breuninger_items"
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

type DbProduct struct {
	ID           string    `bson:"_id"`
	HashedString string    `bson:"competitors_hash"`
	Competitors  []string  `bson:"competitors"`
	Last_Checked time.Time `bson:"last_checked"`
	Last_Updated time.Time `bson:"last_update"`
}

func CalcHash(ids []string) string {
	joined := strings.Join(ids, "")
	hash := sha256.Sum256([]byte(joined))
	return hex.EncodeToString(hash[:])
}

func GetPage(page int) ([]byte, error) {
	url := fmt.Sprintf("https://webapi.intelligencenode.com//breuninger?page=%d&app_key=213c06c51ed7df6fDEB", page)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("end of pages")
	}

	return io.ReadAll(resp.Body)
}

func calcDifferences(old []string, current []string) (int, int) {
	// sort the ids to make sure the hash is consistent
	sort.Slice(old, func(i, j int) bool {
		return old[i] < old[j]
	})
	sort.Slice(current, func(i, j int) bool {
		return current[i] < current[j]
	})

	// find the differences
	var added int
	var removed int
	i := 0
	j := 0
	for i < len(old) && j < len(current) {
		if old[i] < current[j] {
			removed++
			i++
		} else if old[i] > current[j] {
			added++
			j++
		} else {
			i++
			j++
		}
	}

	// if there are some left in the old slice
	removed += len(old) - i
	// if there are some left in the current slice
	added += len(current) - j

	return added, removed
}

func CheckForChanges(product Product, collection *mongo.Collection) (int, int, bool) {
	productObjectID, err := primitive.ObjectIDFromHex(product.ID)
	if err != nil {
		log.Print(err)
		return 0, 0, false
	}

	var ids []string
	for _, comp := range product.Competitors {
		ids = append(ids, comp.ID)
	}

	// sort the ids to make sure the hash is consistent
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	competitorsHash := CalcHash(ids)

	now := time.Now()
	var dbProduct DbProduct
	err = collection.FindOne(context.TODO(), bson.M{"_id": productObjectID}).Decode(&dbProduct)
	if err == mongo.ErrNoDocuments {
		_, err = collection.InsertOne(context.TODO(), bson.M{
			"_id":              productObjectID,
			"competitors_hash": competitorsHash,
			"competitors":      ids,
			"checked":          now,
			"changed":          now,
		})
		if err != nil {
			log.Printf("failed to insert into mongodb %v", err)
		}

		return len(ids), 0, true
	}

	if err != nil {
		log.Printf("Failed to execute mongo request for %s, err: %v", product.ID, err)
		return 0, 0, false
	}

	var update bson.M
	changed := false
	if competitorsHash == dbProduct.HashedString {
		update = bson.M{"$set": bson.M{"last_checked": now}}
	} else {
		update = bson.M{
			"$set": bson.M{
				"competitors_hash": competitorsHash,
				"competitors":      ids,
				"checked":          now,
				"changed":          now,
			},
		}

		changed = true
		log.Printf("Product %s has been changed", product.ID)
	}

	fresh, removed := calcDifferences(dbProduct.Competitors, ids)

	_, err = collection.UpdateByID(context.TODO(), productObjectID, update)
	if err != nil {
		log.Printf("Failed to update product %s, err: %v", product.ID, err)
	}

	return fresh, removed, changed
}

func ProcessPage(wg *sync.WaitGroup, jobs <-chan int, results chan<- []Product, snapshotColl *mongo.Collection) {
	defer wg.Done()
	for page := range jobs {
		body, err := GetPage(page)
		if err != nil {
			log.Printf("error geting page %d %v", page, err)
			continue
		}
		var skus Skus
		if err := json.Unmarshal(body, &skus); err != nil {
			log.Printf("error unmarshaling page %d %v", page, err)
			continue
		}
		results <- skus.Products

	}
}

func main() {
	start := time.Now()
	connectionstring := flag.String("mongo", "mongodb://localhost:27017", "Mongo connection string")
	dbname := flag.String("dbname", "fashion", "Database name")
	logfile := flag.String("log", "", "Print the log into a file")
	help := flag.Bool("help", false, "Print this help")

	flag.Parse()

	if *help {
		flag.PrintDefaults()
		return
	}

	if len(*logfile) > 0 {
		f, err := os.OpenFile(*logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer f.Close()

		log.SetOutput(f)
	}

	// connect to mongo
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(*connectionstring))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO())
	snapshotColl := client.Database(*dbname).Collection(collectionname)
	changesColl := client.Database(*dbname).Collection("breuninger_changes_history")

	var wg sync.WaitGroup
	jobs := make(chan int, 100)
	results := make(chan []Product, 100)

	for w := 0; w < 5; w++ {
		wg.Add(1)
		go ProcessPage(&wg, jobs, results, snapshotColl)
	}

	go func() {
		page := 1
		for {
			_, err := GetPage(page)
			if err != nil {
				if err.Error() == "end of pages" {
					break
				}
				log.Printf("error getting page %d %v", page, err)
			}
			jobs <- page
			page++
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	updatedIDs := 0
	checked := 0
	freshMatches := 0
	removedMatches := 0

	for products := range results {
		for _, product := range products {
			if fresh, missing, isChanged := CheckForChanges(product, snapshotColl); isChanged {
				updatedIDs++
				freshMatches += fresh
				removedMatches += missing
			}
		}
		checked += len(products)
	}

	if updatedIDs == 0 {
		log.Print("No changed items found")
	} else {
		log.Printf("Total changed items: %d", updatedIDs)
	}
	end := time.Now()
	fmt.Println(end.Sub(start))
}
