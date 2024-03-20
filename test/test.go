package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

// Define a struct that matches the JSON response structure
type NumberOfpages struct {
	NumberOfPages int `json:"number_of_pages"`
}

func GetPagesNumber() int {
	url := "https://webapi.intelligencenode.com/breuninger?page=1&app_key=213c06c51ed7df6fDEB"
	response, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	var apiResponse NumberOfpages
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		log.Fatal(err)
	}
	return apiResponse.NumberOfPages
}

func main() {

	pages := GetPagesNumber()
	for i := 1; i <= pages; i++ {
		fmt.Println(i)
	}
}
