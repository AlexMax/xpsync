package main

import "log"

func main() {
	filename := "zanxp.db"

	_, err := NewDatabase(filename)
	if err != nil {
		log.Fatalf("could not open database: %s", filename)
	}
}
