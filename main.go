package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/datastore"
)

func main() {
	projectID := "dusted-codes"
	fromNamespace := "prod"
	fromKind := "ketchup-dotnet"
	toNamespace := "ketchup"
	toKind := "dotnet"

	fmt.Printf(
		"Starting data migration from %s/%s to %s/%s in project %s.",
		fromNamespace, fromKind, toNamespace, toKind, projectID)

	ctx := context.Background()

	fmt.Println("Initialising client...")
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		panic(fmt.Errorf("failed to create Google Cloud Datastore client: %w", err))
	}

	fmt.Println("Querying all entities...")
	var entities []Entity
	query := datastore.NewQuery(fromKind).Namespace(fromNamespace)
	keys, err := client.GetAll(ctx, query, &entities)
	if err != nil {
		panic(fmt.Errorf("error reading from Google Cloud Datastore: %w", err))
	}

	fmt.Printf("\nTotal entities found: %d", len(keys))

	fmt.Printf("\nWriting entities to new destination: %s/%s", toNamespace, toKind)
	for i, e := range entities {
		key := datastore.NameKey(toKind, keys[i].Name, nil)
		key.Namespace = toNamespace
		if _, err := client.Put(ctx, key, &e); err != nil {
			panic(fmt.Errorf("error writing to Google Cloud Datastore: %w", err))
		}
	}

	fmt.Println("Data migration completed successfully.")
}
