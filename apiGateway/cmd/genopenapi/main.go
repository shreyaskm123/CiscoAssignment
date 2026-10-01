// Command genopenapi writes the telemetry API's OpenAPI document.
//
// It exists so the document is a build artefact of the code rather than a
// hand-maintained file. Run it through `go generate ./...` (see
// internal/openapi) or directly:
//
//	go run ./cmd/genopenapi -out openapi.json
//
// It is a separate command so the server binary does not carry the reflection
// code, and so CI can run it and diff the result without starting anything.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"apigateway/internal/openapi"
)

func main() {
	out := flag.String("out", "openapi.json", "path to write the OpenAPI document to")
	check := flag.Bool("check", false, "verify the file on disk is up to date instead of writing it; exit 1 if it is stale")
	flag.Parse()

	want, err := openapi.Generate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate openapi: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", filepath.Dir(*out), err)
		os.Exit(1)
	}

	if *check {
		got, err := os.ReadFile(*out)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s is missing: %v\nrun: go generate ./...\n", *out, err)
			os.Exit(1)
		}
		if string(got) != string(want) {
			fmt.Fprintf(os.Stderr, "%s is out of date with the API definition\nrun: go generate ./...\n", *out)
			os.Exit(1)
		}
		fmt.Printf("%s is up to date\n", *out)
		return
	}

	if err := os.WriteFile(*out, want, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", *out, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s (%d bytes)\n", *out, len(want))
}
