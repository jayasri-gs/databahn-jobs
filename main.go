package main

import (
	"context"
	"flag"
	"github.com/databahn-ai/databahn-jobs/cmd"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"net/http"
)

func main() {
	job := flag.String("job", "", "job name")
	flag.Parse()

	ctx := context.Background()
	go cmd.RunJob(ctx, *job)

	r := chi.NewRouter()
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		render.Status(r, http.StatusOK)
		render.PlainText(w, r, "healthy")
		return
	})
	err := http.ListenAndServe(":8080", r)
	if err != nil {
		panic(err)
	}
}
