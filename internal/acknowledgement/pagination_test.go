package acknowledgement

import (
	"errors"
	"testing"
)

func TestForEachEntityPageMultiPageCursor(t *testing.T) {
	pages := [][]string{
		{"entity-a", "entity-b"},
		{"entity-c"},
		nil,
	}
	cursors := []string{"", "entity-b", "entity-c"}
	call := 0
	var processed [][]string

	pagesProcessed, err := forEachEntityPage(2, func(afterEntityID string, limit int) ([]string, error) {
		if call >= len(cursors) {
			t.Fatal("unexpected extra fetch call")
		}
		if afterEntityID != cursors[call] {
			t.Fatalf("page %d: expected cursor %q, got %q", call, cursors[call], afterEntityID)
		}
		if limit != 2 {
			t.Fatalf("expected page size 2, got %d", limit)
		}
		ids := pages[call]
		call++
		return ids, nil
	}, func(entityIds []string, pageNum int) error {
		processed = append(processed, append([]string(nil), entityIds...))
		if pageNum != len(processed) {
			t.Fatalf("unexpected page number %d", pageNum)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if pagesProcessed != 2 {
		t.Fatalf("expected 2 pages, got %d", pagesProcessed)
	}
	if len(processed) != 2 || len(processed[0]) != 2 || processed[1][0] != "entity-c" {
		t.Fatalf("unexpected processed pages: %v", processed)
	}
}

func TestForEachEntityPageEmpty(t *testing.T) {
	pagesProcessed, err := forEachEntityPage(100, func(afterEntityID string, limit int) ([]string, error) {
		if afterEntityID != "" {
			t.Fatalf("expected empty cursor, got %q", afterEntityID)
		}
		return nil, nil
	}, func(entityIds []string, pageNum int) error {
		t.Fatal("onPage should not be called")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if pagesProcessed != 0 {
		t.Fatalf("expected 0 pages, got %d", pagesProcessed)
	}
}

func TestForEachEntityPageFetchError(t *testing.T) {
	fetchErr := errors.New("fetch failed")
	_, err := forEachEntityPage(100, func(afterEntityID string, limit int) ([]string, error) {
		return nil, fetchErr
	}, func(entityIds []string, pageNum int) error {
		t.Fatal("onPage should not be called")
		return nil
	})
	if !errors.Is(err, fetchErr) {
		t.Fatalf("expected fetch error, got %v", err)
	}
}

func TestForEachEntityPageHandlerErrorStopsPagination(t *testing.T) {
	handlerErr := errors.New("page failed")
	pagesProcessed, err := forEachEntityPage(100, func(afterEntityID string, limit int) ([]string, error) {
		return []string{"entity-a"}, nil
	}, func(entityIds []string, pageNum int) error {
		return handlerErr
	})
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if pagesProcessed != 1 {
		t.Fatalf("expected 1 page processed before error, got %d", pagesProcessed)
	}
}

func TestForEachEntityPageUsesDefaultPageSize(t *testing.T) {
	t.Setenv(ackProcessorEntityPageSizeEnv, "")
	_, err := forEachEntityPage(0, func(afterEntityID string, limit int) ([]string, error) {
		if limit != defaultEntityPageSize {
			t.Fatalf("expected default page size %d, got %d", defaultEntityPageSize, limit)
		}
		return nil, nil
	}, func(entityIds []string, pageNum int) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
