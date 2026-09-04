package acknowledgement

type entityPageFetcher func(afterEntityID string, limit int) ([]string, error)

type entityPageHandler func(entityIds []string, pageNum int) error

// forEachEntityPage walks distinct entity ids using cursor pagination.
func forEachEntityPage(pageSize int, fetchEntityIds entityPageFetcher, onPage entityPageHandler) (pagesProcessed int, err error) {
	if pageSize <= 0 {
		pageSize = getEntityPageSize()
	}

	lastEntityID := ""
	for {
		entityIds, err := fetchEntityIds(lastEntityID, pageSize)
		if err != nil {
			return pagesProcessed, err
		}
		if len(entityIds) == 0 {
			return pagesProcessed, nil
		}

		pagesProcessed++
		if err := onPage(entityIds, pagesProcessed); err != nil {
			return pagesProcessed, err
		}
		lastEntityID = entityIds[len(entityIds)-1]
	}
}
