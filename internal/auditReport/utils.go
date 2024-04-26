package auditReport

import "os"

func createTempFile(fileName string) (*os.File, error) {
	f, err := os.Create(os.TempDir() + "/" + fileName + ".csv")
	if err != nil {
		return nil, err
	}
	return f, nil
}
