package common

import "os"

func CreateTempFile(fileName string) (*os.File, error) {
	f, err := os.Create(os.TempDir() + "/" + fileName + ".csv")
	if err != nil {
		return nil, err
	}
	return f, nil
}
