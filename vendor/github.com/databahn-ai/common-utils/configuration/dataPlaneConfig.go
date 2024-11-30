package configuration

var dataPlaneId string

func loadDataPlaneId(reader ConfigReader) {
	dataPlaneId = reader.GetString("dataplane.id")
}

func GetDataPlaneId(appConfig ConfigReader) string {
	if dataPlaneId == "" {
		loadDataPlaneId(appConfig)
	}
	return dataPlaneId
}
