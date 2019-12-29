package internal

import (
	. "github.com/armandmcqueen/goofys/api/common"
	"io"

	"bufio"
	"encoding/csv"
	"os"
)

type ManifestEntry struct {
	Path   string `json:"path"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

var dfsLog = GetLogger("DatasetFS")


func loadManifest() []ManifestEntry {
	dfsLog.Info("Loading test manifest")

	csvFile, _ := os.Open("dataset_fs/manifest.csv")
	reader := csv.NewReader(bufio.NewReader(csvFile))

	var manifest []ManifestEntry
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		manifest = append(manifest, ManifestEntry{
			Path:   line[0],
			Bucket: line[1],
			Key:    line[2],
		})
	}
	//manifestJson, _ := json.MarshalIndent(manifest, "", "    ")
	//fmt.Println(string(manifestJson))

	return manifest
}



