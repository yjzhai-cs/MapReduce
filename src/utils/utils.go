package utils

import "os"
import "path"
import "fmt"
import "io/ioutil"
import "log"
import "net"
import "encoding/json"
import "sort"

type ErrorMessage string

func (e ErrorMessage) Error() string {
	return string(e)
}

func MapMarshal(m map[string]string) string {
	if data, err := json.Marshal(m); err == nil {
		return string(data) 
	} else {
		log.Fatal("Marshal failed! Error:", err)
	}
	return ""
} 

func MapUnmarshal(s string) map[string]string {
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err == nil {
		return m
	} else {
		log.Fatal("Unmarshal Failed! Error:", err)
	}
	return nil
}

func SaveMapToFile(dir string, filename string, postFix string, m map[string]string) {
	filePath := path.Join(dir, filename + postFix)
	file, err := os.Create(filePath)
	defer file.Close()
	if err != nil {
		log.Fatal("Can't create file! Error:", err)
		return
	}
	keys := make([]string, 0)
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(file, "%v %v\n", k, m[k])
	}
}

func ReadFromTxt(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("can't open %v.", path)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("can't read %v.", path)
	}
	file.Close()
	return string(content), err
}

func GetFreePort() (port int, err error) {
	// resolve address
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	// if the port of addr is 0, this function ListenTCP will return available port
	listen, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	// close
	defer listen.Close()
	return listen.Addr().(*net.TCPAddr).Port, nil
}

func GetSelfPort() (port int, err error) {

    listen, err := net.Listen("tcp", ":0")
    if err != nil {
		return 0, err
    }
	defer listen.Close()
    return listen.Addr().(*net.TCPAddr).Port, nil
}