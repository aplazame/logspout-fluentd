package fluentd

import (
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fluent/fluent-logger-golang/fluent"
	docker "github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
	"github.com/stretchr/testify/assert"
)

const RECV_BUF_LEN = 1024

// Test data
var (
	testData       = "TEST DATA"
	testTag        = "docker"
	testLabel      = "com.service"
	testLabelValue = "test"
)

// Setup data
var (
	serverIP   = "127.0.0.1"
	serverPort = 3000
	serverAddr = serverIP + ":" + strconv.Itoa(serverPort)
	container  = &docker.Container{
		ID:   "8dfafdbc3a40",
		Name: "\x00container",
		Config: &docker.Config{
			Hostname: "8dfafdbc3a40",
			Labels:   map[string]string{testLabel: testLabelValue},
		},
	}
)

func NewTestTCPServer(t *testing.T, serverAddr string, tester func([]interface{})) {
	ln, err := net.Listen("tcp", serverAddr)
	defer ln.Close()
	if err != nil {
		t.Fatal(err)
	}
	var conn net.Conn
	conn, err = ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	// Get the data
	buf := make([]byte, RECV_BUF_LEN)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	// Unmarshal
	var data []interface{}
	err = json.Unmarshal(buf[:n], &data)
	if err != nil {
		t.Fatal(err)
	}

	// Assert on the data
	tester(data)
}

func sendLogStream(stream chan *router.Message, data string) {
	msg := &router.Message{
		Container: container,
		Data:      data,
		Time:      time.Now(),
		Source:    "stdout",
	}
	stream <- msg
	close(stream)
}

func TestAdapterSuccessfullyWritesToFluentd(t *testing.T) {
	t.Parallel()

	// Wait for tcp server go routine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		NewTestTCPServer(t, serverAddr, func(data []interface{}) {
			tag := data[0].(string)
			assert.Equal(t, testTag+"."+testLabelValue, tag)

			log := data[2].(map[string]interface{})["log"]
			log = log.(string)
			assert.Equal(t, testData, log)

			containerID := data[2].(map[string]interface{})["container_id"]
			assert.Equal(t, container.ID, containerID)

			labelValue := data[2].(map[string]interface{})["container_label_" + strings.ReplaceAll(testLabel, ".", "_")]
			assert.Equal(t, testLabelValue, labelValue)
		})
	}()

	adapter := Adapter{
		writer: &fluent.Fluent{
			Config: fluent.Config{
				MaxRetry:      10,
				MaxRetryWait:  2,
				Async:         false,
				FluentNetwork: "tcp",
				FluentPort:    serverPort,
				MarshalAsJSON: true,
			},
		},
		tagPrefix:      testTag,
		tagSuffixLabel: testLabel,
		dockerLabels: "yes",
	}

	stream := make(chan *router.Message)
	go adapter.Stream(stream)
	sendLogStream(stream, testData)
	wg.Wait()
}
