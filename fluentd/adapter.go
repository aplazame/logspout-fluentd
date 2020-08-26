package fluentd

/**
*
*
This is a fluent forwarder plugin for Logspout. It uses the fluent-logger-golang
library to forward logs to fluentd (or fluentbit). Run logspout via the following
command after building:

	>> docker run --rm --name="logspout" \
			-v /var/run/docker.sock:/var/run/docker.sock \
			-e TAG_PREFIX=docker \
			-e TAG_SUFFIX_LABEL="com.amazonaws.ecs.container-name" \
			-e FLUENTD_ASYNC_CONNECT="true" \
			-e LOGSPOUT="ignore" \
			<REGISTRY>/<CUSTOM_LOGSPOUT>:<VERSION> \
				./logspout fluentd://<FLUENTD_IP>:<FLUENTD_PORT>
*
*
*/
import (
	"log"
	"math"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/fluent/fluent-logger-golang/fluent"
	"github.com/gliderlabs/logspout/router"
	"github.com/pkg/errors"
)

const (
	defaultProtocol    = "tcp"
	defaultBufferLimit = 1024 * 1024

	defaultWriteTimeout = 3
	defaultRetryWait    = 1000
	defaultMaxRetries   = math.MaxInt32
)

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

// Adapter is an adapter for streaming JSON to a fluentd collector.
type Adapter struct {
	writer         *fluent.Fluent
	tagPrefix      string
	tagSuffixLabel string
	dockerLabels   string
}

// Stream handles a stream of messages from Logspout. Implements router.logAdapter.
func (ad *Adapter) Stream(logstream chan *router.Message) {
	for message := range logstream {
		// Skip if message is empty
		messageIsEmpty, err := regexp.MatchString("^[[:space:]]*$", message.Data)
		if messageIsEmpty {
			log.Println("Skipping empty message!")
			continue
		}

		// Set tag
		tag := ""
		if len(ad.tagPrefix) > 0 {
			tag = ad.tagPrefix
		}
		tagSuffix := message.Container.Config.Labels[ad.tagSuffixLabel]
		if tagSuffix == "" {
			tagSuffix = message.Container.Name + "-" + message.Container.Config.Hostname
		}
		tag = tag + "." + tagSuffix

		// Construct record
		record := map[string]string{
			"log":            message.Data,
			"container_id":   message.Container.ID,
			"container_name": message.Container.Name,
			"source":         message.Source,
		}

		if len(ad.dockerLabels) > 0 {
			for label, value := range message.Container.Config.Labels {
				record["container_label_" + strings.ReplaceAll(label, ".", "_")] = value
			}
		}

		log.Println(tag, message.Time, record)

		// Send to fluentd
		err = ad.writer.PostWithTime(tag, message.Time, record)
		if err != nil {
			log.Println("fluentd-adapter PostWithTime Error: ", err)
			continue
		}
	}
}

// NewAdapter creates a Logspout fluentd adapter instance.
func NewAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("tcp"))
	if !found {
		return nil, errors.New("Unable to find adapter: " + route.Adapter)
	}
	_, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}
	log.Println("Connectivity successful to fluentd @ " + route.Address)

	// Construct fluentd config object
	host, port, err := net.SplitHostPort(route.Address)
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return nil, errors.Wrapf(err, "Invalid fluentd-address %s", route.Address)
	}

	bufferLimit, err := strconv.Atoi(getenv("FLUENTD_BUFFER_LIMIT", strconv.Itoa(defaultBufferLimit)))
	if err != nil {
		return nil, err
	}

	retryWait, err := strconv.Atoi(getenv("FLUENTD_RETRY_WAIT", strconv.Itoa(defaultRetryWait)))
	if err != nil {
		return nil, err
	}

	maxRetries, err := strconv.Atoi(getenv("FLUENTD_MAX_RETRIES", strconv.Itoa(defaultMaxRetries)))
	if err != nil {
		return nil, err
	}

	asyncConnect, err := strconv.ParseBool(getenv("FLUENTD_ASYNC_CONNECT", "false"))
	if err != nil {
		return nil, err
	}

	subSecondPrecision, err := strconv.ParseBool(getenv("FLUENTD_SUBSECOND_PRECISION", "false"))
	if err != nil {
		return nil, err
	}

	requestAck, err := strconv.ParseBool(getenv("FLUENTD_REQUEST_ACK", "false"))
	if err != nil {
		return nil, err
	}

	writeTimeout, err := strconv.Atoi(getenv("FLUENTD_WRITE_TIMEOUT", strconv.Itoa(defaultWriteTimeout)))
	if err != nil {
		return nil, err
	}

	fluentConfig := fluent.Config{
		FluentHost:         host,
		FluentPort:         portNum,
		FluentNetwork:      defaultProtocol,
		FluentSocketPath:   "",
		BufferLimit:        bufferLimit,
		RetryWait:          retryWait,
		MaxRetry:           maxRetries,
		Async:              asyncConnect,
		SubSecondPrecision: subSecondPrecision,

		// RequestAck currently doesn't work with fluent-bit
		// Set to false for now if forwarding to fluent-bit.
		// https://github.com/fluent/fluent-bit/issues/786
		RequestAck:   requestAck,
		WriteTimeout: time.Duration(writeTimeout) * time.Second,
	}
	writer, err := fluent.New(fluentConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to create fluentd logger")
	}

	return &Adapter{
		writer:         writer,
		tagPrefix:      getenv("TAG_PREFIX", "docker"),
		tagSuffixLabel: getenv("TAG_SUFFIX_LABEL", ""),
		dockerLabels:   getenv("DOCKER_LABELS", ""),
	}, nil
}

func init() {
	router.AdapterFactories.Register(NewAdapter, "fluentd")
}
