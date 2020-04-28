// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//
// Copyright 2019 Crunchy Data
//
// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go
//

// The main package for the Prometheus server executable.
package main


import (
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
  "os/signal"
	"time"

	"path/filepath"
  "github.com/crunchydata/postgresql-prometheus-adapter/pkg/postgresql"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	//"github.com/jamiealquiza/envy"

	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"

  "github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	//"github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
  "gopkg.in/alecthomas/kingpin.v2"
  //"flag"

)

type config struct {
	remoteTimeout      time.Duration
	listenAddr         string
	telemetryPath      string
	pgPrometheusConfig postgresql.Config
	logLevel           string
	haGroupLockId      int
	prometheusTimeout  time.Duration
  promlogConfig      promlog.Config
}

const (
	tickInterval      = time.Second
	promLivenessCheck = time.Second
  max_bgwriter      = 10
  max_bgparser      = 20
)

var (
	receivedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	sentSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sent_samples_total",
			Help: "Total number of processed samples sent to remote storage.",
		},
		[]string{"remote"},
	)
	failedSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "failed_samples_total",
			Help: "Total number of processed samples which failed on send to remote storage.",
		},
		[]string{"remote"},
	)
	sentBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sent_batch_duration_seconds",
			Help:    "Duration of sample batch send calls to the remote storage.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"remote"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_ms",
			Help:    "Duration of HTTP request in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path"},
	)
)

var worker [max_bgwriter]postgresql.PGWriter

func init() {
	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(sentSamples)
	prometheus.MustRegister(failedSamples)
	prometheus.MustRegister(sentBatchDuration)
	prometheus.MustRegister(httpRequestDuration)
}

func main() {
	cfg := parseFlags()
  logger := promlog.New(&cfg.promlogConfig)
	level.Info(logger).Log("config", fmt.Sprintf("%+v", cfg))
	level.Info(logger).Log("pgPrometheusConfig", fmt.Sprintf("%+v", cfg.pgPrometheusConfig))

  if ( cfg.pgPrometheusConfig.PGWriters < 0 ) {
    cfg.pgPrometheusConfig.PGWriters=1
  }
  if ( cfg.pgPrometheusConfig.PGWriters > max_bgwriter ) {
    cfg.pgPrometheusConfig.PGWriters=max_bgwriter
  }

  if ( cfg.pgPrometheusConfig.PGParsers < 0 ) {
    cfg.pgPrometheusConfig.PGParsers=1
  }
  if ( cfg.pgPrometheusConfig.PGParsers > max_bgparser ) {
    cfg.pgPrometheusConfig.PGParsers=max_bgparser
  }

	http.Handle(cfg.telemetryPath, promhttp.Handler())
	writer, reader := buildClients(logger, cfg)

  c := make(chan os.Signal, 1)
  signal.Notify(c, os.Interrupt)
  go func(){
      for sig := range c {
        fmt.Printf("Signal: %v\n", sig)
        for t := 0; t < cfg.pgPrometheusConfig.PGWriters; t++ {
          fmt.Printf("Calling shutdown %d\n", t)
          worker[t].PGWriterShutdown()
        }
        for t := 0; t < cfg.pgPrometheusConfig.PGWriters; t++ {
		      for worker[t].Running {
            time.Sleep( 1 * time.Second )
            fmt.Printf("Waiting for shutdown %d...\n", t)
          }
        }
        os.Exit(0)
      }
  }()
  for t := 0; t < cfg.pgPrometheusConfig.PGWriters; t++ {
    go worker[t].RunPGWriter(logger, t, cfg.pgPrometheusConfig.CommitSecs, cfg.pgPrometheusConfig.CommitRows, cfg.pgPrometheusConfig.PGParsers, cfg.pgPrometheusConfig.PartitionScheme)
    defer worker[t].PGWriterShutdown()
  }

  level.Info(logger).Log("msg", "Starting HTTP Listerner")

	http.Handle("/write", timeHandler("write", write(logger, writer)))
	http.Handle("/read", timeHandler("read", read(logger, reader)))

	level.Info(logger).Log("msg", "Starting up...")
	level.Info(logger).Log("msg", "Listening", "addr", cfg.listenAddr)

  err := http.ListenAndServe(cfg.listenAddr, nil)

  level.Info(logger).Log("msg", "Started HTTP Listerner")

	if err != nil {
		level.Error(logger).Log("msg", "Listen failure", "err", err)
		os.Exit(1)
	}
}

func parseFlags() *config {
	a := kingpin.New(filepath.Base(os.Args[0]), "Remote storage adapter [ PostgreSQL ]")
	a.HelpFlag.Short('h')

	cfg := &config{
    promlogConfig:    promlog.Config{},
  }

	a.Flag("adapter-send-timeout", "The timeout to use when sending samples to the remote storage.").Default("30s").DurationVar(&cfg.remoteTimeout)
	a.Flag("web-listen-address", "Address to listen on for web endpoints.").Default(":9201").StringVar(&cfg.listenAddr)
	a.Flag("web-telemetry-path", "Address to listen on for web endpoints.").Default("/metrics").StringVar(&cfg.telemetryPath)
  flag.AddFlags(a, &cfg.promlogConfig)

  a.Flag("pg-partition", "daily or hourly partitions, default: hourly").Default("hourly").StringVar(&cfg.pgPrometheusConfig.PartitionScheme)
  a.Flag("pg-commit-secs", "Write data to database every N seconds").Default("15").IntVar(&cfg.pgPrometheusConfig.CommitSecs)
  a.Flag("pg-commit-rows", "Write data to database every N Rows").Default("20000").IntVar(&cfg.pgPrometheusConfig.CommitRows)
  a.Flag("pg-threads", "Writer DB threads to run 1-10").Default("1").IntVar(&cfg.pgPrometheusConfig.PGWriters)
  a.Flag("parser-threads", "parser threads to run per DB writer 1-10").Default("5").IntVar(&cfg.pgPrometheusConfig.PGParsers)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error parsing commandline arguments")
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	return cfg
}

type writer interface {
	Write(samples model.Samples) error
	Name() string
}

type reader interface {
	Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error)
	Name() string
	HealthCheck() error
}

func buildClients(logger log.Logger, cfg *config) (writer, reader) {
	pgClient := postgresql.NewClient(log.With(logger, "storage", "PostgreSQL"), &cfg.pgPrometheusConfig)

	return pgClient, pgClient
}

func write(logger log.Logger, writer writer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			level.Error(logger).Log("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			level.Error(logger).Log("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			level.Error(logger).Log("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		samples := protoToSamples(&req)
		receivedSamples.Add(float64(len(samples)))

		err = sendSamples(writer, samples)
		if err != nil {
			level.Warn(logger).Log("msg", "Error sending samples to remote storage", "err", err, "storage", writer.Name(), "num_samples", len(samples))
		}

		//counter, err := sentSamples.GetMetricWithLabelValues(writer.Name())
		//if err != nil {
		//	level.Warn(logger).Log("msg", "Couldn't get a counter", "labelValue", writer.Name(), "err", err)
		//}

	})
}

func read(logger log.Logger, reader reader) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			level.Error(logger).Log("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			level.Error(logger).Log("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			level.Error(logger).Log("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var resp *prompb.ReadResponse
		resp, err = reader.Read(&req)
		if err != nil {
      fmt.Printf("MAIN req.Queries: %v\n", req.Queries)
			level.Warn(logger).Log("msg", "Error executing query", "query", req, "storage", reader.Name(), "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func health(reader reader) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := reader.HealthCheck()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "0")
	})
}

func protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}
	return samples
}

func sendSamples(w writer, samples model.Samples) error {
	begin := time.Now()
	var err error
  err = w.Write(samples)
	duration := time.Since(begin).Seconds()
	if err != nil {
		failedSamples.WithLabelValues(w.Name()).Add(float64(len(samples)))
		return err
	}
	sentSamples.WithLabelValues(w.Name()).Add(float64(len(samples)))
	sentBatchDuration.WithLabelValues(w.Name()).Observe(duration)
	return nil
}

// timeHandler uses Prometheus histogram to track request time
func timeHandler(path string, handler http.Handler) http.Handler {
	f := func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		handler.ServeHTTP(w, r)
		elapsedMs := time.Since(start).Nanoseconds() / int64(time.Millisecond)
		httpRequestDuration.WithLabelValues(path).Observe(float64(elapsedMs))
	}
	return http.HandlerFunc(f)
}

