package postgresql

import (
  "encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"
  "sync"
	"github.com/crunchydata/postgresql-prometheus-adapter/pkg/log"
  "context"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

type tMetricIDMap map[string]int64

// Config for the database
type Config struct {
	CommitSecs                int
	CommitRows                int
	partitionScheme           string
}

// ParseFlags parses the configuration flags specific to PostgreSQL
func ParseFlags(cfg *Config) *Config {
	flag.IntVar(&cfg.CommitSecs, "pg-commit-secs", 15, "Write data to database every N seconds or N Rows")
	flag.IntVar(&cfg.CommitRows, "pg-commit-rows", 15, "Write data to database every N Rows or N Seconds")
	flag.StringVar(&cfg.partitionScheme, "pg-partition", "daily", "daily or hourly partitions, default: daily")
	return cfg
}

// Client sends Prometheus samples to PostgreSQL
type Client struct {
	DBW             *pgx.Conn
	DBR             *pgx.Conn
	cfg             *Config
	KeepRunning     bool
	Interval        int
	Rows            int
  Mutex           sync.Mutex
  WhichOne        int
  valueRowsOne    [][]interface{}
  valueRowsTwo    [][]interface{}
  labelRows       [][]interface{}
  vMetricIDMap    tMetricIDMap
}

// Run starts the client and listens for a shutdown call.
func (c *Client) Run() {
  period := c.cfg.CommitSecs
	log.Info("bgwriter","Started")

	// Loop that runs forever
	for c.KeepRunning {
		if period <= 0 {
		  c.Action()
      period = c.cfg.CommitSecs
    }else {
      time.Sleep( 10 * time.Second)
      period -= 10
    }
	}
  c.Action()
	log.Info("bgwriter","Shutdown")
}

// Shutdown is a graceful shutdown mechanism 
func (c *Client) Shutdown() {
  c.Mutex.Lock()
  c.KeepRunning = false
  c.Mutex.Unlock()
}

// Action defines what the client does; override this. 
// For now we'll just wait two seconds and print to simulate work.
func (c *Client) Action() {
  var copyCount, lblCount, rowCount int64
  var err error
  if (c.WhichOne == 1 && len(c.valueRowsOne) > c.Rows ) || (c.WhichOne == 2 && len(c.valueRowsTwo) > c.Rows ) || !c.KeepRunning {
    c.Mutex.Lock()
    if c.WhichOne == 1 {
      c.WhichOne = 2
    } else {
      c.WhichOne = 1
    }
	  begin := time.Now()
    lblCount = int64(len(c.labelRows))
    if lblCount > 0 {
      copyCount, err := c.DBW.CopyFrom(context.Background(), pgx.Identifier{"metric_labels"}, []string{"metric_id", "metric_name", "metric_name_label", "metric_labels"}, pgx.CopyFromRows(c.labelRows))
      c.labelRows = nil
	    if err != nil {
		    log.Error("msg", "COPY failed for metric_labels", "err", err)
	    }
	    if copyCount != lblCount {
		    log.Error("msg", "All rows not copied metric_labels", "err", err)
	    }
    }
    if c.WhichOne == 1 {
      copyCount, err = c.DBW.CopyFrom(context.Background(), pgx.Identifier{"metric_values"}, []string{"metric_id", "metric_time", "metric_value"}, pgx.CopyFromRows(c.valueRowsTwo))
      rowCount = int64(len(c.valueRowsTwo))
      c.valueRowsTwo = nil
    } else {
      copyCount, err = c.DBW.CopyFrom(context.Background(), pgx.Identifier{"metric_values"}, []string{"metric_id", "metric_time", "metric_value"}, pgx.CopyFromRows(c.valueRowsOne))
      rowCount = int64(len(c.valueRowsOne))
      c.valueRowsOne = nil
    }
    c.Mutex.Unlock()
    if err != nil {
      log.Error("msg", "COPY failed for metric_values", "err", err)
    }
    if copyCount != rowCount {
      log.Error("msg", "All rows not copied metric_values", "err", err)
    }
    duration := time.Since(begin).Seconds()
    log.Info("metric", fmt.Sprintf("BGWriter: Processed samples count,%d, duration,%v", rowCount + lblCount, duration) )
  }
}



// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config) *Client {

	conn1, err1 := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
  if err1 != nil {
    log.Error("err", err1)
    os.Exit(1)
  }

	conn2, err2 := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
  if err2 != nil {
    log.Error("err", err2)
    os.Exit(1)
  }

	client := &Client{
		DBW:  conn1,
		DBR:  conn2,
		cfg: cfg,
		KeepRunning:    true,
    WhichOne:       1,
		Interval:        cfg.CommitSecs,
		Rows  :          cfg.CommitRows,
	}

  log.Info("msg", "calling client.setupPgPrometheus()")
  err := client.setupPgPrometheus()

  if err != nil {
    log.Error("err", err)
    os.Exit(1)
  }

	return client
}

func (c *Client) setupPgPrometheus() error {
  c.Mutex.Lock()
  defer c.Mutex.Unlock()
  log.Info("msg", "creating tables")

	_, err := c.DBW.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS metric_labels ( metric_id BIGINT PRIMARY KEY, metric_name TEXT NOT NULL, metric_name_label TEXT NOT NULL, metric_labels jsonb, UNIQUE(metric_name, metric_labels) )")
	if err != nil {
		return err
	}

	_, err = c.DBW.Exec(context.Background(), "CREATE INDEX IF NOT EXISTS metric_labels_labels_idx ON metric_labels USING GIN (metric_labels)")
  if err != nil {
		return err
	}

	_, err = c.DBW.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS metric_values (metric_id BIGINT, metric_time TIMESTAMPTZ, metric_value FLOAT8 ) PARTITION BY RANGE (metric_time)")
  if err != nil {
		return err
	}

	_, err = c.DBW.Exec(context.Background(), "CREATE INDEX IF NOT EXISTS metric_values_id_time_idx on metric_values USING btree (metric_id, metric_time DESC)")
  if err != nil {
		return err
	}

	_, err = c.DBW.Exec(context.Background(), "CREATE INDEX IF NOT EXISTS metric_values_time_idx  on metric_values USING btree (metric_time DESC)")
  if err != nil {
		return err
	}


  sDate := time.Now()
  eDate := sDate
  for i:=0; i<5;  i++ {
    if c.cfg.partitionScheme == "daily" {
	    _, err = c.DBW.Exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_values_%s PARTITION OF metric_values FOR VALUES FROM ('%s 00:00:00') TO ('%s 00:00:00')", sDate.AddDate(0, 0, i).Format("20060102"), sDate.AddDate(0, 0, i).Format("2006-01-02"), eDate.AddDate(0, 0, 1+i).Format("2006-01-02") ) )
      if err != nil {
		    return err
      }
    } else if c.cfg.partitionScheme == "hourly" {
	    _, err = c.DBW.Exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_values_%s PARTITION OF metric_values FOR VALUES FROM ('%s 00:00:00') TO ('%s 00:00:00') PARTITION BY RANGE (metric_time)", sDate.AddDate(0, 0, i).Format("20060102"), sDate.AddDate(0, 0, i).Format("2006-01-02"), eDate.AddDate(0, 0, 1+i).Format("2006-01-02") ) )
      if err != nil {
		    return err
      }
      var h int
      for h=0; h<23; h++ {
	      _, err = c.DBW.Exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_values_%s_%02d PARTITION OF metric_values_%s FOR VALUES FROM ('%s %02d:00:00') TO ('%s %02d:00:00')", sDate.AddDate(0, 0, i).Format("20060102"), h, sDate.AddDate(0, 0, i).Format("20060102"), sDate.AddDate(0, 0, i).Format("2006-01-02"), h, eDate.AddDate(0, 0, i).Format("2006-01-02"), h+1 ) )
        if err != nil {
		      return err
        }
      }
	    _, err = c.DBW.Exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_values_%s_%02d PARTITION OF metric_values_%s FOR VALUES FROM ('%s %02d:00:00') TO ('%s 00:00:00')", sDate.AddDate(0, 0, i).Format("20060102"), h, sDate.AddDate(0, 0, i).Format("20060102"), sDate.AddDate(0, 0, i).Format("2006-01-02"), h, eDate.AddDate(0, 0, i+1).Format("2006-01-02") ) )
      if err != nil {
		    return err
      }
    }
  }

  /*for i:=0; i<5;  i++ {
	  _, err = c.DBW.Exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_values_%s PARTITION OF metric_values FOR VALUES FROM ('%s 00:00:00') TO ('%s 00:00:00')", sDate.AddDate(0, 0, i).Format("20060102"), sDate.AddDate(0, 0, i).Format("2006-01-02"), eDate.AddDate(0, 0, 1+i).Format("2006-01-02")  ))
    if err != nil {
		  return err
    }
  }*/

  c.vMetricIDMap = make(tMetricIDMap)
	rows, err1 := c.DBW.Query(context.Background(), "SELECT metric_name_label, metric_id from metric_labels" )

	if err1 != nil {
    rows.Close()
    log.Info("msg","Error reading metric_labels");
		return err
	}

  for rows.Next() {
    var (
      metric_name_label   string
      metric_id  int64
    )
    err := rows.Scan(&metric_name_label, &metric_id)

    if err != nil {
      rows.Close()
      log.Info("msg","Error scaning metric_labels");
      return err
    }
    //log.Info("msg",fmt.Sprintf("YS>\t>%s<\t>%s<",metric_name_label, metric_id ) )
    c.vMetricIDMap[metric_name_label]=metric_id
  }
  log.Info("msg",fmt.Sprintf("%d Rows Loaded in map: ", len(c.vMetricIDMap ) ) )
  rows.Close()

	return nil
}

func metricString(m model.Metric) string {
	metricName, hasName := m[model.MetricNameLabel]
	numLabels := len(m) - 1
	if !hasName {
		numLabels = len(m)
	}
	labelStrings := make([]string, 0, numLabels)
	for label, value := range m {
		if label != model.MetricNameLabel {
			labelStrings = append(labelStrings, fmt.Sprintf("\"%s\": %q", label, value))
		}
	}

	switch numLabels {
	case 0:
		if hasName {
			return string(metricName)
		}
		return "{}"
	default:
		sort.Strings(labelStrings)
		return fmt.Sprintf("%s{%s}", metricName, strings.Join(labelStrings, ", "))
	}
}

// Write implements the Writer interface and writes metric samples to the database
func (c *Client) Write(samples model.Samples) error {
	begin := time.Now()
  c.Mutex.Lock()
  defer c.Mutex.Unlock()
  var nextId int64 = int64(len(c.vMetricIDMap) + 1)

  for _, sample := range samples {
    sMetric:=metricString(sample.Metric)
		milliseconds := sample.Timestamp.UnixNano() / 1000000

    id, ok := c.vMetricIDMap[sMetric]

    if !ok {
      c.vMetricIDMap[sMetric]=nextId
      i := strings.Index(sMetric, "{")
      jsonbMap := make(map[string]interface{})
      json.Unmarshal([]byte( sMetric[i:] ), &jsonbMap)
      c.labelRows = append(c.labelRows, []interface{}{int64(nextId), sMetric[:i], sMetric, jsonbMap })

      id = nextId
      nextId += 1
    }
    if (c.WhichOne == 1) {
      c.valueRowsOne = append(c.valueRowsOne, []interface{}{int64(id), toTimestamp(milliseconds), float64( sample.Value) } )
    } else {
      c.valueRowsTwo = append(c.valueRowsTwo, []interface{}{int64(id), toTimestamp(milliseconds), float64( sample.Value) } )
    }

	}
	duration := time.Since(begin).Seconds()
  log.Info("metric", fmt.Sprintf("Writer: Processed samples count,0, duration,%v", duration) )

	return nil
}

type sampleLabels struct {
	JSON        []byte
	Map         map[string]string
	OrderedKeys []string
}

func createOrderedKeys(m *map[string]string) []string {
	keys := make([]string, 0, len(*m))
	for k := range *m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (c *Client) Close() {
	if c.DBW != nil {
		if err1 := c.DBW.Close(context.Background()); err1 != nil {
			log.Error("msg", err1.Error())
		}
	}
	if c.DBR != nil {
		if err2 := c.DBR.Close(context.Background()); err2 != nil {
			log.Error("msg", err2.Error())
		}
	}
}

func (l *sampleLabels) Scan(value interface{}) error {
	if value == nil {
		l = &sampleLabels{}
		return nil
	}

	switch t := value.(type) {
	case []uint8:
		m := make(map[string]string)
		err := json.Unmarshal(t, &m)

		if err != nil {
			return err
		}

		*l = sampleLabels{
			JSON:        t,
			Map:         m,
			OrderedKeys: createOrderedKeys(&m),
		}
		return nil
	}
	return fmt.Errorf("invalid labels value %s", reflect.TypeOf(value))
}

func (l sampleLabels) String() string {
	return string(l.JSON)
}

func (l sampleLabels) key(extra string) string {
	// 0xff cannot cannot occur in valid UTF-8 sequences, so use it
	// as a separator here.
	separator := "\xff"
	pairs := make([]string, 0, len(l.Map)+1)
	pairs = append(pairs, extra+separator)

	for _, k := range l.OrderedKeys {
		pairs = append(pairs, k+separator+l.Map[k])
	}
	return strings.Join(pairs, separator)
}

func (l *sampleLabels) len() int {
	return len(l.OrderedKeys)
}

// Read implements the Reader interface and reads metrics samples from the database
func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {

  fmt.Printf("READ req.Queries: %v\n", req.Queries)
	labelsToSeries := map[string]*prompb.TimeSeries{}

	for _, q := range req.Queries {
		command, err := c.buildCommand(q)

		if err != nil {
			return nil, err
		}

		log.Debug("msg", "Executed query", "query", command)

		rows, err := c.DBR.Query(context.Background(), command)

		if err != nil {
      rows.Close()
			return nil, err
		}

		for rows.Next() {
			var (
				value  float64
				name   string
				labels sampleLabels
				time   time.Time
			)
			err := rows.Scan(&time, &name, &value, &labels)

			if err != nil {
		    rows.Close()
				return nil, err
			}

			key := labels.key(name)
			ts, ok := labelsToSeries[key]

			if !ok {
				labelPairs := make([]prompb.Label, 0, labels.len()+1)
				labelPairs = append(labelPairs, prompb.Label{
					Name:  model.MetricNameLabel,
					Value: name,
				})

				for _, k := range labels.OrderedKeys {
					labelPairs = append(labelPairs, prompb.Label{
						Name:  k,
						Value: labels.Map[k],
					})
				}

				ts = &prompb.TimeSeries{
					Labels:  labelPairs,
					Samples: make([]prompb.Sample, 0, 100),
				}
				labelsToSeries[key] = ts
			}

			ts.Samples = append(ts.Samples, prompb.Sample{
				Timestamp: time.UnixNano() / 1000000,
				Value:     value,
			})
		}

		err = rows.Err()
		rows.Close()

		if err != nil {
			return nil, err
		}
	}

	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: make([]*prompb.TimeSeries, 0, len(labelsToSeries)),
			},
		},
	}
	for _, ts := range labelsToSeries {
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
	}

	log.Debug("msg", "Returned response", "#timeseries", len(labelsToSeries))

	return &resp, nil
}

// HealthCheck implements the healtcheck interface
func (c *Client) HealthCheck() error {
	rows, err := c.DBR.Query(context.Background(), "SELECT 1")
  defer rows.Close()
	if err != nil {
		log.Debug("msg", "Health check error", "err", err)
		return err
	}

	return nil
}

func toTimestamp(milliseconds int64) time.Time {
	sec := milliseconds / 1000
	nsec := (milliseconds - (sec * 1000)) * 1000000
	return time.Unix(sec, nsec).UTC()
}

func (c *Client) buildQuery(q *prompb.Query) (string, error) {
	matchers := make([]string, 0, len(q.Matchers))
	labelEqualPredicates := make(map[string]string)

	for _, m := range q.Matchers {
		escapedName := escapeValue(m.Name)
		escapedValue := escapeValue(m.Value)

		if m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				if len(escapedValue) == 0 {
					matchers = append(matchers, fmt.Sprintf("(l.metric_name IS NULL OR name = '')"))
				} else {
					matchers = append(matchers, fmt.Sprintf("l.metric_name = '%s'", escapedValue))
				}
			case prompb.LabelMatcher_NEQ:
				matchers = append(matchers, fmt.Sprintf("l.metric_name != '%s'", escapedValue))
			case prompb.LabelMatcher_RE:
				matchers = append(matchers, fmt.Sprintf("l.metric_name ~ '%s'", anchorValue(escapedValue)))
			case prompb.LabelMatcher_NRE:
				matchers = append(matchers, fmt.Sprintf("l.metric_name !~ '%s'", anchorValue(escapedValue)))
			default:
				return "", fmt.Errorf("unknown metric name match type %v", m.Type)
			}
		} else {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				if len(escapedValue) == 0 {
					// From the PromQL docs: "Label matchers that match
					// empty label values also select all time series that
					// do not have the specific label set at all."
					matchers = append(matchers, fmt.Sprintf("((l.metric_labels ? '%s') = false OR (l.metric_labels->>'%s' = ''))",
						escapedName, escapedName))
				} else {
					labelEqualPredicates[escapedName] = escapedValue
				}
			case prompb.LabelMatcher_NEQ:
				matchers = append(matchers, fmt.Sprintf("l.metric_labels->>'%s' != '%s'", escapedName, escapedValue))
			case prompb.LabelMatcher_RE:
				matchers = append(matchers, fmt.Sprintf("l.metric_labels->>'%s' ~ '%s'", escapedName, anchorValue(escapedValue)))
			case prompb.LabelMatcher_NRE:
				matchers = append(matchers, fmt.Sprintf("l.metric_labels->>'%s' !~ '%s'", escapedName, anchorValue(escapedValue)))
			default:
				return "", fmt.Errorf("unknown match type %v", m.Type)
			}
		}
	}
	equalsPredicate := ""

	if len(labelEqualPredicates) > 0 {
		labelsJSON, err := json.Marshal(labelEqualPredicates)

		if err != nil {
			return "", err
		}
		equalsPredicate = fmt.Sprintf(" AND l.metric_labels @> '%s'", labelsJSON)
	}

	matchers = append(matchers, fmt.Sprintf("v.metric_time >= '%v'", toTimestamp(q.StartTimestampMs).Format(time.RFC3339)))
	matchers = append(matchers, fmt.Sprintf("v.metric_time <= '%v'", toTimestamp(q.EndTimestampMs).Format(time.RFC3339)))

	return fmt.Sprintf("SELECT v.metric_time, l.metric_name, v.metric_value, l.metric_labels FROM metric_values v, metric_labels l WHERE l.metric_id = v.metric_id and %s %s ORDER BY v.metric_time",
		strings.Join(matchers, " AND "), equalsPredicate), nil
}

func (c *Client) buildCommand(q *prompb.Query) (string, error) {
	return c.buildQuery(q)
}

func escapeValue(str string) string {
	return strings.Replace(str, `'`, `''`, -1)
}

// anchorValue adds anchors to values in regexps since PromQL docs
// states that "Regex-matches are fully anchored."
func anchorValue(str string) string {
	l := len(str)

	if l == 0 || (str[0] == '^' && str[l-1] == '$') {
		return str
	}

	if str[0] == '^' {
		return fmt.Sprintf("%s$", str)
	}

	if str[l-1] == '$' {
		return fmt.Sprintf("^%s", str)
	}

	return fmt.Sprintf("^%s$", str)
}

// Name identifies the client as a PostgreSQL client.
func (c Client) Name() string {
	return "PostgreSQL"
}

