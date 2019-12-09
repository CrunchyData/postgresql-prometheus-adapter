package postgresql

import (
  "runtime"
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
  "container/list"
)

type tMetricIDMap map[string]int64

// Config for the database
type Config struct {
  CommitSecs                int
  CommitRows                int
  PGWriters                 int
  PartitionScheme           string
}

// ParseFlags parses the configuration flags specific to PostgreSQL
func ParseFlags(cfg *Config) *Config {
  flag.IntVar(&cfg.CommitSecs, "pg-commit-secs", 15, "Write data to database every N seconds or N Rows")
  flag.IntVar(&cfg.CommitRows, "pg-commit-rows", 20000, "Write data to database every N Rows or N Seconds")
  flag.IntVar(&cfg.PGWriters, "pg-threads", 1, "Writer DB threads to run 1-50")
  flag.StringVar(&cfg.PartitionScheme, "pg-partition", "daily", "daily or hourly partitions, default: daily")
  return cfg
}

var promSamples = list.New()
var QueueMutex    sync.Mutex
var vMetricIDMapMutex sync.Mutex
var vMetricIDMap  tMetricIDMap

// Threaded writer
type PGWriter struct {
  DB             *pgx.Conn
  id              int
  KeepRunning     bool
  Running         bool

  valueRows       [][]interface{}
  labelRows       [][]interface{}

  lastPartitionTS time.Time
}


// Run starts the client and listens for a shutdown call.
func (c *PGWriter) Run(tid int, commitSecs int, commitRows int, partitionScheme string) {
  c.id=tid
  period := commitSecs * 1000
  var err error
  var samples *model.Samples
  c.DB, err = pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
  if err != nil {
    log.Error("err", err)
    os.Exit(1)
  }
  if c.id == 0 {
    c.setupPgPrometheus()
  }
  log.Info(fmt.Sprintf("bgwriter%d",c.id),"Started")
  c.Running = true
  c.KeepRunning=true
  // Loop that runs forever
  for c.KeepRunning {

    samples = Pop()
    if samples != nil {
      for _, sample := range (*samples) {
        sMetric:=metricString(sample.Metric)
        ts := time.Unix(sample.Timestamp.Unix(), 0)
        milliseconds := sample.Timestamp.UnixNano() / 1000000
        if ts.Year() != c.lastPartitionTS.Year() ||
           ts.Month() != c.lastPartitionTS.Month() ||
           ts.Day() != c.lastPartitionTS.Day() {
          c.lastPartitionTS = ts
          _ = c.setupPgPartitions(partitionScheme)
        }
        vMetricIDMapMutex.Lock()
        id, ok := vMetricIDMap[sMetric]

        if !ok {
          var nextId int64 = int64(len(vMetricIDMap) + 1)
          vMetricIDMap[sMetric]=nextId
          i := strings.Index(sMetric, "{")
          jsonbMap := make(map[string]interface{})
          json.Unmarshal([]byte( sMetric[i:] ), &jsonbMap)
          c.labelRows = append(c.labelRows, []interface{}{int64(nextId), sMetric[:i], sMetric, jsonbMap })

          id = nextId
        }
        vMetricIDMapMutex.Unlock()
        c.valueRows = append(c.valueRows, []interface{}{int64(id), toTimestamp(milliseconds), float64( sample.Value) } )
      }
      samples = nil
      log.Info(fmt.Sprintf("bgwriter%d",c.id), fmt.Sprintf("Parsed %d rows", len(c.valueRows) ) )
      runtime.GC()
    }
    if ( period <= 0 && len(c.valueRows) > 0 ) || (len(c.valueRows) > commitRows ) {
      c.Action()
      period = commitSecs * 1000
    }else {
      time.Sleep( 10 * time.Millisecond)
      period -= 10
    }
  }
  c.Action()
  log.Info(fmt.Sprintf("bgwriter%d",c.id),"Shutdown")
  c.Running = false
}

// Shutdown is a graceful shutdown mechanism 
func (c *PGWriter) Shutdown() {
  c.KeepRunning = false
}

// Action defines what the client does; override this. 
// For now we'll just wait two seconds and print to simulate work.
func (c *PGWriter) Action() {
  var copyCount, lblCount, rowCount int64
  var err error
  begin := time.Now()
  lblCount = int64(len(c.labelRows))
  if lblCount > 0 {
    copyCount, err := c.DB.CopyFrom(context.Background(), pgx.Identifier{"metric_labels"}, []string{"metric_id", "metric_name", "metric_name_label", "metric_labels"}, pgx.CopyFromRows(c.labelRows))
    c.labelRows = nil
    if err != nil {
      log.Error("msg", "COPY failed for metric_labels", "err", err)
    }
    if copyCount != lblCount {
      log.Error("msg", "All rows not copied metric_labels", "err", err)
    }
  }
  copyCount, err = c.DB.CopyFrom(context.Background(), pgx.Identifier{"metric_values"}, []string{"metric_id", "metric_time", "metric_value"}, pgx.CopyFromRows(c.valueRows))
  rowCount = int64(len(c.valueRows))
  c.valueRows = nil
  if err != nil {
    log.Error("msg", "COPY failed for metric_values", "err", err)
  }
  if copyCount != rowCount {
    log.Error("msg", "All rows not copied metric_values", "err", err)
  }
  duration := time.Since(begin).Seconds()
  log.Info("metric", fmt.Sprintf("BGWriter%d: Processed samples count,%d, duration,%v", c.id, rowCount + lblCount, duration) )
}

func Push(samples *model.Samples) {
  QueueMutex.Lock()
  promSamples.PushBack(samples)
  QueueMutex.Unlock()
}

func Pop() *model.Samples {
  QueueMutex.Lock()
  defer QueueMutex.Unlock()
  p := promSamples.Front()
  if ( p != nil ) {
    return promSamples.Remove(p).(*model.Samples)
  }
  return nil
}

// Threaded writer
type Client struct {
  DB              *pgx.Conn
  cfg             *Config
}


// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config) *Client {

  conn1, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
  if err != nil {
    log.Error("err", err)
    os.Exit(1)
  }

  client := &Client{
    DB:  conn1,
    cfg: cfg,
  }

  return client
}

func (c *PGWriter) setupPgPrometheus() error {
  log.Info("msg", "creating tables")

  _, err := c.DB.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS metric_labels ( metric_id BIGINT PRIMARY KEY, metric_name TEXT NOT NULL, metric_name_label TEXT NOT NULL, metric_labels jsonb, UNIQUE(metric_name, metric_labels) )")
  if err != nil {
    return err
  }

  _, err = c.DB.Exec(context.Background(), "CREATE INDEX IF NOT EXISTS metric_labels_labels_idx ON metric_labels USING GIN (metric_labels)")
  if err != nil {
    return err
  }

  _, err = c.DB.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS metric_values (metric_id BIGINT, metric_time TIMESTAMPTZ, metric_value FLOAT8 ) PARTITION BY RANGE (metric_time)")
  if err != nil {
    return err
  }

  _, err = c.DB.Exec(context.Background(), "CREATE INDEX IF NOT EXISTS metric_values_id_time_idx on metric_values USING btree (metric_id, metric_time DESC)")
  if err != nil {
    return err
  }

  _, err = c.DB.Exec(context.Background(), "CREATE INDEX IF NOT EXISTS metric_values_time_idx  on metric_values USING btree (metric_time DESC)")
  if err != nil {
    return err
  }

  vMetricIDMapMutex.Lock()
  defer vMetricIDMapMutex.Unlock()
  vMetricIDMap = make(tMetricIDMap)
  rows, err1 := c.DB.Query(context.Background(), "SELECT metric_name_label, metric_id from metric_labels" )

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
    vMetricIDMap[metric_name_label]=metric_id
  }
  log.Info("msg",fmt.Sprintf("%d Rows Loaded in map: ", len(vMetricIDMap ) ) )
  rows.Close()

  return nil
}

func (c *PGWriter) setupPgPartitions(partitionScheme string) error {
  sDate := c.lastPartitionTS
  eDate := sDate
  if partitionScheme == "daily" {
    log.Info("msg","Creating partition, daily")
    _, err := c.DB.Exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_values_%s PARTITION OF metric_values FOR VALUES FROM ('%s 00:00:00') TO ('%s 00:00:00')", sDate.Format("20060102"), sDate.Format("2006-01-02"), eDate.AddDate(0, 0, 1).Format("2006-01-02") ) )
    if err != nil {
      return err
    }
  } else if partitionScheme == "hourly" {
    sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_values_%s PARTITION OF metric_values FOR VALUES FROM ('%s 00:00:00') TO ('%s 00:00:00') PARTITION BY RANGE (metric_time);", sDate.Format("20060102"), sDate.Format("2006-01-02"), eDate.AddDate(0, 0, 1).Format("2006-01-02") )
    var h int
    for h=0; h<23; h++ {
      sql = fmt.Sprintf("%s CREATE TABLE IF NOT EXISTS metric_values_%s_%02d PARTITION OF metric_values_%s FOR VALUES FROM ('%s %02d:00:00') TO ('%s %02d:00:00');", sql, sDate.Format("20060102"), h, sDate.Format("20060102"), sDate.Format("2006-01-02"), h, eDate.Format("2006-01-02"), h+1 )
    }
    log.Info("msg","Creating partition, hourly")
    _, err := c.DB.Exec(context.Background(), fmt.Sprintf("%s CREATE TABLE IF NOT EXISTS metric_values_%s_%02d PARTITION OF metric_values_%s FOR VALUES FROM ('%s %02d:00:00') TO ('%s 00:00:00');", sql, sDate.Format("20060102"), h, sDate.Format("20060102"), sDate.Format("2006-01-02"), h, eDate.AddDate(0, 0, 1).Format("2006-01-02") ) )
    if err != nil {
      return err
    }
  }
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
  Push(&samples)
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
  if c.DB != nil {
    if err1 := c.DB.Close(context.Background()); err1 != nil {
      log.Error("msg", err1.Error())
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

    rows, err := c.DB.Query(context.Background(), command)

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
  rows, err := c.DB.Query(context.Background(), "SELECT 1")
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

