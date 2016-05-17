package replicator

import (
	"bytes"
	"expvar"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cluster"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	sarama "gopkg.in/Shopify/sarama.v1"
	sarama_cluster "gopkg.in/bsm/sarama-cluster.v2"
	etcd "gopkg.in/coreos/etcd.v2/client"
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	err              chan error
	brokers          []string
	consensusServers []string
	replSetName      string
	nodeID           string

	replicationWriter sarama.SyncProducer
	replicationReader *sarama_cluster.Consumer
	schemaReader      *sarama_cluster.Consumer
	consensus         etcd.KeysAPI
	points            chan *cluster.WritePointsRequest
	wg                sync.WaitGroup
	closed            bool
	closing           chan struct{}
	halted            bool
	haltSync          chan struct{}
	mu                sync.Mutex

	query chan string

	currentState string

	Logger  *log.Logger
	statMap *expvar.Map

	QueryExecutor *influxql.QueryExecutor

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	s := &Service{
		err:              make(chan error),
		brokers:          c.Brokers,
		consensusServers: c.ConsensusServers,
		replSetName:      c.ReplSetName,
		nodeID:           c.NodeID,
		points:           make(chan *cluster.WritePointsRequest),
		query:            make(chan string),
		Logger:           log.New(os.Stderr, "[replicator] ", log.LstdFlags),
		statMap:          influxdb.NewStatistics("replicator", "replicator", nil),
		closed:           true,
		closing:          make(chan struct{}),
		halted:           true,
		haltSync:         make(chan struct{}),
	}
	producer, err := s.createWriter()
	if err != nil {
		panic(err)
	}
	s.replicationWriter = producer

	reader, err := s.createReader()
	if err != nil {
		panic(err)
	}
	s.replicationReader = reader

	schemaReader, err := s.createSchemaReader()
	if err != nil {
		panic(err)
	}
	s.schemaReader = schemaReader

	consensus, err := s.createConsensus()
	if err != nil {
		panic(err)
	}
	s.consensus = consensus

	return s
}

// Open starts the service
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Logger.Println("Starting replicator service")

	s.closing = make(chan struct{})
	s.closed = false
	s.haltSync = make(chan struct{})
	s.halted = false

	s.wg.Add(2)
	s.manageState()
	go s.readQueries()
	go s.writePoints()
	s.Logger.Println("opened service")
	return nil
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	s.mu.Lock()
	close(s.points)
	close(s.query)
	s.closed = true
	select {
	case <-s.closing:
		// do nothing
	default:
		close(s.closing)
	}

	go func() {
		s.halted = true
		select {
		case <-s.haltSync:
			// do nothing
		case <-time.After(time.Second * 1):
			close(s.haltSync)
			return
		default:
			close(s.haltSync)
		}
	}()
	defer s.mu.Unlock()

	s.wg.Wait()
	if err := s.replicationWriter.Close(); err != nil {
		s.Logger.Println("Failed to shut down replication writer cleanly", err)
		return err
	}
	if err := s.replicationReader.Close(); err != nil {
		s.Logger.Println("Failed to shut down replication reader cleanly", err)
		return err
	}
	s.Logger.Println("closed service")
	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

func (s *Service) createWriter() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10

	producer, err := sarama.NewSyncProducer(s.brokers, config)
	if err != nil {
		return nil, err
	}
	return producer, err
}

func (s *Service) createReader() (*sarama_cluster.Consumer, error) {
	config := sarama_cluster.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	consumer, err := sarama_cluster.NewConsumer(
		s.brokers,
		s.nodeID,
		[]string{"influxdb_points"},
		config,
	)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func (s *Service) createSchemaReader() (*sarama_cluster.Consumer, error) {
	config := sarama_cluster.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	consumer, err := sarama_cluster.NewConsumer(
		s.brokers,
		s.nodeID,
		[]string{"influxdb_schema"},
		config,
	)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func (s *Service) createConsensus() (etcd.KeysAPI, error) {
	cfg := etcd.Config{
		Endpoints: s.consensusServers,
		Transport: etcd.DefaultTransport,
	}

	c, err := etcd.New(cfg)
	if err != nil {
		return nil, err
	}
	return etcd.NewKeysAPI(c), nil
}

// Points returns a channel into which write point requests can be sent.
func (s *Service) Points() chan<- *cluster.WritePointsRequest {
	return s.points
}

// Query returns a channel into which a query can be sent.
func (s *Service) Query() chan<- string {
	return s.query
}

func (s *Service) manageState() {
	s.wg.Add(1)
	go s.raceToBeLeader()
}

func (s *Service) raceToBeLeader() {
	defer s.wg.Done()
	tickChan := time.NewTicker(time.Second * 5).C

	for {
		select {
		case <-tickChan:
			s.Logger.Println("leader check")
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				s.Logger.Println("service closed, exiting")
				return
			}
			s.mu.Unlock()
			resp, err := s.consensus.Set(
				context.Background(),
				fmt.Sprintf("/%s/leader", s.replSetName),
				s.nodeID,
				&etcd.SetOptions{
					PrevValue: s.nodeID,
					TTL:       time.Second * 30,
				},
			)
			if err != nil && isLeaderNotMe(err) {
				if s.currentState != "SECONDARY" {
					s.enterStartup()
					s.wg.Add(2)
					go s.staySynced()
					go s.processQueries()
				}
			} else if err != nil && etcd.IsKeyNotFound(err) {
				log.Println("no leader set")
				_, err2 := s.consensus.Set(
					context.Background(),
					fmt.Sprintf("/%s/leader", s.replSetName),
					s.nodeID,
					&etcd.SetOptions{
						PrevExist: etcd.PrevNoExist,
						TTL:       time.Second * 30,
					},
				)
				if err2 != nil && isLeaderAlreadyTaken(err) {
					s.enterStartup()
					s.wg.Add(2)
					go s.staySynced()
					go s.processQueries()
				} else {
					s.setState("PRIMARY")
					// kill sync if running
					log.Println("halting sync")
					go func() {
						s.halted = true
						select {
						case <-s.haltSync:
							// do nothing
						default:
							close(s.haltSync)
						}
					}()
					s.Logger.Println("sync halted!!!")
					s.haltSync = make(chan struct{})
				}
			} else if resp.Node.Value == s.nodeID {
				s.setState("PRIMARY")
			} else if err != nil {
				fmt.Printf("leader error, %v\n", err)
			}
			s.Logger.Printf("current state (%s) for node %s\n", s.currentState, s.nodeID)
		case <-s.closing:
			s.Logger.Println("closing")
			return
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (s *Service) enterStartup() {
	s.Logger.Printf("transitioning to STARTUP for %s\n", s.nodeID)
	s.setState("STARTUP")
	for {
		select {
		case msg := <-s.replicationReader.Messages():
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				s.Logger.Println("service closed not going to consume any more messages")
				return
			}
			s.mu.Unlock()
			db, rp, point, err := scanDatabaseAndRetention(msg.Value, 0)
			if err != nil {
				s.Logger.Fatalf("failed to parse message: %s\n", err)
			}
			s.Logger.Printf("database and retention: %s %s\n", string(db), string(rp))
			pts, err := models.ParsePoints(point)
			if err != nil {
				s.Logger.Fatalf("failed to parse points: %s\n", err)
			}
			s.Logger.Printf("point: %s\n", pts[0].String())
			if err := s.PointsWriter.WritePoints(string(db), string(rp), models.ConsistencyLevelOne, pts); influxdb.IsClientError(err) {
				s.Logger.Fatalln("error writing point", err)
				return
			}
			s.Logger.Printf("Consumed message offset %d\n", msg.Offset)
			s.replicationReader.MarkOffset(msg, "")
		case <-s.closing:
			s.Logger.Println("closing")
			return
		case <-time.After(time.Second * 1):
			s.Logger.Printf("no messages queued, converting to SECONDARY\n")
			s.setState("SECONDARY")
			return
		}
	}
}

func (s *Service) readQueries() {
	defer s.wg.Done()

	for {
		select {
		case q := <-s.query:
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				fmt.Println("service closed not going to consume any more queries")
				return
			}
			s.mu.Unlock()
			db, stmt, err := scanDatabase([]byte(q), 0)
			s.Logger.Println("received statement:", stmt)
			partition, offset, err := s.replicationWriter.SendMessage(&sarama.ProducerMessage{
				Topic: "influxdb_schema",
				Value: sarama.ByteEncoder(fmt.Sprintf("%s %s", db, stmt)),
			})
			if err != nil {
				// s.statMap.Add(statWriteFailures, 1)
				return
			}
			s.Logger.Printf("statement sent to %d with offset %d\n", partition, offset)
			s.schemaReader.MarkOffset(
				&sarama.ConsumerMessage{
					Offset:    offset,
					Topic:     "influxdb_schema",
					Partition: partition,
				},
				"",
			)
			s.Logger.Printf("offset %d committed\n", offset)
		case <-s.closing:
			return
		}
	}
}

func (s *Service) processQueries() {
	defer s.wg.Done()

	for {
		select {
		case <-s.haltSync:
			s.halted = false
			s.Logger.Println("halting query processor")
			return
		case msg := <-s.schemaReader.Messages():
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				s.Logger.Println("service closed not going to consume any more queries")
				return
			}
			s.mu.Unlock()
			s.Logger.Println("received query:", string(msg.Value))
			db, q, err := scanDatabase(msg.Value, 0)
			if err != nil {
				s.Logger.Fatalln("failed scanDatabase", err)
				return
			}
			parser := influxql.NewParser(bytes.NewReader(q))
			query, err := parser.ParseQuery()
			if err != nil {
				s.Logger.Fatalln("failed ParseQuery", err)
				return
			}
			s.QueryExecutor.ExecuteQuery(query, string(db), 10000, false, s.closing)
			s.schemaReader.MarkOffset(msg, "")
			log.Printf("Marked schema offset %d for %s\n", msg.Offset, query)
		}
	}
}

func (s *Service) writePoints() {
	defer s.wg.Done()

	for p := range s.points {
		s.mu.Lock()
		if s.currentState != "PRIMARY" {
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()
		for _, point := range p.Points {
			s.Logger.Println(point)
			partition, offset, err := s.replicationWriter.SendMessage(&sarama.ProducerMessage{
				Topic: "influxdb_points",
				Value: sarama.ByteEncoder(fmt.Sprintf("%s %s %s", p.Database, p.RetentionPolicy, point.String())),
			})
			if err != nil {
				// s.statMap.Add(statWriteFailures, 1)
				return
			}
			s.replicationReader.MarkOffset(
				&sarama.ConsumerMessage{
					Offset:    offset,
					Topic:     "influxdb_points",
					Partition: partition,
				},
				"",
			)
		}
		// s.statMap.Add(statPointsWritten, int64(len(p.Points)))
	}
}

func (s *Service) staySynced() {
	defer s.wg.Done()

	for {
		select {
		case msg := <-s.replicationReader.Messages():
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				fmt.Println("service closed not going to consume any more messages")
				return
			}
			s.mu.Unlock()
			db, rp, point, err := scanDatabaseAndRetention(msg.Value, 0)
			if err != nil {
				log.Fatalf("failed to parse message: %s\n", err)
			}
			log.Printf("database and retention: %s %s\n", string(db), string(rp))
			pts, err := models.ParsePoints(point)
			if err != nil {
				log.Fatalf("failed to parse points: %s\n", err)
			}
			log.Printf("point: %s\n", pts[0].String())
			if err := s.PointsWriter.WritePoints(string(db), string(rp), models.ConsistencyLevelOne, pts); influxdb.IsClientError(err) {
				s.Logger.Fatalln("error writing point", err)
				return
			}
			log.Printf("Consumed message offset %d\n", msg.Offset)
			s.replicationReader.MarkOffset(msg, "")
		case <-s.haltSync:
			s.halted = false
			s.Logger.Println("halting sync")
			return
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func scanDatabase(buf []byte, start int) ([]byte, []byte, error) {
	i := start
	for {
		// reached the end of buf?
		if i >= len(buf) {
			return buf[start:i], buf[i:], fmt.Errorf("malformed message")
		}
		// reached end of block?
		if buf[i] == ' ' {
			break
		}
		i++
	}
	return buf[start:i], buf[i:], nil
}

func scanDatabaseAndRetention(buf []byte, start int) ([]byte, []byte, []byte, error) {
	foundDatabase := false
	i := start
	var dbIndex int
	for {
		// reached the end of buf?
		if i >= len(buf) {
			return buf[start:i], buf[start:i], buf[i:], fmt.Errorf("malformed message")
		}
		// reached end of block?
		if buf[i] == ' ' {
			if foundDatabase {
				break
			}
			dbIndex = i
			foundDatabase = true
		}
		i++
	}
	return buf[start:dbIndex], buf[dbIndex+1 : i], buf[i:], nil
}

func (s *Service) setState(state string) error {
	resp, err := s.consensus.Set(
		context.Background(),
		fmt.Sprintf("/%s/nodes/%s/state", s.replSetName, s.nodeID),
		state,
		nil,
	)
	if err != nil {
		return err
	}
	s.currentState = resp.Node.Value
	return nil
}

func isLeaderAlreadyTaken(err error) bool {
	if cErr, ok := err.(etcd.Error); ok {
		return cErr.Code == etcd.ErrorCodeNodeExist
	}
	return false
}

func isLeaderNotMe(err error) bool {
	if cErr, ok := err.(etcd.Error); ok {
		return cErr.Code == etcd.ErrorCodeTestFailed
	}
	return false
}
