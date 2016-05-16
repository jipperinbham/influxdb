package replicator

// Config represents a configuration for a replicator service.
type Config struct {
	Enabled          bool     `toml:"enabled"`
	LogEnabled       bool     `toml:"log-enabled"`
	Brokers          []string `toml:"brokers"`
	ConsensusServers []string `toml:"consensus_servers"`
	ReplSetName      string   `toml:"repl_set_name"`
	NodeID           string   `toml:"node_id"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		Enabled:    true,
		LogEnabled: true,
	}
}
