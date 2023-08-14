package kafka

import (
	"github.com/elastic/beats/v7/packetbeat/config"
)

type kafkaConfig struct {
	config.ProtocolCommon `config:",inline"`
}

var defaultConfig = kafkaConfig{}
