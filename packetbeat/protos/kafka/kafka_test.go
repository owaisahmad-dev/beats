// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build !integration
// +build !integration

package kafka

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/packetbeat/procs"
	"github.com/elastic/beats/v7/packetbeat/publish"
)

type eventStore struct {
	events []beat.Event
}

func (e *eventStore) publish(event beat.Event) {
	publish.MarshalPacketbeatFields(&event, nil, nil)
	e.events = append(e.events, event)
}

func kafkaModForTests(store *eventStore) *kafkaPlugin {
	callback := func(beat.Event) {}
	if store != nil {
		callback = store.publish
	}

	var kafka kafkaPlugin
	config := defaultConfig
	kafka.init(callback, procs.ProcessesWatcher{}, &config)
	return &kafka
}

func TestKafkaParser_Produce_v7(t *testing.T) {
	kafka := kafkaModForTests(nil)

	data := []byte(
		"00000079000000070000007100077264" +
			"6b61666b61ffffffff00007530000000" +
			"01000474657374000000010000000000" +
			"00004a00000000000000000000003e00" +
			"00000002d801bd270000000000000000" +
			"0183845fb6bb00000183845fb6bbffff" +
			"ffffffffffffffffffffffff00000001" +
			"18000000010c0008776f6f6600")

	message, err := hex.DecodeString(string(data))
	if err != nil {
		t.Error("Failed to decode hex string")
	}

	stream := &kafkaStream{data: message, message: new(kafkaMessage), isClient: true}

	ok, complete := kafka.kafkaMessageParser(stream)

	if !ok {
		t.Error("Parsing returned error")
	}
	if !complete {
		t.Error("Expecting a complete message")
	}
	if stream.message.topics[0] != "test" {
		t.Error("Failed to parse topic")
	}

	if stream.message.messages[0][2:] != "woof" {
		t.Error("Failed to parse message")
	}

	if stream.message.size != 121 {
		t.Errorf("Wrong message size %d", stream.message.size)
	}

	data = []byte(
		"00000034000000710000000100047465" +
			"73740000000100000000000000000000" +
			"00000080ffffffffffffffff00000000" +
			"0000000000000000")

	message, err = hex.DecodeString(string(data))
	if err != nil {
		t.Error("Failed to decode hex string")
	}

	stream = &kafkaStream{data: message, message: new(kafkaMessage), isClient: false}

	ok, complete = kafka.kafkaMessageParser(stream)

	if !ok {
		t.Error("Parsing returned error")
	}
	if !complete {
		t.Error("Expecting a complete message")
	}
}

func TestKafkaParser_Fetch_v11(t *testing.T) {
	kafka := kafkaModForTests(nil)

	data := []byte(
		"0000005a0001000b0000000200077264" +
			"6b61666b61ffffffff000001f4000000" +
			"01032000000100000000ffffffff0000" +
			"00010004746573740000000100000000" +
			"ffffffff0000000000000074ffffffff" +
			"ffffffff00100000000000000000")

	message, err := hex.DecodeString(string(data))
	if err != nil {
		t.Error("Failed to decode hex string")
	}

	stream := &kafkaStream{data: message, message: new(kafkaMessage), isClient: true}

	ok, complete := kafka.kafkaMessageParser(stream)

	if !ok {
		t.Error("Parsing returned error")
	}
	if !complete {
		t.Error("Expecting a complete message")
	}
	if stream.message.topics[0] != "test" {
		t.Error("Failed to parse topic")
	}

	if stream.message.size != 90 {
		t.Errorf("Wrong message size %d", stream.message.size)
	}

	data = []byte(
		"000005c4000000020000000000000000" +
			"00000000000100047465737400000001" +
			"00000000000000000000000000870000" +
			"00000000008700000000000000000000" +
			"0000ffffffff0000057e000000000000" +
			"00740000003e000000000224f03d3b00" +
			"000000000000000183845f2a13000001" +
			"83845f2a13ffffffffffffffffffffff" +
			"ffffff0000000118000000010c020870" +
			"7572720000000000000000750000003e" +
			"0000000002a977504f00000000000000" +
			"000183845f35cc00000183845f35ccff" +
			"ffffffffffffffffffffffffff000000" +
			"0118000000010c0008776f6f66000000" +
			"0000000000760000003e00000000021b" +
			"1c6ab600000000000000000183845f41" +
			"8400000183845f4184ffffffffffffff" +
			"ffffffffffffff000000011800000001" +
			"0c00086261726b000000000000000077" +
			"0000003e000000000276aa7852000000" +
			"00000000000183845f4d3c0000018384" +
			"5f4d3cffffffffffffffffffffffffff" +
			"ff0000000118000000010c0008626172" +
			"6b0000000000000000780000003e0000" +
			"0000029b9f25b2000000000000000001" +
			"83845f58f500000183845f58f5ffffff" +
			"ffffffffffffffffffffff0000000118" +
			"000000010c00086261726b0000000000" +
			"000000790000003e0000000002e6a9ba" +
			"8300000000000000000183845f64af00" +
			"000183845f64afffffffffffffffffff" +
			"ffffffffff0000000118000000010c00" +
			"08776f6f6600000000000000007a0000" +
			"003e0000000002b87137c90000000000" +
			"0000000183845f706800000183845f70" +
			"68ffffffffffffffffffffffffffff00" +
			"00000118000000010c0008776f6f6600" +
			"000000000000007b0000003e00000000" +
			"02f12db0e60000000000000000018384" +
			"5f7c2000000183845f7c20ffffffffff" +
			"ffffffffffffffffff00000001180000" +
			"00010c0008776f6f6600000000000000" +
			"007c0000003e000000000265edb04c00" +
			"000000000000000183845f87d8000001" +
			"83845f87d8ffffffffffffffffffffff" +
			"ffffff0000000118000000010c000862" +
			"61726b00000000000000007d0000003e" +
			"0000000002c9e1656900000000000000" +
			"000183845f939100000183845f9391ff" +
			"ffffffffffffffffffffffffff000000" +
			"0118000000010c020870757272000000" +
			"00000000007e0000003e00000000026b" +
			"77085b00000000000000000183845f9f" +
			"4a00000183845f9f4affffffffffffff" +
			"ffffffffffffff000000011800000001" +
			"0c02086d656f7700000000000000007f" +
			"0000003e000000000262138b5a000000" +
			"00000000000183845fab020000018384" +
			"5fab02ffffffffffffffffffffffffff" +
			"ff0000000118000000010c0208707572" +
			"720000000000000000800000003e0000" +
			"000002d801bd27000000000000000001" +
			"83845fb6bb00000183845fb6bbffffff" +
			"ffffffffffffffffffffff0000000118" +
			"000000010c0008776f6f660000000000" +
			"000000810000003e00000000029db247" +
			"2700000000000000000183845fc27300" +
			"000183845fc273ffffffffffffffffff" +
			"ffffffffff0000000118000000010c02" +
			"086d656f770000000000000000820000" +
			"003e00000000024b60abe40000000000" +
			"0000000183845fce2c00000183845fce" +
			"2cffffffffffffffffffffffffffff00" +
			"00000118000000010c02086d656f7700" +
			"00000000000000830000003e00000000" +
			"023d1a56330000000000000000018384" +
			"5fd9e500000183845fd9e5ffffffffff" +
			"ffffffffffffffffff00000001180000" +
			"00010c00086261726b00000000000000" +
			"00840000003e000000000245a276a500" +
			"000000000000000183845fe59e000001" +
			"83845fe59effffffffffffffffffffff" +
			"ffffff0000000118000000010c02086d" +
			"656f770000000000000000850000003e" +
			"00000000024b47e59a00000000000000" +
			"000183845ff15700000183845ff157ff" +
			"ffffffffffffffffffffffffff000000" +
			"0118000000010c02086d656f77000000" +
			"0000000000860000003e0000000002b2" +
			"40b92000000000000000000183845ffd" +
			"1000000183845ffd10ffffffffffffff" +
			"ffffffffffffff000000011800000001" +
			"0c02086d656f7700")

	message, err = hex.DecodeString(string(data))
	if err != nil {
		t.Error("Failed to decode hex string")
	}

	stream = &kafkaStream{data: message, message: new(kafkaMessage), isClient: false}

	ok, complete = kafka.kafkaMessageParser(stream)

	if !ok {
		t.Error("Parsing returned error")
	}
	if !complete {
		t.Error("Expecting a complete message")
	}

	fmt.Println(stream.message.messages)
}

func TestKafkaParser_Produce_v5(t *testing.T) {
	kafka := kafkaModForTests(nil)

	data := []byte(
		"0000008c000000050000000300077264" +
			"6b61666b61ffffffff00007530000000" +
			"01000474657374000000010000000000" +
			"00005d00000000000000000000005100" +
			"00000002a73a8c1f0000000000000000" +
			"01840b55007d000001840b55007dffff" +
			"ffffffffffffffffffffffff00000001" +
			"3e000000013248656c6c6f20576f726c" +
			"643f20486f772061726520796f753f00")

	message, err := hex.DecodeString(string(data))
	if err != nil {
		t.Error("Failed to decode hex string")
	}

	stream := &kafkaStream{data: message, message: new(kafkaMessage), isClient: true}

	ok, complete := kafka.kafkaMessageParser(stream)

	if !ok {
		t.Error("Parsing returned error")
	}
	if !complete {
		t.Error("Expecting a complete message")
	}
	if stream.message.topics[0] != "test" {
		t.Error("Failed to parse topic")
	}

	if stream.message.messages[0] != "Hello World? How are you?" {
		t.Error("Failed to parse message")
	}

	if stream.message.size != 140 {
		t.Errorf("Wrong message size %d", stream.message.size)
	}

	data = []byte(
		"00000034000000030000000100047465" +
			"73740000000100000000000000000000" +
			"00000000ffffffffffffffff00000000" +
			"0000000000000000")

	message, err = hex.DecodeString(string(data))
	if err != nil {
		t.Error("Failed to decode hex string")
	}

	stream = &kafkaStream{data: message, message: new(kafkaMessage), isClient: false}

	ok, complete = kafka.kafkaMessageParser(stream)

	if !ok {
		t.Error("Parsing returned error")
	}
	if !complete {
		t.Error("Expecting a complete message")
	}
}

func TestKafkaParser_Fetch_v7(t *testing.T) {
	kafka := kafkaModForTests(nil)

	data := []byte(
		"00000054000100070000000300077264" +
			"6b61666b61ffffffff000001f4000000" +
			"01032000000100000000ffffffff0000" +
			"00010004746573740000000100000000" +
			"0000000000000002ffffffffffffffff" +
			"0010000000000000")

	message, err := hex.DecodeString(string(data))
	if err != nil {
		t.Error("Failed to decode hex string")
	}

	stream := &kafkaStream{data: message, message: new(kafkaMessage), isClient: true}

	ok, complete := kafka.kafkaMessageParser(stream)

	if !ok {
		t.Error("Parsing returned error")
	}
	if !complete {
		t.Error("Expecting a complete message")
	}
	if stream.message.topics[0] != "test" {
		t.Error("Failed to parse topic")
	}

	if stream.message.size != 84 {
		t.Errorf("Wrong message size %d", stream.message.size)
	}

	data = []byte(
		"00000159000000030000000000000000" +
			"00000000000100047465737400000001" +
			"00000000000000000000000000050000" +
			"00000000000500000000000000000000" +
			"00000000011700000000000000020000" +
			"00510000000002a904a8800000000000" +
			"00000001840b6245d9000001840b6245" +
			"d9ffffffffffffffffffffffffffff00" +
			"0000013e000000013248656c6c6f2057" +
			"6f726c643f20486f772061726520796f" +
			"753f0000000000000000030000005100" +
			"000000021391e9880000000000000000" +
			"01840b626341000001840b626341ffff" +
			"ffffffffffffffffffffffff00000001" +
			"3e000000013248656c6c6f20576f726c" +
			"643f20486f772061726520796f753f00" +
			"00000000000000040000005100000000" +
			"02d9ad7a5a000000000000000001840b" +
			"62c131000001840b62c131ffffffffff" +
			"ffffffffffffffffff000000013e0000" +
			"00013248656c6c6f20576f726c643f20" +
			"486f772061726520796f753f00")

	message, err = hex.DecodeString(string(data))
	if err != nil {
		t.Error("Failed to decode hex string")
	}

	stream = &kafkaStream{data: message, message: new(kafkaMessage), isClient: false}

	ok, complete = kafka.kafkaMessageParser(stream)

	if !ok {
		t.Error("Parsing returned error")
	}
	if !complete {
		t.Error("Expecting a complete message")
	}

	fmt.Println(stream.message.messages)
}
