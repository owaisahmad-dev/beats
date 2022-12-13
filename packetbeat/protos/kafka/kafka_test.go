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

func TestKafkaParser_Produce_v3(t *testing.T) {
	kafka := kafkaModForTests(nil)

	data := []byte(
		"0000008c000000030000000300077264" +
			"6b61666b61ffffffff00007530000000" +
			"01000474657374000000010000000000" +
			"00005d00000000000000000000005100" +
			"00000002283b9cfc0000000000000000" +
			"01844407e36a000001844407e36affff" +
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
		"0000002c000000030000000100047465" +
			"73740000000100000000000000000000" +
			"00000001ffffffffffffffff00000000")

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

func TestKafkaParser_Produce_v9(t *testing.T) {
	kafka := kafkaModForTests(nil)

	data := []byte(
		"0000007a00000009000000070010636f" +
			"6e736f6c652d70726f64756365720000" +
			"ffff000005dc02057465737402000000" +
			"004a00000000000000000000003dffff" +
			"ffff02a0f791ce000000000000000001" +
			"847b20276b000001847b20276b000000" +
			"00000000000000000000010000000116" +
			"000000010a68656c6c6f00000000")

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

	if stream.message.messages[0] != "hello" {
		t.Error("Failed to parse message")
	}

	if stream.message.size != 122 {
		t.Errorf("Wrong message size %d", stream.message.size)
	}

	data = []byte(
		"00000033000000070002057465737402" +
			"0000000000000000000000000001ffff" +
			"ffffffffffff00000000000000000100" +
			"00000000000000")

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

func TestKafkaParser_Fetch_v13(t *testing.T) {
	kafka := kafkaModForTests(nil)

	data := []byte(
		"0000006b0001000d0000000a0010636f" +
			"6e736f6c652d636f6e73756d657200ff" +
			"ffffff000001f4000000010320000000" +
			"0000000000000000029f667c6296b446" +
			"f994bf89ec0c0c512a02000000000000" +
			"00000000000000000000ffffffffffff" +
			"ffffffffffff001000000000010100")

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
	if stream.message.topics[0] != "9f667c62-96b4-46f9-94bf-89ec0c0c512a" {
		t.Error("Failed to parse topic")
	}

	if stream.message.size != 107 {
		t.Errorf("Wrong message size %d", stream.message.size)
	}

	data = []byte(
		"000001420000000a000000000000004b" +
			"d21dfa029f667c6296b446f994bf89ec" +
			"0c0c512a020000000000000000000000" +
			"00000300000000000000030000000000" +
			"00000000fffffffffa01000000000000" +
			"00000000003d0000000002376f3c0100" +
			"0000000000000001847ed4ec5a000001" +
			"847ed4ec5a0000000000000000000000" +
			"0000000000000116000000010a68656c" +
			"6c6f0000000000000000010000005200" +
			"0000000211b17ce30000000000000000" +
			"01847ee02638000001847ee026380000" +
			"00000000000000000000000100000001" +
			"40000000013468656c6c6f20776f726c" +
			"6420776173737375707070703f3f3f3f" +
			"00000000000000000200000046000000" +
			"0002212b877300000000000000000184" +
			"7ef08fb7000001847ef08fb700000000" +
			"00000001000000000000000000012800" +
			"0000011c594f4f4f4f20576173737570" +
			"707000000000")

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

func TestKafkaParser_Fetch_v2(t *testing.T) {
	kafka := kafkaModForTests(nil)

	data := []byte(
		"0000010e00010002000000010016636f" +
			"6e736f6c652d636f6e73756d65722d39" +
			"30373336ffffffff0000006400000001" +
			"0000000100086d792d746f7069630000" +
			"000d0000000500000000000000000010" +
			"00000000000000000000000000000010" +
			"00000000000700000000000000000010" +
			"00000000000400000000000000000010" +
			"00000000000c00000000000000000010" +
			"00000000000600000000000000000010" +
			"00000000000b00000000000000000010" +
			"00000000000a00000000000000000010" +
			"00000000000900000000000000000010" +
			"00000000000200000000000000000010" +
			"00000000000300000000000000000010" +
			"00000000000100000000000000000010" +
			"00000000000800000000000000000010" +
			"0000")

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
	if stream.message.topics[0] != "my-topic" {
		t.Error("Failed to parse topic")
	}

	if stream.message.size != 270 {
		t.Errorf("Wrong message size %d", stream.message.size)
	}

	data = []byte(
		"000001ac000000010000000000000001" +
			"00086d792d746f7069630000000d0000" +
			"00000000000000000000000000000000" +
			"00000005000000000000000000000000" +
			"00000000000a00000000000000000001" +
			"00000033000000000000000000000027" +
			"c8edfbb10100000001850a6132aeffff" +
			"ffff00000011796f207365636f6e6420" +
			"617474656d7074000000010000000000" +
			"00000000010000002600000000000000" +
			"000000001a814048fd0100000001850a" +
			"6330d0ffffffff000000047774686800" +
			"00000600000000000000000000000000" +
			"00000000090000000000000000000000" +
			"00000000000002000000000000000000" +
			"00000000000000000c00000000000000" +
			"00000000000000000000070000000000" +
			"00000000010000002800000000000000" +
			"000000001c2b1da1cd0100000001850a" +
			"5f48fcffffffff000000065774686868" +
			"68000000030000000000000000000000" +
			"0000000000000b000000000000000000" +
			"00000000000000000800000000000000" +
			"00000000000000000000040000000000" +
			"00000000010000002700000000000000" +
			"000000001b780ae5f00100000001850a" +
			"5ec602ffffffff0000000548656c6c6f")

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
