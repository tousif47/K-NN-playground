package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

type FlowLog struct {
	SerialNo string  `json:"serial_no"`
	Ts       int64   `json:"ts"`
	RSSI     float32 `json:"rssi"`
	SNR      float32 `json:"snr"`
	Codes    []byte  `json:"codes"`
}

// FlowLogCode struct
type FlowLogCode struct {
	Value     uint16          `json:"value,omitempty"`
	Type      FlowLogCodeType `json:"type,omitempty"`
	Ts        int64           `json:"ts,omitempty"`
	TempGroup *uint16         `json:"temp,omitempty"`
}

// TemperatureCode implements FlowerPointer interface
func (fc *FlowLogCode) TemperatureCode() *uint16 {
	return fc.TempGroup
}

// CodeType implements FlowerPointer interface
func (fc *FlowLogCode) CodeType() *FlowLogCodeType {
	return &fc.Type
}

// Float implements FlowerPointer interface
func (fc *FlowLogCode) Float() *float64 {
	v := fc.toFloat()
	return &v
}

// Q2 implements FlowerPointer interface
func (fc *FlowLogCode) Q2() *uint16 {
	return &fc.Value
}

func (fc *FlowLogCode) toFloat() float64 {
	switch fc.Type {
	case 0:
		return float64(fc.Value) / 4.0
	default:
		return float64(fc.Value)
	}
}

// FlowLogCodeType opts codes
type FlowLogCodeType int

func (f FlowLogCodeType) String() string {
	switch int(f) {
	case 1, 2:
		return "absolute"
	case 3:
		return "pause"
	case 4:
		return "dt"
	case 5:
		return "temp"
	default:
		return "n/a"
	}
}

// Flow log code types
const (
	FlowAbsoluteCodeType FlowLogCodeType = 1
	FlowCodeType         FlowLogCodeType = 2
	PauseLengthCodeType  FlowLogCodeType = 3
	DeltaFlowLogCodeType FlowLogCodeType = 4
	TempGroupCodeType    FlowLogCodeType = 5
)

func DecompressFlowLogTimeSeries(codes []byte, timestamp *time.Time) ([]*FlowLogCode, error) {
	decompressedCodes, err := DecompressFlowLog(codes)
	if err != nil {
		return nil, err
	}

	var flowSeries []*FlowLogCode
	var tempGroup *uint16

	j := 0 // series counter
	ts := timestamp.Unix()
	for _, v := range decompressedCodes {
		switch v.Type {
		case PauseLengthCodeType:
			pause := v.Value
			for i := uint16(0); i < pause; i++ {
				flowSeries = append(flowSeries,
					&FlowLogCode{Ts: ts, Value: 0, TempGroup: tempGroup, Type: PauseLengthCodeType})
				ts++
				j++
			}

		case FlowCodeType, DeltaFlowLogCodeType:
			flowSeries = append(flowSeries,
				&FlowLogCode{Ts: ts, Value: v.Value, TempGroup: tempGroup, Type: v.Type})
			ts++
			j++
		case TempGroupCodeType:
			tempGroup = &v.Value
		default:
		}
	}
	return flowSeries, nil
}

// Code for flow log decompression
type Code struct {
	Value uint16
	Type  FlowLogCodeType
}

func DecompressFlowLog(codes []byte) ([]Code, error) {
	readingA16bit := false
	code16 := uint16(0)
	flow := uint16(0)
	var FlowLogCodes []Code

	for _, code := range codes {

		if readingA16bit {
			code16 += uint16(code)
			readingA16bit = false
			if code16 >= 0xF000 {
				pauseLength := code16 - 0xF000
				FlowLogCodes = append(FlowLogCodes, Code{
					Value: pauseLength,
					Type:  PauseLengthCodeType,
				})
				continue
			} else {
				flow = code16 - 0xE000 // flow from absolute readout
				FlowLogCodes = append(FlowLogCodes, Code{
					Type:  FlowCodeType,
					Value: code16 - 0xE000,
				})
				continue
			}
		} else {

			if code >= 0xE0 {
				// #if 3 top bits are '111', this is a start of a long code
				code16 = (uint16(code) << 8) // #shift and store the upper half of the new long code
				readingA16bit = true
				continue

			} else {
				// #short code
				if code >= 0xDC {
					//#temperature change
					tempGroup := uint16(code) - 0xDC
					FlowLogCodes = append(FlowLogCodes, Code{
						Type:  TempGroupCodeType,
						Value: tempGroup,
					})
					continue
				}
				//#delta flow
				deltaFlow := uint16(code) - 109
				flow += deltaFlow
				FlowLogCodes = append(FlowLogCodes, Code{
					Type:  DeltaFlowLogCodeType,
					Value: flow,
				})
				continue
			}
		}

	}
	return FlowLogCodes, nil
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

type FlowRate struct {
	SerialNo string   `json:"serial_no"`
	Ts       int64    `json:"ts"`
	Value    *float64 `json:"value"`
}

func main() {
	jsonFile, err := os.Open("../Data/flows.json")
	checkError(err)
	defer jsonFile.Close()

	byteValue, err := ioutil.ReadAll(jsonFile)
	checkError(err)

	data := make([]*FlowLog, 0)
	err = json.Unmarshal(byteValue, &data)
	checkError(err)

	series := make([]*FlowRate, 0)

	for _, record := range data {
		ts := time.Unix(record.Ts, 0)
		serie, err := DecompressFlowLogTimeSeries(record.Codes, &ts)
		checkError(err)
		for _, entry := range serie {
			switch entry.Type {
			case PauseLengthCodeType, FlowCodeType, DeltaFlowLogCodeType:
				series = append(series, &FlowRate{
					SerialNo: record.SerialNo,
					Value:    entry.Float(),
					Ts:       entry.Ts,
				})
			default:
				continue
			}
		}
	}

	file, err := json.MarshalIndent(series, "", "")
	checkError(err)

	err = ioutil.WriteFile("../Data/flow_rates.json", file, 0644)
	checkError(err)

	fmt.Println("DONE")
}
