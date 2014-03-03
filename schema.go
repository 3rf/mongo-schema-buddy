package main

import (
	"bytes"
	"flag"
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"reflect"
	"time"
)

type BsonType struct {
	Name string
	Id   uint
}

//TODO refactor this to a fucntion, so that it works
//     with mintype and js w/ scope and other edge cases
var typeMap = map[reflect.Type]BsonType{
	reflect.TypeOf(0.1):                    {"Double", 1},
	reflect.TypeOf(""):                     {"String", 2},
	reflect.TypeOf(bson.Binary{}):          {"Binary", 5},
	reflect.TypeOf(bson.NewObjectId()):     {"ObjectId", 7},
	reflect.TypeOf(true):                   {"Boolean", 8},
	reflect.TypeOf(time.Time{}):            {"Date", 9},
	reflect.TypeOf(nil):                    {"Null", 10},
	reflect.TypeOf(bson.RegEx{}):           {"RegEx", 11},
	reflect.TypeOf(int(1)):                 {"Integer", 16}, //TODO INTS ARE WEIRDDD
	reflect.TypeOf(int32(1)):               {"Integer32", 16},
	reflect.TypeOf(bson.MongoTimestamp(1)): {"Timestamp", 17},
	reflect.TypeOf(int64(1)):               {"Integer64", 18},
}

type DocCounter struct {
	Name     string
	Counter  int64
	IsSubDoc bool
	Fields   map[string]*FieldCounter
}

type FieldCounter struct {
	Name               string
	IsArray            bool
	Counter            int64
	TypeCounter        map[reflect.Type]int64
	ArraySubCounter    *FieldCounter
	SubDocumentCounter *DocCounter
}

func NewFieldCounter(name string) *FieldCounter {
	fc := FieldCounter{
		Name:        name,
		TypeCounter: make(map[reflect.Type]int64)}
	return &fc
}

func NewArrayCounter() *FieldCounter {
	fc := FieldCounter{
		IsArray:     true,
		TypeCounter: make(map[reflect.Type]int64)}
	return &fc
}

func (self *FieldCounter) AddValue(val interface{}) {
	self.Counter += 1
	valType := reflect.TypeOf(val)
	if valType.Kind() == reflect.Slice {
		self.AddArrayValue(val.([]interface{})) //TODO is this safe???
		return
	}
	if valType.Kind() == reflect.Map {
		self.AddSubDocValue(val.(bson.M))
		return
	}
	self.TypeCounter[valType] += 1
}

func (self *FieldCounter) AddArrayValue(valArray []interface{}) {
	// Init special counter for arrays, if it doesn't exit
	if self.ArraySubCounter == nil {
		self.ArraySubCounter = NewArrayCounter()
	}
	for _, val := range valArray {
		self.ArraySubCounter.AddValue(val)
	}
}

func (self *FieldCounter) AddSubDocValue(doc bson.M) {
	if self.SubDocumentCounter == nil {
		self.SubDocumentCounter = NewSubDoc()
	}
	self.SubDocumentCounter.AddDocument(doc)
}

func NewDoc(name string) *DocCounter {
	doc := DocCounter{
		Name:   name,
		Fields: make(map[string]*FieldCounter)}
	return &doc
}

func NewSubDoc() *DocCounter {
	doc := DocCounter{
		IsSubDoc: true,
		Fields:   make(map[string]*FieldCounter)}
	return &doc
}

func (self *DocCounter) AddDocument(doc bson.M) {
	self.Counter += 1
	for key, val := range doc {
		field := self.Fields[key]
		if field == nil {
			field = NewFieldCounter(key)
			self.Fields[key] = field
		}
		field.AddValue(val)
	}

}

func (self *FieldCounter) stringToBuffer(strBuf *bytes.Buffer) {
	for typeName, _ := range self.TypeCounter {
		strBuf.WriteString(fmt.Sprintf("%s ", typeMap[typeName].Name))
	}
	if self.ArraySubCounter != nil {
		strBuf.WriteString("[ ")
		self.ArraySubCounter.stringToBuffer(strBuf)
		strBuf.WriteString("] ")
	}
	if self.SubDocumentCounter != nil {
		strBuf.WriteString("{ ")
		self.SubDocumentCounter.stringToBuffer(strBuf)
		strBuf.WriteString("} ")
	}
}

func (self *DocCounter) String() string {
	var strBuf bytes.Buffer
	self.stringToBuffer(&strBuf)
	return strBuf.String()
}

func (self *DocCounter) stringToBuffer(strBuf *bytes.Buffer) {
	for key, counter := range self.Fields {
		strBuf.WriteString(fmt.Sprintf("\"%s\": ", key))
		counter.stringToBuffer(strBuf)
		if !self.IsSubDoc { //TODO config
			strBuf.WriteString("\n")
		} else {
			strBuf.WriteString(", ")
		}
	}
}

func main() {
	flag.Parse()
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	c := session.DB(flag.Arg(0)).C(flag.Arg(1))
	defer session.Close()

	dc := NewDoc(flag.Arg(1))
	resultCursor := c.Find(bson.M{}).Iter()
	var res bson.M
	for resultCursor.Next(&res) {
		dc.AddDocument(res)
	}
	if err := resultCursor.Close(); err != nil {
		panic(err)
	}

	fmt.Println(dc)

}
