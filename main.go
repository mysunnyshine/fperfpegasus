package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"time"
)


func main() {
	msetNum := 100
	cfgPath, _ := filepath.Abs("./config.json")
	rawCfg, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		fmt.Println(err)
		return
	}

	cfg := &pegasus.Config{}
	json.Unmarshal(rawCfg, cfg)
	c := pegasus.NewClient(*cfg)

	tb, err := c.OpenTable(context.Background(), "temp")
	if err != nil {
		return
	}
	e1 := tb.Set(context.Background(), []byte("0"), []byte("weili"), []byte("124"))
	if e1 != nil {
		fmt.Println("err 发生", e1)
	}
	total := int64(0)
	for i := 0; i < 1000; i++ {
		sortedKeys := make([][]byte, msetNum)
		values := make([][]byte, msetNum)
		for j := 0; j < msetNum; j++ {
			var k int
			k = i * msetNum + j
			s := strconv.Itoa(k)
			sortedKeys[j] = []byte(s)
			values[j] = []byte(s)
		}
		start := time.Now().UnixNano()
		tb.MultiSet(context.Background(), []byte("0"), sortedKeys, values)
		end := time.Now().UnixNano()
		delta := end - start
		total = total + delta/1000000
		fmt.Println("第 %s 次 mset， 时间= %s ns", i, delta, total)
	}
}
