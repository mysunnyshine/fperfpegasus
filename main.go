package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"strconv"
	"time"
)

var msetNum int = 200
var mgetNum int = 2000
func main() {
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

	//mset(tb)
	mget(tb)

}


func mget(tb pegasus.TableConnector){
	sortedKeys := make([][]byte, mgetNum)
	total := int64(0)
	for j := 0; j < 10000000; j++ {
		for i := 0; i < mgetNum; i++ {
			k := RandInt(200000000, 500000000)
			s := strconv.Itoa(k)
			//fmt.Println("sss", s)
			sortedKeys[i] = []byte(s)
		}
		start := time.Now().UnixNano()
		values, _, _ := tb.MultiGet(context.Background(), []byte("0"), sortedKeys)
		vvv := ""
		kkk := ""
		for _, vv := range values {
			vvv = string(vv.Value)
			kkk = string(vv.SortKey)
		}
		end := time.Now().UnixNano()
		delta := end - start
		total = total + delta/1000000
		fmt.Println("第 %s 次 mget， 时间= %s ns", j, delta, total/int64(j+1), vvv, kkk)
	}
}

func mset(tb pegasus.TableConnector) {
	total := int64(0)
	for i := 1000000; i < 1500000; i++ {
		sortedKeys := make([][]byte, msetNum)
		values := make([][]byte, msetNum)
		for j := 0; j < msetNum; j++ {
			var k int
			k = i*msetNum + j
			s := strconv.Itoa(k)
			sortedKeys[j] = []byte(s)
			values[j] = []byte(s)
		}
		start := time.Now().UnixNano()
		_ = tb.MultiSet(context.Background(), []byte("1"), sortedKeys, values)
		end := time.Now().UnixNano()
		delta := end - start
		total = total + delta/1000000
		fmt.Println(fmt.Sprintf("第 %d 次 mset， 时间= %d ns, 平均时间= %d ns", i, delta, total/int64(i-1000000+1)))
	}
}

func RandInt(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	if min >= max || min == 0 || max == 0 {
		return max
	}
	return rand.Intn(max-min) + min
}
