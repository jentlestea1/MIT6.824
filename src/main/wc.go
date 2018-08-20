package main

import (
	"fmt"
	"mapreduce"
	"os"
	"unicode"
	"strings"
	"strconv"
	"log"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func mapF(filename string, contents string) []mapreduce.KeyValue {
	// Your code here (Part II).
	// 把contents拆分成单词，并组成pairs
	var words []string
	var kvMap map[string]int 
	kvMap = make(map[string]int)
	var ret []mapreduce.KeyValue
	//分割函数
	splitFun := func(c rune) bool{
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	words = strings.FieldsFunc(contents,splitFun)
	//分割了之后在value中统计Key(word)出现的频率
	for _,w := range words{
		kvMap[w]++;
	}
	for k , _:= range kvMap{
		ret = append(ret,mapreduce.KeyValue{k,strconv.Itoa(kvMap[k])})
	}
	//fmt.Println(filename,"mapF end\n")
	return ret

}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceF(key string, values []string) string {
	// Your code here (Part II).
	//fmt.Println("reduceF start\n")
	//fmt.Println("string values:",len(values))
	//计算所有values的整形和
	count := 0
	//print("key :",key,"  values: ",values,"\n")
	for _,v := range values{
		tmp, err := strconv.Atoi(v)
		//print("value:",v,"\n")
		if err!=nil{
			log.Fatal("wc.go reduceF() : ",err)
		}
		count += tmp
	}
	return strconv.Itoa(count)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
	}
}
