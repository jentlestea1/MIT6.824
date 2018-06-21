package mapreduce

import(
	//"fmt"
	"os"
	"encoding/json"
	"log"
	"sort"
)
type KeyValues []KeyValue
func (kv KeyValues) Len() int {
    return len(kv)
}

func (kv KeyValues) Less(i, j int) bool {
    return kv[i].Key < kv[j].Key
}

//Swap()
func (kv KeyValues) Swap(i, j int) {
    kv[i], kv[j] = kv[j], kv[i]
}
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	//得到原文件名,并将打开的文件描述符保存
	//var intermediateFilesName []string
	var intermediateFiles []*os.File

	//出现错误：multiple-value os.Open() in single-value context
	//解决方法：缺少了err
	for i:=0; i<nMap; i++{
		openFile,err := os.Open(reduceName(jobName,i,reduceTask))
		if err!=nil{
			log.Fatal("doReduce() : ",err)
		}
		intermediateFiles = append(intermediateFiles,openFile)
	}
	//对于所有文件，读出KeyValue数组
	kvMap := make(map[string][]string)
	for _ , file := range intermediateFiles{
		dec := json.NewDecoder(file)
		var kv KeyValue
		for dec.More() {
			err := dec.Decode(&kv)
			if err!=nil{
				log.Fatal("doReduce() : ",err)
			}
			kvMap[kv.Key] = append(kvMap[kv.Key],kv.Value)
		}
	}
	//对Key进行排序
	var Keys []string
	for k := range kvMap{
		Keys = append(Keys,k)
	}
	sort.Strings(Keys)

	//对每一个key和其对应的[]string 输入到reduceF中,将结果输出到outFile
	outputFile,err := os.Create(outFile)
	if err!=nil {
		log.Fatal("doReduce() :",err)
	}
	enc := json.NewEncoder(outputFile)
	for i,_:= range Keys{
		err := enc.Encode(KeyValue{Keys[i],reduceF(Keys[i],kvMap[Keys[i]])})
		if err!=nil{
			log.Fatal(err)
		}
	}
	for _, file := range intermediateFiles{
		file.Close()
	}
	outputFile.Close()

/*
	var s string
	w_fp,err1 := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE, 0755)
	if err1!=nil{
		log.Fatal(err1)
	}
	enc:= json.NewEncoder(w_fp)
	//fileName := reduceName(jobName,nMap,reduceTask)
	fp,_ := os.Open(fileName)
	fmt.Println("fileName:",fileName)
	fmt.Println("outFile:",outFile)
	dec :=json.NewDecoder(fp)
	var v KeyValue

	for dec.More() {
		dec.Decode(&v)
		fmt.Println(v)
		err := enc.Encode(KeyValue{v.Key,reduceF(v.Key,s)})
		if err!=nil{
			log.Fatal(err)
		}
	}
	w_fp.Close()
	fp.Close()
*/

}
