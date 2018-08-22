package main
import "fmt"
//
//func test(a int,c int){
//	fmt.Printf("hello\n")
//	go func(c){
//		fmt.Printf("%v\n",c)
//	}(c)
//	return 
//}
//
//func main(){
//	test(1,2)
//}
//func adder() func(int) int {
//    sum := 0
//    return func(x int) int {
//
//		fmt.Printf("sum:%v\n",sum)
//        sum += x
//        return sum
//    }
//}
//func demoFunction2() {
//    pos, neg := adder(), adder()
//	fmt.Printf("%v %v\n",pos,neg)
//    for i := 0; i < 10; i++ {
//        fmt.Println(pos(i), neg(-2 * i),
//        )
//    }
//}
//func main(){
//	demoFunction2()
//}
//func forwardRegistrations(ch chan string) {
//	i := 0
//	for {
//		if 10 > i {
//			// there's a worker that we haven't told schedule() about.
//			w := "aa"
//			go func() { ch <- w }() // send without holding the lock.
//			i = i + 1
//		}
//	}
//}
func aaa(){
	c := make(chan int,2)
	//defer close(c)
	go func() {
		for i:=0;i<10;i++{
			c <- i
		}
		close(c)
	}()
	for {
		i,ok := <-c
		if ok{
			fmt.Println(i)
		}
	}
}
func main(){
	//ch := make(chan string)
	//go func(){
	//	for i:=0;i<5;i++ {
	//		fmt.Println(<-ch)
	//	}
	//}()
	//forwardRegistrations(ch);
	aaa()
}




