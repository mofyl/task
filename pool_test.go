package task

import (
	"fmt"
	"testing"
)

// var (
// 	test_wg *sync.WaitGroup
// )

// func newPool() *Pool {
// 	test_wg = &sync.WaitGroup{}
// 	p := NewPool(nil, test_wg)
// 	return p
// }

// func TestPool(t *testing.T) {
// 	//res := make(chan []byte, 100)
// 	p := newPool()
// 	for i := 0; i < 10; i++ {
// 		id := int64(i)
// 		job := &Job{
// 			ID:        id,
// 			ResNotify: nil,
// 			Do: func() *TaskContext {
// 				fmt.Println(11111)
// 				time.Sleep(3 * time.Second)
// 				//ids := strconv.FormatInt(id, 10)
// 				fmt.Println(22222)
// 				return nil
// 			},
// 		}
// 		p.DoJob(job)
// 		fmt.Println(i)
// 	}
// 	fmt.Println("Send Finish")
// 	p.Close()
// 	test_wg.Wait()
// }

// func BenchmarkDoJob(b *testing.B) {
// 	//b.ResetTimer()
// 	p := newPool()
// 	time.Sleep(2 * time.Second)
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		id := int64(i)
// 		job := &Job{
// 			ID:        id,
// 			ResNotify: nil,
// 			Do: func() *TaskContext {
// 				//time.Sleep(3 * time.Second)
// 				//ids := strconv.FormatInt(id, 10)
// 				return nil
// 			},
// 		}
// 		p.DoJob(job)
// 	}
// 	fmt.Println("Send Finish")
// 	p.Close()
// 	test_wg.Wait()
// }

func TestSelect(t *testing.T) {

	m := make(map[string]string, 1)

	m["test"] = "test1"

	for k := range m {
		fmt.Println(k)
	}

}
