package task

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

type TaskConf struct {
	// 秒为单位
	ExpTime       int64 `toml:"exp_time"`
	TaskNum       int64 `toml:"task_num"`
	WorkerNum     int32 `toml:"worker_num"`
	WorkerContent int64 `toml:"worker_content"`
}

var (
	defaultConf = &TaskConf{
		TaskNum:       4,  // 表示 TaskManager Channel 的长度
		WorkerNum:     2,  // 表示创建多少个 worker
		WorkerContent: 1,  // 表示 worker的 channel 的长度
		ExpTime:       60, // 表示 临时worker 的maxLifeTime
	}

	// defaultLog logger.Logger
)

type StartTask interface {
	StartTask(ctx *TaskContext) bool
}

type TaskManager struct {
	pool        *Pool
	TaskChan    chan *TaskContext
	wg          *sync.WaitGroup
	closeCtx    context.Context
	closeCancel context.CancelFunc
	isClosed    int32 // 1 表示关闭 2 表示开启
	conf        *TaskConf
}

func NewTaskManager(conf *TaskConf) *TaskManager {

	if conf == nil {
		conf = defaultConf
	}
	ctx, cancel := context.WithCancel(context.Background())
	tm := &TaskManager{}
	tm.wg = &sync.WaitGroup{}
	tm.TaskChan = make(chan *TaskContext, conf.TaskNum)
	tm.pool = NewPool(conf, tm.wg)
	tm.conf = conf
	tm.closeCtx = ctx
	tm.closeCancel = cancel
	tm.wg.Add(1)
	go tm.goTask()
	tm.isClosed = 2
	return tm
}

func (t *TaskManager) StartTask(ctx *TaskContext) bool {

	if t.isClose() {
		return false
	}

	t.TaskChan <- ctx
	if t.isClose() {
		t.closeCancel()
	}
	return true
}

func (t *TaskManager) goTask() {

	defer func() {
		if err := recover(); err != nil {
			runtime.Caller(1)
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)
			pl := fmt.Sprintf("http server panic: %v\n%s\n", err, buf[:n])
			fmt.Printf("worker recover is recover  %v \n", pl)
		}
	}()

	for {
		select {
		case <-t.closeCtx.Done():
			close(t.TaskChan)
			for v := range t.TaskChan {
				t.doJob(v)
			}
			t.pool.Close()

			t.wg.Done()
			return
		case ctx, ok := <-t.TaskChan:
			if !ok {
				break
			}
			t.doJob(ctx)
		}
	}
}

func (t *TaskManager) doJob(ctx *TaskContext) {
	t.pool.DoJob(ctx)
}

func (t *TaskManager) Close() {
	if t.isClose() {
		return
	}
	atomic.StoreInt32(&t.isClosed, 1)

	t.wg.Wait()
}

func (t *TaskManager) isClose() bool {
	return atomic.LoadInt32(&t.isClosed) == 1
}
