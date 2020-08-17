package task

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

type worker struct {
	ID     int64
	job    chan *TaskContext
	jobNum int64
	wg     *sync.WaitGroup
	wType  WorkType
	info   *poolInfo

	Blocking uint32 // 1表示正在执行阻塞任务 若该worker正在执行阻塞任务，那么后面便不会给他排任务 直到阻塞任务执行完成
}

func newWorker(id int64, chanLen int64, wg *sync.WaitGroup, info *poolInfo, wType WorkType) *worker {
	return &worker{
		ID:     id,
		job:    make(chan *TaskContext, chanLen),
		jobNum: 0,
		wg:     wg,
		info:   info,
		wType:  wType,
	}

}

func (w *worker) sendJob(j *TaskContext) bool {
	w.job <- j
	w.setBlock()
	return true
	//default:
	//	return false
}

func (w *worker) IsBlocking() bool {
	return atomic.LoadUint32(&w.Blocking) == 1
}

func (w *worker) setBlock() {
	atomic.StoreUint32(&w.Blocking, 1)
}

func (w *worker) startWorker() {

	defer func() {
		if err := recover(); err != nil {
			runtime.Caller(1)
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)
			pl := fmt.Sprintf("http server panic: %v\n%s\n", err, buf[:n])
			fmt.Printf("worker recover is recover : %v \n", pl)
		}
	}()

	fmt.Printf("Worker Start ID is %d\n", w.ID)
	for {
		select {
		case <-w.info.ctx.Done():
			// 将自己从Pool身上删除
			if w.wType == TEMPORARY && w.info.cancel != nil {
				w.info.cancel()
				w.info.f(w.ID)
			}
			close(w.job)
			for v := range w.job {
				w.excCtx(v)
			}
			w.wg.Done()
			return
		case job, ok := <-w.job:
			if !ok {
				//log.Info("Worker is Closed Id is %s", w.ID)
				break
			}
			// Do pool
			//atomic.AddInt64(&w.jobNum, 1)
			atomic.AddInt64(&w.jobNum, 1)
			//log.Debug("Do PoolJob JobID is %d", job.ID)
			w.excCtx(job)
			atomic.AddInt64(&w.jobNum, -1)
			atomic.StoreUint32(&w.Blocking, 0)
		}
	}
}

func (w *worker) excCtx(ctx *TaskContext) {
	atomic.StoreInt64(&ctx.WorkID, w.ID)
	for ctx.curStage < ctx.TaskStage &&
		int(ctx.curStage) < len(ctx.TaskHandlers) &&
		ctx.TaskHandlers[ctx.curStage](ctx) {
		ctx.curStage++
	}

	if ctx.NotifyUserChan != nil {
		ctx.NotifyUserChan <- ctx
	}
}
