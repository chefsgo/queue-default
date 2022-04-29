package bus

import (
	"errors"
	"sync"

	"github.com/chefsgo/queue"
	"github.com/chefsgo/util"
)

// mo

var (
	errRunning = errors.New("Queue is running")
)

type (
	defaultDriver  struct{}
	defaultConnect struct {
		mutex   sync.RWMutex
		running bool
		actives int64

		name     string
		config   queue.Config
		delegate queue.Delegate

		runner *util.Runner
		queues map[string]chan *defaultMsg
	}

	defaultMsg struct {
		name string
		data []byte
	}
)

//连接
func (driver *defaultDriver) Connect(name string, config queue.Config) (queue.Connect, error) {
	return &defaultConnect{
		name: name, config: config, runner: util.NewRunner(),
		queues: make(map[string]chan *defaultMsg, 0),
	}, nil
}

//打开连接
func (connect *defaultConnect) Open() error {
	return nil
}
func (connect *defaultConnect) Health() (queue.Health, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return queue.Health{Workload: connect.actives}, nil
}

//关闭连接
func (connect *defaultConnect) Close() error {
	connect.runner.End()
	return nil
}

func (connect *defaultConnect) Accept(delegate queue.Delegate) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	connect.delegate = delegate

	return nil
}

//省点事，一起注册了
//除了单机和nats，不打算支持其它总线驱动了
//以后要支持其它总线驱动的时候，再说
//还有种可能，就是，nats中队列单独定义使用jetstream做持久的时候
//那也可以同一个Register方法，定义实体来注册，加入Type或其它方式来区分
func (connect *defaultConnect) Register(name string) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	connect.queues[name] = make(chan *defaultMsg, 10)

	return nil
}

//开始订阅者
func (connect *defaultConnect) Start() error {
	if connect.running {
		return errRunning
	}

	for _, cccc := range connect.queues {
		connect.runner.Run(func() {
			for {
				select {
				case msg := <-cccc:
					connect.delegate.Serve(msg.name, msg.data)
				case <-connect.runner.Stop():
					return
				}
			}
		})
	}

	connect.running = true
	return nil
}

func (connect *defaultConnect) Enqueue(name string, data []byte) error {
	if qqq, ok := connect.queues[name]; ok {
		qqq <- &defaultMsg{name, data}
	}
	return nil
}

//------------------------- 默认队列驱动 end --------------------------
