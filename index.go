package bus

import "github.com/chefsgo/queue"

func Driver() queue.Driver {
	return &defaultDriver{}
}

func init() {
	queue.Register("default", Driver())
}
