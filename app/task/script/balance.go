package script

import (
	"errors"
	"fmt"
	"go-micro.dev/v4/registry"
	"go-micro.dev/v4/registry/cache"
	"go-micro.dev/v4/selector"
	"sync"
	"sync/atomic"
	"time"
)

/**
 * @note: 真正的负载均衡轮训策略
 * @auth: tongWz
 * @date: 2024/2/26 19:50
**/
func NewRoundRobinSelector(opts ...selector.Option) selector.Selector {
	fmt.Printf("注册负载均衡选择器 \n")
	sopts := selector.Options{
		Strategy: TongRound,
	}

	for _, opt := range opts {
		opt(&sopts)
	}

	if sopts.Registry == nil {
		sopts.Registry = registry.DefaultRegistry
	}

	s := &RoundRobin{
		i:  0,
		so: sopts,
	}
	s.rc = s.newCache()

	return s
}

var i = new(uint64)
var mtx sync.Mutex

// 轮训 负载均衡策略
func TongRound(services []*registry.Service) selector.Next {
	lenServices := len(services)
	nodes := make([]*registry.Node, 0, uint64(lenServices))
	fmt.Printf("一共多少nodes：%d \n", lenServices)

	for _, service := range services {
		nodes = append(nodes, service.Nodes...)
	}

	return func() (*registry.Node, error) {
		if len(nodes) == 0 {
			return nil, selector.ErrNoneAvailable
		}

		// fmt.Printf("打印的轮训前i：%d \n", i)
		// fmt.Printf("打印的轮训前i：%d \n", *i)
		// fmt.Printf("打印的轮训前i指针：%p \n", i)
		mtx.Lock()
		node := nodes[*i%uint64(len(nodes))]
		// 原子性递增;
		atomic.AddUint64(i, 1)
		// fmt.Printf("一共多少nodes：%d \n", len(nodes))
		// fmt.Printf("打印的轮训后i：%d \n", *i)
		mtx.Unlock()

		return node, nil
	}
}

// 负载均衡selector interface实现
type RoundRobin struct {
	i  uint64
	so selector.Options
	rc cache.Cache
	mu sync.RWMutex
}

func (c *RoundRobin) newCache() cache.Cache {
	opts := make([]cache.Option, 0, 1)

	if c.so.Context != nil {
		if t, ok := c.so.Context.Value("selector_ttl").(time.Duration); ok {
			opts = append(opts, cache.WithTTL(t))
		}
	}

	return cache.New(c.so.Registry, opts...)
}

func (c *RoundRobin) Init(opts ...selector.Option) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, o := range opts {
		o(&c.so)
	}

	c.rc.Stop()
	c.rc = c.newCache()

	return nil
}

func (c *RoundRobin) Options() selector.Options {
	return c.so
}

func (c *RoundRobin) Select(service string, opts ...selector.SelectOption) (selector.Next, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sopts := selector.SelectOptions{
		Strategy: c.so.Strategy,
	}

	for _, opt := range opts {
		opt(&sopts)
	}

	// get the service
	// try the cache first
	// if that fails go directly to the registry
	services, err := c.rc.GetService(service)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, selector.ErrNotFound
		}

		return nil, err
	}

	// apply the filters
	for _, filter := range sopts.Filters {
		services = filter(services)
	}

	// if there's nothing left, return
	if len(services) == 0 {
		return nil, selector.ErrNoneAvailable
	}

	return sopts.Strategy(services), nil

}

func (c *RoundRobin) Mark(service string, node *registry.Node, err error) {
}

func (c *RoundRobin) Reset(service string) {
}

// Close stops the watcher and destroys the cache.
func (c *RoundRobin) Close() error {
	c.rc.Stop()

	return nil
}

func (c *RoundRobin) String() string {
	return "registry"
}
