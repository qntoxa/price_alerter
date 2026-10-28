package cache

import "sync"

type call[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

// Group реализует паттерн singleflight (подавление дублирующихся одновременных вызовов
// для одного и того же ключа) для защиты от эффекта Thundering Herd при истечении кеша.
type Group[V any] struct {
	mu sync.Mutex
	m  map[string]*call[V]
}

func NewGroup[V any]() *Group[V] {
	return &Group[V]{
		m: make(map[string]*call[V]),
	}
}

// Do выполняет функцию fn только один раз для конкурентных вызовов с одинаковым key.
// Второе возвращаемое значение shared указывает, был ли результат разделён между несколькими горутинами.
func (g *Group[V]) Do(key string, fn func() (V, error)) (V, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call[V])
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := new(call[V])
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err, false
}
