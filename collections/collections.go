package collections

type OrderedMap[K comparable, V comparable] struct {
	slice []V
	m     map[K]V
}

func NewOrderedMap[K comparable, V comparable]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		slice: make([]V, 0, 10),
		m:     make(map[K]V, 10),
	}
}

func (o *OrderedMap[K, V]) Add(key K, value V) {
	if _, ok := o.m[key]; ok {
		return
	}
	o.slice = append(o.slice, value)
	o.m[key] = value
}

func (o *OrderedMap[K, V]) List() []V {
	return o.slice
}

func (o *OrderedMap[K, V]) Get(key K) (v V, ok bool) {
	v, ok = o.m[key]
	return
}

func (o *OrderedMap[K, V]) Remove(key K) {
	value, ok := o.m[key]
	if !ok {
		return
	}
	delete(o.m, key)
	for i, v := range o.slice {
		if v == value {
			if i == len(o.slice)-1 {
				o.slice = o.slice[:i]
			} else {
				o.slice = append(o.slice[:i], o.slice[i+1:]...)
			}
			return
		}
	}
}

type Multimap[K comparable, V any] struct {
	m map[K][]V
}

func NewMultimap[K comparable, V any]() *Multimap[K, V] {
	return &Multimap[K, V]{m: make(map[K][]V)}
}

func (m *Multimap[K, V]) AddAll(key K, values ...V) {
	m.m[key] = append(m.m[key], values...)
}

func (m *Multimap[K, V]) Add(key K, value V) {
	m.m[key] = append(m.m[key], value)
}

func (m *Multimap[K, V]) Delete(key K) {
	delete(m.m, key)
}

func (m *Multimap[K, V]) Get(key K) ([]V, bool) {

	value, ok := m.m[key]
	return value, ok
}
