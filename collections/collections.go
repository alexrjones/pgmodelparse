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

type Multimap[K comparable, V comparable] struct {
	m map[K][]V
}

func NewMultimap[K comparable, V comparable]() *Multimap[K, V] {
	return &Multimap[K, V]{m: make(map[K][]V)}
}

func (m *Multimap[K, V]) AddAll(key K, values ...V) {
	m.m[key] = append(m.m[key], values...)
}

func (m *Multimap[K, V]) Add(key K, value V) {
	m.m[key] = append(m.m[key], value)
}

func (m *Multimap[K, V]) Remove(key K) {
	delete(m.m, key)
}

func (m *Multimap[K, V]) RemoveValue(key K, value V) {

	s, ok := m.m[key]
	if !ok {
		return
	}
	for i, v := range s {
		if v == value {
			s = append(s[:i], s[min(i+1, len(s)):]...)
			if len(s) == 0 {
				delete(m.m, key)
			} else {
				m.m[key] = s
			}
			return
		}
	}
}

func (m *Multimap[K, V]) Get(key K) ([]V, bool) {

	value, ok := m.m[key]
	return value, ok
}

type BidiMultimap[L, R comparable] struct {
	LeftToRight *Multimap[L, R]
	RightToLeft *Multimap[R, L]
}

func NewBidiMultimap[L, R comparable]() *BidiMultimap[L, R] {

	return &BidiMultimap[L, R]{
		LeftToRight: NewMultimap[L, R](),
		RightToLeft: NewMultimap[R, L](),
	}
}

func (m *BidiMultimap[L, R]) GetLeft(key L) ([]R, bool) {
	return m.LeftToRight.Get(key)
}

func (m *BidiMultimap[L, R]) AddLeft(key L, value R) {

	m.LeftToRight.Add(key, value)
	m.RightToLeft.Add(value, key)
}

func removeSide[L, R comparable](m1 *Multimap[L, R], m2 *Multimap[R, L], key L) {

	v, ok := m1.Get(key)
	if !ok {
		return
	}
	for _, e := range v {
		m2.RemoveValue(e, key)
	}
	m1.Remove(key)
}

func (m *BidiMultimap[L, R]) RemoveRight(key R) {

	removeSide(m.RightToLeft, m.LeftToRight, key)
}

func (m *BidiMultimap[L, R]) RemoveLeft(key L) {

	removeSide(m.LeftToRight, m.RightToLeft, key)
}
