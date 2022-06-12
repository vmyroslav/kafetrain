package kafetrain

type Comparator interface {
	Compare(topic string, msg Message) bool
}

type KeyComparator struct {
	lm lockMap
}

func NewKeyComparator() *KeyComparator {
	return &KeyComparator{lm: make(lockMap)}
}

func (k KeyComparator) Compare(topic string, msg Message) bool {
	//TODO implement me
	panic("implement me")
}

// topic map ['topic-name' => ['topic-key' => ['id1', 'id2', 'id3'], 'topic-key-2' => ['id4', 'id5', 'id6']]].
type lockMap map[string]map[string][]string
