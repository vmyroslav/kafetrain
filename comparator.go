package kafetrain

type Comparator interface {
	IsRelated(topic string, msg Message) bool
}

type KeyComparator struct {
	lm lockMap
}

func NewKeyComparator(topic string) *KeyComparator {
	return &KeyComparator{lm: make(lockMap)}
}

func (k KeyComparator) IsRelated(topic string, msg Message) bool {

	return false
}

// topic map ['topic-name' => ['topic-key' => ['id1', 'id2', 'id3'], 'topic-key-2' => ['id4', 'id5', 'id6']]].
type lockMap map[string]map[string][]string
