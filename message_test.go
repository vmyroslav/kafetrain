package kafetrain

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHeaderList(t *testing.T) {
	t.Parallel()
	type args struct {
		key string
		val string
	}

	tests := []args{
		{"key", "value"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	var hl HeaderList
	for _, tt := range tests {
		t.Run("s", func(t *testing.T) {
			hl.Set(tt.key, tt.val)

			value, ok := hl.Get(tt.key)
			if !ok || value != tt.val {
				t.Errorf("Get() got = %v, want %v", value, tt.val)
			}
		})
	}
}

func TestHeaderListDuplication(t *testing.T) {
	var hl HeaderList

	hl.Set("key", "value-1")
	hl.Set("key", "value-2")

	value, ok := hl.Get("key")

	assert.True(t, ok, "value should be found")
	assert.Equal(t, "value-1", value, "expected to get first value")
}
