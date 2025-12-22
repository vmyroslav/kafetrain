package resilience

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
			SetHeader[string](&hl, tt.key, tt.val)

			value, ok := GetHeaderValue[string](&hl, tt.key)
			if !ok || value != tt.val {
				t.Errorf("GetHeaderValue() got = %v, want %v", value, tt.val)
			}
		})
	}
}

func TestHeaderListUpdate(t *testing.T) {
	var hl HeaderList

	SetHeader[string](&hl, "key", "value-1")
	SetHeader[string](&hl, "key", "value-2")

	value, ok := GetHeaderValue[string](&hl, "key")

	assert.True(t, ok, "value should be found")
	assert.Equal(t, "value-2", value, "expected updated value")
	assert.Len(t, hl, 1, "should have only one header, not duplicates")
}

func TestHeaderGet(t *testing.T) {
	var hl HeaderList

	SetHeader[string](&hl, "key1", "value-1")
	SetHeader[int](&hl, "key2", 42)
	SetHeader[time.Time](&hl, "key3", time.Unix(1234567890, 0))

	val1, ok := GetHeaderValue[string](&hl, "key1")
	assert.True(t, ok, "string value should be found")
	assert.Equal(t, "value-1", val1)

	val2, ok := GetHeaderValue[int](&hl, "key2")
	assert.True(t, ok, "int value should be found")
	assert.Equal(t, 42, val2)

	val3, ok := GetHeaderValue[time.Time](&hl, "key3")
	assert.True(t, ok, "time value should be found")
	assert.Equal(t, time.Unix(1234567890, 0), val3)
}
