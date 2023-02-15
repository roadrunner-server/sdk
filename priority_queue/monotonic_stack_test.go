package priorityqueue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMonotonicStackInit(t *testing.T) {
	c := newStack()
	c.add(1)
	c.add(2)
	c.add(3)
	c.add(5)
	c.add(6)
	c.add(10)
	c.add(14)

	assert.Equal(t, [][2]int{{1, 3}, {5, 6}, {10, 10}, {14, 14}}, c.indices())
}

func TestMonotonicStackLastStartEl(t *testing.T) {
	c := newStack()
	c.add(1)
	c.add(3)
	c.add(5)
	c.add(7)
	c.add(9)
	c.add(11)
	c.add(13)

	assert.Equal(t, [][2]int{{1, 1}, {3, 3}, {5, 5}, {7, 7}, {9, 9}, {11, 11}, {13, 13}}, c.indices())
}
