package events

import (
	"sync"

	"github.com/google/uuid"
)

var evBus *Bus
var onInit = &sync.Once{}

func NewEventBus() (*Bus, string) {
	onInit.Do(func() {
		evBus = newEventsBus()
		go evBus.handleEvents()
	})

	// return events bus with subscriberID
	return evBus, uuid.NewString()
}
