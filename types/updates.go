package types

import "sync/atomic"

func Last(conf *Configuration) *LastConfiguration {
	last := &LastConfiguration{}
	last.Update(conf)
	return last
}

type notifiable struct {
	conf  *Configuration
	event chan struct{}
}

// LastConfiguration is a utility to broadcast updates.
// When channel notification is received
type LastConfiguration struct {
	value atomic.Value
}

func (c *LastConfiguration) Update(conf *Configuration) {
	current := c.value.Load()
	c.value.Store(notifiable{conf, make(chan struct{})})
	if current != nil {
		close(current.(notifiable).event)
	}
}

func (c *LastConfiguration) Last() (*Configuration, <-chan struct{}) {
	val := c.value.Load().(notifiable)
	return val.conf, val.event
}

func (c *LastConfiguration) Configuration() *Configuration {
	return c.value.Load().(notifiable).conf
}
