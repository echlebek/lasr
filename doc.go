// Package lasr implements a persistent message queue backed by BoltDB. This queue is useful when the producers and consumers can live in the same process.
//
// lasr is designed to never lose information. When the Send method completes, messages have been safely written to disk. On Receive, messages are not deleted until Ack is called. Users should make sure they always respond to messages with Ack or Nack.
//
// Dead-lettering is supported, but disabled by default.
package lasr
