package lasr

// ID is used for uniquely identifying messages in a Q.
type ID int64

// Message is a messaged returned from Q on Receive.
//
// Message contains a Body and an ID. The ID will be equal to the ID that was
// returned on Send, Delay or Wait for this message.
type Message struct {
	Body []byte
	ID   ID
	q    *Q
	once int32
	err  error
}
