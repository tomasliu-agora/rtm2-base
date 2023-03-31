package base

type InvokeCallback interface {
	OnEvent(event interface{})
}

type Invoker interface {
	PreLogin()
	PostLogin()

	PreLogout()
	PostLogout()

	OnReceived(req interface{}) (interface{}, int32, error)
	OnAsyncReceived(req interface{}, callback func(interface{}, int32, error)) error
	SetCallback(callback InvokeCallback)
}
