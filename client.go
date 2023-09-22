package base

import (
	"context"
	"encoding/json"
	"github.com/tevino/abool/v2"
	"github.com/tomasliu-agora/rtm2"
	"go.uber.org/zap"
	"strings"
	"sync"
)

type messageSub struct {
	options *rtm2.MessageOptions
	mc      chan *rtm2.Message
}

type client struct {
	InvokeCallback
	ctx    context.Context
	cancel context.CancelFunc
	lg     *zap.Logger

	lock     *lock
	storage  *storage
	presence *presence

	config  rtm2.RTMConfig
	invoker Invoker
	params  map[string]interface{}
	login   abool.AtomicBool

	streamChannels  sync.Map
	messageChannels sync.Map

	cc chan *rtm2.ConnectionEvent
	tc chan string
	mc chan *rtm2.Message
}

func (c *client) Login(token string) (<-chan *rtm2.ConnectionEvent, <-chan string, <-chan *rtm2.Message, error) {
	c.lg.Debug("Login")
	if c.login.SetToIf(false, true) {
		c.invoker.PreLogin()

		preParams := make(map[string]interface{})
		for key, value := range c.params {
			c.params[key] = value
			if !strings.HasPrefix(key, golangPrefix) {
				preParams[key] = value
			}
		}

		req := &LoginReq{AppId: c.config.Appid, UserId: c.config.UserId, AreaCode: c.config.AreaCode, PresenceTimeout: c.config.PresenceTimeout, Token: token, LogPath: c.config.FilePath}
		if len(preParams) != 0 {
			paramsStr, err := json.Marshal(preParams)
			if err != nil {
				c.lg.Warn("failed to marshal pre params", zap.Any("params", preParams), zap.Error(err))
			} else {
				c.lg.Debug("pre params", zap.Any("params", preParams))
				req.Params = string(paramsStr)
			}
		}

		_, errCode, err := c.invoker.OnReceived(req)
		if err != nil {
			return nil, nil, nil, err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			return nil, nil, nil, err
		}
		c.invoker.PostLogin()
	}
	return c.cc, c.tc, c.mc, nil
}

func (c *client) Logout() error {
	c.lg.Debug("Logout")
	if c.login.SetToIf(true, false) {
		c.invoker.PreLogout()
		c.streamChannels.Range(func(key, value interface{}) bool {
			channel := value.(*stream)
			err := channel.Leave()
			c.streamChannels.Delete(key)
			c.lg.Debug("closing remaining stream channel", zap.String("channel", key.(string)), zap.Error(err))
			return true
		})

		c.messageChannels.Range(func(key, value interface{}) bool {
			channel := key.(string)
			err := c.Unsubscribe(channel)
			c.messageChannels.Delete(key)
			c.lg.Debug("closing remaining message channel", zap.String("channel", key.(string)), zap.Error(err))
			return true
		})
		c.invoker.PostLogout()
	}
	return nil
}

func (c *client) RenewToken(token string) error {
	c.lg.Debug("Renew Token")
	req := &RenewTokenReq{
		Token: token,
	}
	_, errCode, err := c.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (c *client) GetParameters() map[string]interface{} {
	return c.params
}

func (c *client) SetParameters(params map[string]interface{}) error {
	c.lg.Debug("SetParameters")

	modified := make(map[string]interface{})
	for key, value := range params {
		c.params[key] = value
		if !strings.HasPrefix(key, golangPrefix) {
			modified[key] = value
		}
	}

	if c.login.IsSet() && len(modified) != 0 {
		paramsStr, err := json.Marshal(modified)
		if err != nil {
			return err
		}
		req := &SetParamsReq{Params: string(paramsStr)}
		_, errCode, err := c.invoker.OnReceived(req)
		if err != nil {
			return err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) Storage() rtm2.Storage {
	return c.storage
}

func (c *client) Lock() rtm2.Lock {
	return c.lock
}

func (c *client) Presence() rtm2.Presence {
	return c.presence
}

func (c *client) Publish(channel string, message []byte, opts ...rtm2.MessageOption) error {
	c.lg.Debug("Publish")
	options := &rtm2.MessageOptions{
		ChannelType: rtm2.ChannelTypeMessage,
	}
	for _, opt := range opts {
		opt(options)
	}
	req := &MessagePublishReq{Channel: channel, Type: int32(options.Type), Message: message, CustomType: options.CustomType, ChannelType: int32(options.ChannelType)}
	_, errCode, err := c.invoker.OnReceived(req)
	if err != nil {
		return err
	}
	if err = rtm2.ErrorFromCode(errCode); err != nil {
		return err
	}
	return nil
}

func (c *client) Subscribe(channel string, opts ...rtm2.MessageOption) (chan *rtm2.Message, error) {
	c.lg.Debug("Subscribe")
	options := &rtm2.MessageOptions{}
	for _, opt := range opts {
		opt(options)
	}
	sub := &messageSub{mc: make(chan *rtm2.Message, defaultChanSize), options: options}
	if value, ok := c.messageChannels.LoadOrStore(channel, sub); !ok {
		c.lg.Info("Subscribe", zap.String("channel", channel))
		if options.Metadata {
			c.storage.subscribe(c.storage.unifyChannelKey(channel, rtm2.ChannelTypeMessage))
		}
		if options.Presence {
			c.presence.subscribe(c.presence.unifyChannelKey(channel, rtm2.ChannelTypeMessage))
		}
		if options.Lock {
			c.lock.subscribe(unify(channel, rtm2.ChannelTypeMessage))
		}

		req := &MessageSubReq{Channel: channel, Metadata: options.Metadata, Message: options.Message, Presence: options.Presence, Lock: options.Lock}
		_, errCode, err := c.invoker.OnReceived(req)
		if err != nil {
			c.storage.unsubscribe(c.storage.unifyChannelKey(channel, rtm2.ChannelTypeMessage))
			c.presence.subscribe(c.presence.unifyChannelKey(channel, rtm2.ChannelTypeMessage))
			c.lock.subscribe(c.storage.unifyChannelKey(channel, rtm2.ChannelTypeMessage))
			return nil, err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			c.storage.unsubscribe(c.storage.unifyChannelKey(channel, rtm2.ChannelTypeMessage))
			c.presence.subscribe(c.presence.unifyChannelKey(channel, rtm2.ChannelTypeMessage))
			c.lock.subscribe(c.storage.unifyChannelKey(channel, rtm2.ChannelTypeMessage))
			return nil, err
		}
		return sub.mc, nil
	} else {
		origin := value.(*messageSub)
		return origin.mc, nil
	}
}

func (c *client) Unsubscribe(channel string) error {
	c.lg.Debug("Unsubscribe")
	if value, ok := c.messageChannels.LoadAndDelete(channel); ok {
		c.lg.Info("Unsubscribe", zap.String("channel", channel))
		req := &MessageUnsubReq{
			Channel: channel,
		}
		_, errCode, err := c.invoker.OnReceived(req)
		if err != nil {
			return err
		}
		if err = rtm2.ErrorFromCode(errCode); err != nil {
			return err
		}
		sub := value.(*messageSub)
		if sub.options.Metadata {
			c.storage.unsubscribe(c.storage.unifyChannelKey(channel, rtm2.ChannelTypeMessage))
		}
		if sub.options.Presence {
			c.presence.unsubscribe(c.presence.unifyChannelKey(channel, rtm2.ChannelTypeMessage))
		}
		if sub.options.Lock {
			c.lock.unsubscribe(unify(channel, rtm2.ChannelTypeMessage))
		}
		close(sub.mc)
		return nil
	} else {
		return rtm2.ERROR_CHANNEL_NOT_SUBSCRIBED
	}
}

func (c *client) StreamChannel(channel string) rtm2.StreamChannel {
	s := &stream{channel: channel, client: c, lg: c.lg.With(zap.String("channel", channel)), joined: abool.New(), topicEvents: make(chan *rtm2.TopicEvent, defaultChanSize), tc: make(chan string, defaultChanSize), options: &rtm2.StreamOptions{}}
	if value, ok := c.streamChannels.LoadOrStore(channel, s); ok {
		origin := value.(*stream)
		return origin
	}
	return s
}

func (e *MessageEvent) toRTM2Event() *rtm2.Message {
	return &rtm2.Message{UserId: e.Publisher, Type: rtm2.MessageType(e.Type), Message: e.Message, CustomType: e.CustomType}
}

func (e *ConnectionStateChangeEvent) toRTM2Event() *rtm2.ConnectionEvent {
	return &rtm2.ConnectionEvent{State: e.State, Reason: e.Reason, Channel: e.Channel}
}

func (e *TokenPrivilegeExpire) toRTM2Event() string {
	return e.Channel
}

func (c *client) OnEvent(event interface{}) {
	switch e := event.(type) {
	case *StreamTopicEvent:
		channel := e.Channel
		if value, ok := c.streamChannels.Load(channel); ok {
			s := value.(*stream)
			s.OnTopicEvent(e)
		}
	case *StreamMessageEvent:
		channel := e.Channel
		if value, ok := c.streamChannels.Load(channel); ok {
			s := value.(*stream)
			s.OnMessage(e)
		}
	case *MessageEvent:
		switch e.ChannelType {
		case int32(rtm2.ChannelTypeMessage):
			channel := e.Channel
			if value, ok := c.messageChannels.Load(channel); ok {
				s := value.(*messageSub)
				s.mc <- e.toRTM2Event()
			}
		case int32(rtm2.ChannelTypeUser):
			c.mc <- e.toRTM2Event()
		}
	case *StorageChannelEvent:
		c.storage.onChannelEvent(e)
	case *StorageUserEvent:
		c.storage.onUserEvent(e)
	case *PresenceEvent:
		c.presence.onEvent(e)
	case *LockEvent:
		c.lock.onEvent(e)
	case *ConnectionStateChangeEvent:
		c.cc <- e.toRTM2Event()
	case *TokenPrivilegeExpire:
		if e.Channel == "" {
			c.tc <- e.toRTM2Event()
		} else {
			if value, ok := c.streamChannels.Load(e.Channel); ok {
				s := value.(*stream)
				s.tc <- e.toRTM2Event()
			}
		}
	default:
		c.lg.Warn("unknown event", zap.Any("event", event))
	}
}

func CreateRTMClient(ctx context.Context, config rtm2.RTMConfig, invoker Invoker) rtm2.RTMClient {
	c, cancel := context.WithCancel(ctx)
	cli := &client{ctx: c, cancel: cancel, lg: config.Logger.With(zap.String("appId", config.Appid), zap.String("userId", config.UserId)), params: make(map[string]interface{}), config: config, invoker: invoker, cc: make(chan *rtm2.ConnectionEvent, defaultChanSize), tc: make(chan string, defaultChanSize), mc: make(chan *rtm2.Message, defaultChanSize)}
	cli.lock = createLock(cli)
	cli.storage = createStorage(cli)
	cli.presence = createPresence(cli)
	cli.invoker = invoker
	invoker.SetCallback(cli)
	return cli
}
