# RTM技术原理

本节展示 app 中实现发送和接收的基本工作流程：

![WorkFlow](WorkFlow.png)

1. 创建并加入 Stream Channel：调用 StreamChannel() 创建一个 StreamChannel 类型实例，再调用 Join 加入频道。
2. 加入 Topic：调用 JoinTopic 加入任意一个 Topic。
3. 在 Topic 中发送和订阅消息：调用 PublishTopic 发布消息，调用 SubscribeTopic 订阅远端用户的消息。
4. 接收远端用户的消息：通过 Subscribe Topic 返回的 `golang chan` 接收远端用户的消息。

# 前提条件

目前Agora RTM2 Golang SDK目前需要在特定的运行环境中执行，具体的`Dockerfile`如下：

```dockerfile

```

# 基本流程

## 1. 导入模块

```go
import "github.com/tomasliu-agora/rtm2.git"
```

## 2. 初始化 RTM Client，并登录

```go
config := &rtm2.RTMConfig{Appid: "<APP_ID>", UserId: "<RTM_USER_ID>", Logger: lg}
client := CreateRTMClient(config)
// 使用 <RTM_TOKEN> token 登录RTM
err := client.Login("<RTM_TOKEN>")
```

## 3. 创建 Stream Channel 并加入频道

在使用 StreamChannel 类中的任何 API 之前，你需要先调用 StreamChannel() 创建 Stream Channel。

```go
// 创建一个频道名为 MyChannel 的 Stream Channel
channel := client.StreamChannel("MyChannel")
// 加入频道
err := channel.Join()
```

## 4. 加入频道中的 Topic 并发布消息

成功加入 Topic 后，SDK 会自动将你注册为该 Topic 的消息发布者，你可以在该 Topic 中发送消息。成功发送后，SDK 会把该消息分发给该 Topic 的所有订阅者。

```go
// 加入名为 MyTopic 的 Topic
err := channel.JoinTopic("MyTopic")
// 在 MyTopic 中发布内容为 data 的消息
err1 := channel.PublishTopic("MyTopic", []byte{"Hello world!\0"})
```

## 5. 订阅用户，获取已订阅用户列表，获取消息，取消订阅用户

调用 subscribeTopic 订阅一个 Topic 中的一位或多位消息发布者，在一个 Topic 中最多只能订阅 64 位消息发布者。你也可以通过 GetSubscribedUsers 查询当前已订阅的消息发布者列表。

```go
// 订阅 MyTopic 中 User ID 为 <OTHER_USER> 的用户
messageChan, err := channel.SubscribeTopic("MyTopic", []string{"<OTHER_USER>"})
// 启动 goroutine 拉取消息
go func() {
    for {
        select {
        case msg := <- messageChan:
            lg.Info("recv", zap.String("msg", string(msg)))
        case <- ctx.Done():
            return
        }
    }
}
// 获取 MyTopic 中你已经订阅的用户列表
users, err1 := channel.GetSubscribedUsers("MyTopic")
// 取消订阅 My Topic 中 User ID 为 <OTHER_USER> 的用户
err2 := channel.UnsubscribeTopic("MyTopic", []string{"<OTHER_USER>"})
```

## 6. 离开 Topic，离开频道，并登出

如果你不需要在该 Topic 中发送消息，调用 LeaveTopic 离开该 Topic。离开某个 Topic 不会影响你订阅其他 Topic 中的消息。如果你不再需要发送或接收该频道中的消息，调用 Leave 离开该频道。如果你不再需要使用 RTM 客户端，调用Logout 退出登录。

```go
// 离开 MyTopic
err := channel.LeaveTopic("MyTopic")
// 退出频道
err1 := channel.Leave()
channel = nil
// 登出
client.Logout()
```

# 开发注意事项

- 一条 RTM 消息可以是字符串或者二进制数据，你需要在业务层自行区分消息负载格式。为更灵活地实现你的业务，你也可以使用 JSON 等其他方式来构建你的负载格式，此时，你需要确保转交给 RTM 的消息负载已字符串序列化。
- userId 为不超过 64 位的任意字符串序列，且具有唯一性。不同用户、同一用户的不同终端设备需要通过不同的 userId 进行区分，所以你需要处理终端用户和 userId 的映射关系，确保终端用户的 userId 唯一且可以被复用。此外，项目中的 userId 数量还会影响最大连接数（PCU）的计量，从而影响计费。
- 成功离开频道后，你在该频道中注册的所有 Topic 发布者的角色以及你在所有 Topic 中的订阅关系都将自动解除。如需恢复之前注册的发布者角色和消息的订阅关系，声网推荐你在调用 leave 之前自行记录相关信息，以便后续重新调用 join、joinTopic 和 subscribeTopic 进行相关设置。