# LabRPC Documentation

## labrpc structure

**labrpc** 是基于 `channel` 的 `RPC` 。**`labrpc` 仿真了一个不可靠的网络**，在该网络中可能会出现**丢失请求、丢失响应、信息延迟以及与主机失去连接**。labrpc 支持**高并发**RPC请求。`labrpc` 采用锁和原子操作来保证共享数据的并发安全。**下图定义了 RPC 的 `Client` 和 `Server`**。

<div align="center">
    <img src="https://raw.githubusercontent.com/JackFroster/Images/main/image/%E6%88%AA%E5%B1%8F2023-04-07%2012.18.10.png" alt="截屏2023-04-07 12.18.10" width="750px" />
</div>

`labrpc` 中包含几个主要的实体：**reqMsg, replyMsg, ClientEnd, Server, Service, NetWork**。

- `reqMsg` 是 `Client` 发送给 `Server` 的请求信息的格式。实际的数据在 `Network` 中是以**字节**的形式传递，由 `labgob` 库实现将具体的数据 `Data` 转换成字节，存储在reqMsg。
- `replyMsg` 是 `Server` 发送给 `Client` 的响应数据的格式。实际的数据在 `Network` 中是以**字节**的形式传递，由 `labgob` 库实现将具体的数据 `Data` 转换成字节，存储在replyMsg。
- `ClientEnd` 是 `RPC`请求的发起者，它会将数据 `Data`通过 `labgob` 库打包字节，存储在 `reqMsg` 中，通过 `endCh` 通道发送到 `Network` 中
- `Server` 负责处理 RPC接受到的请求，比将 `replyMsg` 返回到 `Network` 中。
- `Service` 是 `Server` 能够为 `Client` 提供的服务，由 `reflect` 反射包实现，它会被注册到 `Server`。
- **`Network` 主要提供了一个可以配置的网络环境，实现了丢失请求、丢失响应、信息延迟以及与主机失去连接**。`Network` 还管理了 `Server` 与 `Client` 之间的关联关系。

**注：所有的 `Client` 都会与 `Network` 共享一个 `Channel` 进行通信，即 `endCh`**。



## reqMsg 和 replyMsg

```go
type reqMsg struct {
	endname  interface{} // name of sending ClientEnd
	svcMeth  string      // e.g. "Raft.AppendEntries"
	argsType reflect.Type
	args     []byte
	replyCh  chan replyMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}
```

`Client` 和 `Server` 发送的具体数据信息都会通过 `labgob` 库编码为字节数据，然后存储在`reqMsg.args`或者`replyMsg.reply`中。`reMsg.argsType`指定了 Client 所发送数据的反射类型`reflect.Type`，方便 `Server` 获取到数据后解码。

`reqMsg.reply`中的 `Channel` 用于接受来自 `Server` 的响应信息。

**为什么在网络传输的数据 Data 都需要编码为字节数据，使用时再进行解码？**

因为 `labrpc` 是基于`channel`的 `RPC`。数据 `Data` 很多时候是`Slice`、`Map`、`Struct`等引用类型。引用类型的数据通过 `Channel` 从 `Client(Server)` 传递到 `Server(Client)`，很容易被 `Server(Client)` 意外修改。



## ClientEnd

- `Client` 维护了自己的**端口文件名**`endname`（类似于 RPC 中的`address:port`），**在这是一个唯一的随机字符串**。

- 同时，它还维护一个用于传递 `reqMsg` 的 `ch` 通道，**该channel是NetWork持有的全局唯一的 `channel` 赋值**。

- 此外，`Client` 还维护了一个名为 `done` 的通道，**接受Network关闭网络的通知**，它会将自己注销掉。


```go
type ClientEnd struct {
    endname interface{}   // this end-point's name
    ch      chan reqMsg   // copy of Network.endCh
    done    chan struct{} // closed when Network is cleaned up
}
```

`ClientEnd`实现了`Call`方法，用于发送请求，并等待回复。**该调用是同步调用。**

```go
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	  ...
}
```



## Service

**一个`Server`可以提供很多服务`Service`**。`Service`结构体通过**反射的方式**为所有的外部服务提供了统一的描述方式，使得**不同的外部服务可以很方便地被注册到`Server`中**。

```go
type Service struct {
    name    string // service name
    rcvr    reflect.Value
    typ     reflect.Type
    methods map[string]reflect.Method
}
```

`Service`会记录某个外部服务的值`reflect.Value`和类型`reflect.Type`，以及外部服务的接口`map[string]reflect.Method`

`MakeService`构造方法提供了将指定外部服务`rcvr`构建成`Service`。

```go
func MakeService(rcvr interface{}) *Service {
		...
}
```

`Service`会在`methods`寻找名为`methname`的方法，并调用该方法

```go
func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
		....
  	if method, ok := svc.methods[methname]; ok {
			....
      function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})
  	....
}
```



## Server

`Server`是`service`的集合。`Server`可以处理并发访问服务器的`Goroutinue`。为了保证`map`并发安全，`Server`在访问`map`时需要加锁。

```go
type Server struct {
    mu       sync.Mutex
    services map[string]*Service
    count    int // incoming RPCs
}
```

`Server` 提供了构造方法

```
func MakeServer() *Server {
    rs := &Server{}
    rs.services = map[string]*Service{}
    return rs
}
```

**`Server`为外部请求服务的`goroutine`提供了访问服务的接口**，依据`reqMsg`中方法名`svcMeth`、数据`args`和数据类型`argsType`，通过路由的方式找到响应的`Handler`。



```go
func (rs *Server) dispatch(req reqMsg) replyMsg {
    rs.mu.Lock()
		...
    service, ok := rs.services[serviceName]
    rs.mu.Unlock()
    if ok {
      return service.dispatch(methodName, req)
      ...
}
```

## Network

`Network`是这个库中最重要的一个组件。它提供了一个**可以配置的网络环境，实现了丢失请求、丢失响应、信息延迟以及与主机失去连接。**`Network`还管理了`Server`与`Client`之间的关联关系。

```go
type Network struct {
    mu             sync.Mutex
    reliable       bool
    longDelays     bool                        // pause a long time on send on disabled connection
    longReordering bool                        // sometimes delay replies a long time
    ends           map[interface{}]*ClientEnd  // ends, by name
    enabled        map[interface{}]bool        // by end name
    servers        map[interface{}]*Server     // servers, by name
    connections    map[interface{}]interface{} // endname -> servername
    endCh          chan reqMsg
    done           chan struct{} // closed when Network is cleaned up
    count          int32         // total RPC count, for statistics
    bytes          int64         // total bytes send, for statistics
}
```

其中，`reliable,longDelay,longReordering`分别定义类网络的**是否可靠，网络延迟以及消息响应的延迟时间**。`ends`和`server`分别管理了网络中的`ClientEnd`和`Server`。`connections`定义了两着的连接关系。

`enable`标记了某些设备是在当前网络中是可以用的。

`endCh`定义了`ClientEnd`与`Network`之间唯一的信息传递的通道。

此外，`Network`也记录中网络中总的`RPC`的**请求连接数量**和向网络中**传输的总的字节数**`bytes`



`Network`提供了许多的外部接口和内置方法

```go
// 关闭network, 同时通知Client网络连接已经关闭，ClientEnd.Call()方法将不会发送RPC请求
func (rn *Network) Cleanup() {
    ...
}
```

设置网络可用性状态。`Network`每接收到一个`Client`的`RPC`请求，就会分配一个`Goroutine`去处理请求。因此**对于共享数据的访问必须上锁**。

```go
func (rn *Network) Reliable(yes bool) {
    rn.mu.Lock()
    defer rn.mu.Unlock()

    rn.reliable = yes
}

func (rn *Network) LongReordering(yes bool) {
    rn.mu.Lock()
    defer rn.mu.Unlock()

    rn.longReordering = yes
}

func (rn *Network) LongDelays(yes bool) {
    rn.mu.Lock()
    defer rn.mu.Unlock()

    rn.longDelays = yes
}
```

获取网络中的流量信息（`RPC`请求数量和向网络中传输的字节数），对共享变量的访问使用了**原子操作**。

```go
func (rn *Network) GetTotalCount() int {
    x := atomic.LoadInt32(&rn.count)
    return int(x)
}

func (rn *Network) GetTotalBytes() int64 {
    x := atomic.LoadInt64(&rn.bytes)
    return x
}
```