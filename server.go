package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

/*
 通过一个group去通知这个group中的所有链接，执行操作
 1.使用redis的集合去管理各个链接的clientId
 2.使用redis发布订阅模式
 3.也可以使用go数据类型去实现
*/

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var redisPool *redis.Pool

var logger zerolog.Logger

type WsManager struct {

	// 存储ws长链接
	_map sync.Map

	// 当前的链接个数
	num int64
}

type WsStruct struct {

	// 链接对象
	conn *websocket.Conn

	// 最后一次请求的时间戳
	latestTime int64

	// 注册的时间戳
	registerTime int64

	id string
}

/*
 添加链接
*/
func (m *WsManager) set(clientId string, s *WsStruct) {
	m._map.Store(clientId, s)
}

/*
 获取链接
*/
func (m *WsManager) get(clientId string) *WsStruct {
	value, ok := m._map.Load(clientId)
	if ok {
		return value.(*WsStruct)
	}
	return nil
}

/*
 删除链接
*/
func (m *WsManager) del(clientId string) {
	m._map.Delete(clientId)
}

func NewRedisPool() *redis.Pool {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", fmt.Sprintf("%s:%s", redisAddress, redisPort))
			if err != nil {
				return nil, err
			}

			// 校验是否有密码
			if redisPwd != "" {
				if _, err := c.Do("AUTH", redisPwd); err != nil {
					_ = c.Close()
					return nil, err
				}
			}

			return c, nil
		},
	}
}

func NewLogger(w ...io.Writer) zerolog.Logger {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	mut := zerolog.MultiLevelWriter(w...)
	return zerolog.New(mut).With().Str("time", time.Now().Format("2006-01-02 15:04:05")).Logger()
}

type ResponseJson struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

type ReqStruct struct {
	Method string `json:"method"`
	Params string `json:"params"`
}

var (
	Port, redisPrefix, redisAddress, redisPort, redisPwd, filePath string
	h                                                              bool
	timeOut                                                        int
)

var manager WsManager
var redisClientsHashKey = "CLIENTSHASH"
var redisAllGroupKey = "ALLGROUP"

func newManager() WsManager {
	return WsManager{}
}

func usage() {
	_, _ = fmt.Fprintf(os.Stderr, `simple jsRpc demo
Options:
`)
	flag.PrintDefaults()
}
func main() {

	flag.StringVar(&Port, "p", "9999", "启动端口")
	flag.IntVar(&timeOut, "t", 10, "服务端与客户端通信超时时间，单位:秒")
	flag.StringVar(&redisPrefix, "rdsPrefix", "group:", "group在redis中的前缀")
	flag.StringVar(&redisAddress, "rdsAddr", "127.0.0.1", "redis ip地址")
	flag.StringVar(&redisPort, "rdsPort", "6379", "redis port端口")
	flag.StringVar(&redisPwd, "rdsPwd", "", "redis密码,没有不用传递")
	flag.BoolVar(&h, "h", false, "help")
	flag.StringVar(&filePath, "f", "./jsRpc.log", "日志文件")
	flag.Usage = usage

	flag.Parse()
	if h {
		flag.Usage()
		return
	}

	// 创建日志文件，将信息记录到日志中
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("open file error =>", filePath)
		os.Exit(1)
	}

	logger = NewLogger(file, os.Stdout)
	logger.Info().Dict("config", zerolog.Dict().
		Str("serverPort", Port).
		Int("communicationTime", timeOut).
		Str("rdsPrefix", redisPrefix).
		Str("rdsAddr", redisAddress).
		Str("rdsPort", redisPort).
		Str("rdsPwd", redisPwd).Str("logFilePath", filePath)).
		Msg("初始化配置")

	manager = newManager()
	redisPool = NewRedisPool()
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt)

	// 安全退出,删除redis中的数据
	go func() {
		<-sigChan

		// 删除所有的group
		conn := redisPool.Get()
		allGroup, err := redis.Values(conn.Do("SMEMBERS", redisAllGroupKey))
		if err == nil {
			for _, item := range allGroup {
				logger.Info().Str("group", string(item.([]byte))).Msg("删除group")
			}

			// TODO:: 对执行的redis命令也保存下来
			_ = conn.Send("MULTI")
			_ = conn.Send("del", allGroup...)
			_ = conn.Send("del", redisAllGroupKey)
			_ = conn.Send("del", redisClientsHashKey)
			_, _ = conn.Do("EXEC")

			for _, item := range allGroup {
				logger.Info().Str("redis command",fmt.Sprint("DEL ", string(item.([]byte)))).Msg("执行redis命令")
			}
			logger.Info().Str("redis command", fmt.Sprint("DEL ", redisAllGroupKey)).Msg("执行redis命令")
			logger.Info().Str("redis command", fmt.Sprint("DEL ", redisClientsHashKey)).Msg("执行redis命令")
		} else {
			logger.Error().Err(err).Msg("redis error")
		}

		// 关闭redis池
		if redisPool != nil {
			_ = redisPool.Close()
		}

		logger.Info().Str("port", Port).Msg("关闭server服务")

		// 关闭日志文件
		if file != nil {
			file.Close()
		}

		os.Exit(1)
	}()

	// 关闭接口
	http.HandleFunc("/close", func(w http.ResponseWriter, r *http.Request) {
		/*
			1. 获取conn，关闭通道
			2. 从map中删除掉
			3. 连接数减一
		*/
		clientId := r.URL.Query().Get("clientId")
		if clientId == "" {
			return
		}
		s := manager.get(clientId)
		if s != nil && s.conn != nil {
			_ = s.conn.Close()
			manager.del(clientId)
			atomic.AddInt64(&manager.num, -1)
		}
		_ = delRedisClientId(clientId)

		body, _ := json.Marshal(ResponseJson{
			Code: 0,
			Msg:  "delete success",
			Data: "",
		})
		_, _ = w.Write(body)

		logger.Info().Str("clientId", clientId).Msg("删除websocket连接对象")
	})

	// 注册接口
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		/*
			1. 校验是否已经有了，有了的话，直接返回，不做操作
			2. 创建ws的链接
			3. 加入到map中
			4. 连接数+1
		*/
		clientId := r.URL.Query().Get("clientId")
		if clientId == "" {
			return
		}
		s := manager.get(clientId)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			return
		}
		nowTime := time.Now().Unix()
		s = &WsStruct{
			conn:         conn,
			registerTime: nowTime,
			latestTime:   nowTime,
			id:           clientId,
		}
		manager.set(clientId, s)
		atomic.AddInt64(&manager.num, 1)
		logger.Info().Str("clientId", clientId).Msg("创建websocket连接对象")

		var respBody ResponseJson

		// 校验是否存在group参数，存在的话，将clientId加入到group中
		group := r.URL.Query().Get("group")
		if group == "" {
			respBody.Code = 1
			respBody.Msg = "connection success!"
			_ = conn.WriteJSON(respBody)
			return
		}

		redisConn := redisPool.Get()
		defer func() {
			if redisConn != nil {
				_ = redisConn.Close()
			}
		}()

		// 执行 sadd操作不管返回 1还是0，都当作加入成功
		// 记录一下 clientId和Group之间的关系
		// TODO:: 记录redis执行的命令
		_ = redisConn.Send("MULTI")
		_ = redisConn.Send("SADD", redisPrefix+group, clientId)
		_ = redisConn.Send("SADD", redisAllGroupKey, redisPrefix+group)
		_ = redisConn.Send("HSET", redisClientsHashKey, clientId, redisPrefix+group)
		_, err = redisConn.Do("EXEC")
		if err != nil {
			respBody.Code = 0
			respBody.Msg = "join group fail!"
			_ = conn.WriteJSON(respBody)
			return
		}

		respBody.Code = 1
		respBody.Msg = "join group success!"
		_ = conn.WriteJSON(respBody)
		logger.Info().Str("clientId", clientId).Str("group", redisPrefix+group).Msg("加入group")
	})

	// 执行调用函数
	http.HandleFunc("/do", func(w http.ResponseWriter, r *http.Request) {
		/*
			http://127.0.0.1:9999/do?clientId=zhuyu&&methdo=add&params=["1","2"]
			clientId -> 字符串
			method -> 字符串
			params -> json格式的数组，这个数组中的每个元素就是参数

			jsRpc之间通信的格式为json格式字符串
			发送
			{
				method:"调用的方法名"
				params:"调用的参数"  // json格式的数组
			}
			接收
			{
				method:"调用的方法名"
				data:"返回值" // json格式的数组
			}
		*/

		/*
			do这个接口的响应格式为:
			{
				code:"状态码"
				msg:"描述信息"
				data:"数据"
			}

		*/
		var respBody ResponseJson
		clientId := r.URL.Query().Get("clientId")
		method := r.URL.Query().Get("method")
		params := r.URL.Query().Get("params")

		if method == "" {
			respBody.Code = 0
			respBody.Msg = "缺少method参数，不能为空"
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		if params == "" {
			respBody.Code = 0
			respBody.Msg = "缺少params参数，不能为空，如果调用函数没有参数的话，请提交json格式的空数组"
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		s := manager.get(clientId)
		if s == nil {
			respBody.Code = 0
			respBody.Msg = "clientId 未注册"
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		// 10s时间内执行完毕
		_respData, err := DoClient(s, method, params)
		if err != nil {
			respBody.Code = 0
			respBody.Msg = err.Error()
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		respBody.Code = 1
		respBody.Msg = "success"
		respBody.Data = []interface{}{_respData.Data}
		body, _ := json.Marshal(respBody)

		_, _ = w.Write(body)

	})

	// 调用一个group
	http.HandleFunc("/doMany", func(w http.ResponseWriter, r *http.Request) {
		var respBody ResponseJson
		group := r.URL.Query().Get("group")
		if group == "" {
			respBody.Code = 0
			respBody.Msg = "缺少group参数，不能为空"
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		method := r.URL.Query().Get("method")
		params := r.URL.Query().Get("params")

		if method == "" {
			respBody.Code = 0
			respBody.Msg = "缺少method参数，不能为空"
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		if params == "" {
			respBody.Code = 0
			respBody.Msg = "缺少params参数，不能为空，如果调用函数没有参数的话，请提交json格式的空数组"
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		conn := redisPool.Get()
		defer func() {
			if conn != nil {
				_ = conn.Close()
			}
		}()

		clientIdArray, err := redis.Strings(conn.Do("SMEMBERS", redisPrefix+group))
		if err != nil {
			respBody.Code = 0
			respBody.Msg = "redis error => " + err.Error()
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		// 查询这个key是否存在,不存在的话 clientIdArray就是空切片
		if len(clientIdArray) == 0 {
			respBody.Code = 0
			respBody.Msg = fmt.Sprintf("group =>:%s不存在", group)
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		// 添加一个group，等待所有协程执行完毕
		wg := sync.WaitGroup{}
		var resultArray []interface{}
		for _, clientId := range clientIdArray {
			wg.Add(1)

			// 开启协程去执行 每个websocket conn
			go func(id string) {
				defer wg.Add(-1)

				// 可能存在clientId长时间未使用，被删除掉了
				s := manager.get(id)
				if s == nil {
					respBody.Code = 0
					respBody.Msg = "clientId已失效或未注册"
					respBody.Data = ""
				}

				_respData, err := DoClient(s, method, params)
				if err != nil {
					logger.Error().Err(err).Msg("DoClient error")
					return
				}

				// 将返回内容添加到resultArray中
				resultArray = append(resultArray, _respData.Data)

			}(clientId)
		}

		// 等待所有任务执行完毕
		wg.Wait()

		respBody.Code = 1
		respBody.Msg = "success"
		respBody.Data = resultArray
		body, _ := json.Marshal(respBody)
		_, _ = w.Write(body)
	})

	// TODO::放线上加上权限认证
	// 对一个Group进行删除，查询操作
	http.HandleFunc("/group", func(w http.ResponseWriter, r *http.Request) {
		method := r.Method
		groupName := r.URL.Query().Get("group")
		var respBody ResponseJson
		if groupName == "" {
			respBody.Code = 0
			respBody.Msg = "缺少group参数，不能为空"
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		// 只能读取，删除ALLGROUP中的数据,怕误删除其他字段
		redisConn := redisPool.Get()
		defer func() {
			if redisConn != nil {
				_ = redisConn.Close()
			}
		}()

		isIn, err := redis.Bool(redisConn.Do("SISMEMBER", redisAllGroupKey, redisPrefix+groupName))
		if err != nil {
			respBody.Code = 0
			respBody.Msg = "redis error => " + err.Error()
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		if !isIn {
			respBody.Code = 0
			respBody.Msg = fmt.Sprintf("group =>:%s不存在", groupName)
			respBody.Data = ""
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		// 这里表示 groupName 是存在的
		switch strings.ToUpper(method) {
		case "GET":
			clientIdArray, err := redis.Strings(redisConn.Do("SMEMBERS", fmt.Sprintf("%s%s", redisPrefix, groupName)))
			if err != nil {
				respBody.Code = 0
				respBody.Msg = "redis error => " + err.Error()
				respBody.Data = ""
				body, _ := json.Marshal(respBody)
				_, _ = w.Write(body)
				return
			}

			// 返回成功的数据
			respBody.Code = 1
			respBody.Msg = "success"
			respBody.Data = clientIdArray

		case "DELETE":
			// 这里只是将redis中的group删除了，并不会删除clientId，相当于说就单单接触了group个clientId之间的关联，
			// 继续删除 ALLGROUP 中的值
			// TODO:: 记录redis命令
			_ = redisConn.Send("MULTI")
			_ = redisConn.Send("DEL", fmt.Sprintf("%s%s", redisPrefix, groupName))
			_ = redisConn.Send("SREM", redisAllGroupKey, fmt.Sprintf("%s%s", redisPrefix, groupName))
			_, err = redisConn.Do("EXEC")
			if err != nil {
				respBody.Code = 0
				respBody.Msg = "redis error => " + err.Error()
				respBody.Data = ""
				body, _ := json.Marshal(respBody)
				_, _ = w.Write(body)
				return
			}

			// 返回成功的数据,
			respBody.Code = 1
			respBody.Msg = "success"
			respBody.Data = ""
		default:
			// 目前只支持 get和delete
			respBody.Code = 0
			respBody.Msg = "只支持 GET 和 DELETE 请求"
			respBody.Data = ""
		}

		body, _ := json.Marshal(respBody)
		_, _ = w.Write(body)

	})

	// 获取当前存在的所有group
	http.HandleFunc("/groups", func(w http.ResponseWriter, r *http.Request) {
		var respBody ResponseJson
		if strings.ToUpper(r.Method) != "GET" {
			respBody.Code = 0
			respBody.Msg = "仅支持GET请求方式"
			body, _ := json.Marshal(respBody)
			_, _ = w.Write(body)
			return
		}

		// TODO:: 需要加上权限

		redisConn := redisPool.Get()
		groupArray, err := redis.Strings(redisConn.Do("SMEMBERS", redisAllGroupKey))
		if err != nil {
			respBody.Code = 0
			respBody.Msg = "redis error"
		} else {
			_groupArray := make([]string, 0, len(groupArray))
			// 需要去掉前缀

			for _, item := range groupArray {
				_item := strings.TrimLeft(item, redisPrefix)
				_groupArray = append(_groupArray, _item)
			}

			respBody.Code = 1
			respBody.Msg = "success"
			respBody.Data = _groupArray
		}

		body, _ := json.Marshal(respBody)
		_, _ = w.Write(body)
		return

	})

	/*
	 使用websocket链接进行ping，如果没有反应的话，则删除这个clientId,从集合，HASH表中删除，
	*/
	go func() {
		for {
			select {
			// TODO:: 这里设置30秒检测一次，需要根据真实情况考虑
			case <-time.After(30 * time.Second):
				manager._map.Range(func(key, value interface{}) bool {
					s := value.(*WsStruct)
					if s == nil || s.conn == nil {
						logger.Info().Str("clientId", key.(string)).Msg("删除websocket连接对象")
						manager._map.Delete(key)
						atomic.AddInt64(&manager.num, -1)
						return true
					}
					// 开始ping，正常返回pong，如果超时了，代表已经关闭了
					okChan := make(chan bool)
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						defer cancel()
						done := make(chan struct{})
						go func() {
							_, _, err := s.conn.ReadMessage()
							if err != nil {
								return
							}
							done <- struct{}{}
						}()
						_ = s.conn.WriteMessage(websocket.TextMessage, []byte("ping"))

						select {
						case <-done:
							okChan <- true
						case <-ctx.Done():
							// 超时,通信失败，但也有可能网络由原故，但我还是算作为失败吧
							okChan <- false
						}
					}()

					select {
					case ok := <-okChan:
						if !ok {
							// 删除id,删除group中的id
							logger.Info().Str("clientId", key.(string)).Msg("删除websocket连接对象")
							manager._map.Delete(key)
							atomic.AddInt64(&manager.num, -1)
							_ = delRedisClientId(key.(string))
						}
					}
					return true
				})
			}
		}

	}()

	/*
	 清理一些没有长时间未使用的连接
	*/
	go func() {
		for {
			select {
			// 每30分钟去检测一次
			case <-time.After(30 * 60 * time.Second):
				manager._map.Range(func(key, value interface{}) bool {
					// 校验
					s := value.(*WsStruct)
					// 删除一些24小时未访问的通信
					if time.Now().Unix()-s.latestTime > 60*60*24 {
						if s.conn != nil {
							_ = s.conn.Close()
						}
						manager._map.Delete(key)
						atomic.AddInt64(&manager.num, -1)
						logger.Info().Str("clientId", key.(string)).Msg("删除websocket连接对象")
					}
					return true
				})
			}
		}
	}()

	// 输出当前的连接数
	go func() {
		for {
			select {
			case <-time.After(5 * 60 * time.Second):
				logger.Info().Int64("count", manager.num).Msg("存在连接数")
			}
		}
	}()

	logger.Info().Str("port", Port).Msg("开启server服务")
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", Port), nil))
}

/*
 使用ws于浏览器进行通信
 method：调用的方法
 params：传递的参数
*/
func DoClient(w *WsStruct, method, params string) (ResponseJson, error) {
	var req ReqStruct
	var respBody ResponseJson
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeOut)*time.Second)
	defer cancel()
	done := make(chan struct{})
	dataChan := make(chan []byte, 1)

	// 还是进行判断下，保险起见
	if w == nil {
		return respBody, errors.New("clientId 未注册")
	}

	go func() {
		_, content, err := w.conn.ReadMessage()
		if err != nil {
			// 直接返回的话，done队列会阻塞，不过有执行时间的限制(ctx)，没有影响
			return
		}
		dataChan <- content
		done <- struct{}{}
	}()

	req.Method = method
	req.Params = params
	reqContent, _ := json.Marshal(req)
	_ = w.conn.WriteMessage(websocket.TextMessage, reqContent)
	select {
	case <-done:
		respBody.Code = 0
		respBody.Msg = "success"
		respBody.Data = string(<-dataChan)

		// 更新时间戳
		nowTime := time.Now().Unix()
		w.latestTime = nowTime

	case <-ctx.Done():
		respBody.Code = 1
		respBody.Msg = "任务调用超时"
		respBody.Data = ""
	}

	if len(params) > 100 {
		logger.Info().Str("clientId", w.id).
			Str("method", method).
			Str("params", "").
			Msg("调用函数")
	} else {
		logger.Info().Str("clientId", w.id).
			Str("method", method).
			Str("params", params).
			Msg("调用函数")
	}
	return respBody, nil
}

/*
 在redis中删除一个clientId
 id：删除的clientId
*/
func delRedisClientId(id string) error {
	redisConn := redisPool.Get()
	defer func() {
		if redisConn != nil {
			_ = redisConn.Close()
		}
	}()
	group, err := redis.String(redisConn.Do("HGET", redisClientsHashKey, id))
	if err != nil {
		// 如果有错误的话，redis不做任何操作，单单删除Map中的ID就是
		log.Println("redis error =>", err.Error())
		return err
	}
	_ = redisConn.Send("MULTI")
	_ = redisConn.Send("HDEL", redisClientsHashKey, id)
	_ = redisConn.Send("SREM", group, id)
	_, err = redisConn.Do("EXEC")
	if err != nil {
		return err
	}
	return nil

}
