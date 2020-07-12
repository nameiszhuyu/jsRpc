let ZhuClient = function (url, id) {
    // 链接对象
    this.ws = new WebSocket(url)
    this.id = id
    this.ws.onopen = function (event) {
        console.log("连接服务器成功")
    }

    this.ws.onmessage = (event) => {
        // 做接受服务端信息的处理
        let dataStr = event.data
        console.log("服务端发送过来的信息:", dataStr)

        if (dataStr === "ping") {
            this.ws.send("pong")
        }

        let data = JSON.parse(dataStr)

        // 获取method 和 params
        let method = data["method"]
        let paramsStr = data["params"]

        // method为undefined的话，不传递数据
        if (method === undefined) {
            return
        }


        let callFunc = this.methodsPool[method]
        if (callFunc === undefined) {
            // 发送失败信息
            this.ws.send(JSON.stringify({
                "isSuccess": false,
                "msg": `未实现方法:${method}`,
                "data": "",
                "clientId": this.id
            }))
            return
        }

        let params = JSON.parse(paramsStr)
        let result = callFunc(...params)
        console.log(`调用函数:${method},执行结果为${result}`)
        this.ws.send(JSON.stringify({
            "isSuccess": true,
            "msg": "调用成功",
            "data": result,
            "clientId": this.id
        }))


    }

    // 方法容器
    this.methodsPool = {}

    // 注册方法
    this.registerMethod = function (name, f) {
        this.methodsPool[name] = f
        console.log("添加方法:", name, "成功!")
    }
}

function guid() {
    function S4() {
        return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
    }

    return (S4() + S4() + "-" + S4() + "-" + S4() + "-" + S4() + "-" + S4() + S4() + S4());
}

let clientId = guid()
let group = "dianzixiao"
let port = 9999
let ipAddress = "127.0.0.1"

console.log("clientId为:", clientId)
let url = `ws://${ipAddress}:${port}/register?clientId=${clientId}&group=${group}`
let ws = new ZhuClient(url, clientId)
window["ws"] = ws

let add = function (a, b) {
    return a + b
}
ws.registerMethod("add", add)