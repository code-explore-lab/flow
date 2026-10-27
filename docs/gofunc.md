## 启动TCP监听

```go
listener, err := net.Listen("tcp", ":8554")
defer listener.Close()


for {
    /*`Accept()` 是**阻塞等待新 TCP 客户端连接**的方法：
监听 socket 已经准备好了（`net.Listen` 成功），它就卡在这，直到有客户端完成 TCP 三次握手连上来；成功就返回一个代表这个客户端连接的 `net.Conn`；失败返回 error。 */
	conn, err := listener.Accept()
}
// 给TCP连接设置截止时间
_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))

//作用：**把一整块字符串，按照子串 `\r\n` 切割，切成一行一行的字符串切片 `[]string`**
strings.Split(block, "\r\n")


```

## Error %问题

```txt
- `%v`：**打印 error 的内容（给人看）**，单纯输出错误文本，不包装错误链。
- `%w`：**只用于 `fmt.Errorf` 里面包装 error（代码层面嵌套错误链），不能用来打印！**
```

## 切片展开

```go
// 字节拷贝
buf = append(buf, raw...)
//`raw...` 是 **切片展开（slice expansion）**，把 `raw` 里面每一个 byte 打散，依次传给 `append` 的可变参数。
```

### `strings.Fields(s)`

**功能：把字符串按【任意空白（空格、tab）】切分，自动忽略首尾空白、连续多个空白合并成一个分隔，返回 [] string**

### `strings.TrimSpace(s)`

作用：**删掉字符串两头的空白字符**
空白字符包含：空格、`\t`、`\r`、`\n`。

### `strings.SplitN(s, sep, n)`

`kv := strings.SplitN(line, ":", 2)`

- 含义：**以 `:` 作为分隔符，最多切分成 2 段**

### `strconv.Atoi(s string) (int, error)`

**Atoi = ASCII to integer**：把**数字字符串转成 int 整数**。

## `br := bufio.NewReader(conn)`

```go
 line, err := br.ReadString('\n')
`conn` 本身是 `io.Reader`，直接 `conn.Read()` 是**无缓冲原生读**
```

### HasPrefix(s, prefix string)

> 判断s有没有prefix前缀

## strings.Builder 字符串缓冲
