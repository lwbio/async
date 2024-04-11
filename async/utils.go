package async

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/lwbio/async/encoding"
	"github.com/rabbitmq/amqp091-go"
)

const (
	baseContentType = "application"
)

func Ex(et PbEvent) string {
	return fmt.Sprintf("%s.%s", et.Descriptor().Parent().FullName(), strings.ToLower(et.String()))
}

func Qn(et PbEvent) string {
	return fmt.Sprintf("%s.%s", et.Descriptor().Parent().FullName(), strings.ToLower(et.String()))
}

func Id(et PbEvent) string {
	return string(et.Descriptor().Parent().FullName())
}

func QnFn(id string, fn string) string {
	return fmt.Sprintf("%s.%s", id, fn)
}

/**
 * 驼峰转蛇形 snake string
 * @description XxYy to xx_yy , XxYY to xx_y_y
 * @date 2020/7/30
 * @param s 需要转换的字符串
 * @return string
 **/
func ToSnake(s string) string {
	data := make([]byte, 0, len(s)*2)
	j := false
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		// or通过ASCII码进行大小写的转化
		// 65-90（A-Z），97-122（a-z）
		//判断如果字母为大写的A-Z就在前面拼接一个_
		if i > 0 && d >= 'A' && d <= 'Z' && j {
			data = append(data, '_')
		}
		if d != '_' {
			j = true
		}
		data = append(data, d)
	}
	//ToLower把大写字母统一转小写
	return strings.ToLower(string(data[:]))
}

/**
 * 蛇形转驼峰
 * @description xx_yy to XxYx  xx_y_y to XxYY
 * @date 2020/7/30
 * @param s要转换的字符串
 * @return string
 **/
func ToCamel(s string) string {
	data := make([]byte, 0, len(s))
	j := false
	k := false
	num := len(s) - 1
	for i := 0; i <= num; i++ {
		d := s[i]
		if !k && d >= 'A' && d <= 'Z' {
			k = true
		}
		if d >= 'a' && d <= 'z' && (j || !k) {
			d = d - 32
			j = false
			k = true
		}
		if k && d == '_' && num > i && s[i+1] >= 'a' && s[i+1] <= 'z' {
			j = true
			continue
		}
		data = append(data, d)
	}
	return string(data[:])
}
func GetFunctionName(i interface{}, seps ...rune) string {
	// 获取函数名称
	fn := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()

	// 用 seps 进行分割
	fields := strings.FieldsFunc(fn, func(sep rune) bool {
		for _, s := range seps {
			if sep == s {
				return true
			}
		}
		return false
	})

	if size := len(fields); size > 0 {
		return strings.TrimSuffix(fields[size-1], "-fm")
	}
	return ""
}

func ContentSubtype(contentType string) string {
	left := strings.Index(contentType, "/")
	if left == -1 {
		return ""
	}
	right := strings.Index(contentType, ";")
	if right == -1 {
		right = len(contentType)
	}
	if right < left {
		return ""
	}
	return contentType[left+1 : right]
}

// CodecForRequest get encoding.Codec via amqp091.Delivery
func CodecForRequest(r *amqp091.Delivery) (encoding.Codec, bool) {
	codec := encoding.GetCodec(ContentSubtype(r.ContentType))
	if codec != nil {
		return codec, true
	}

	return encoding.GetCodec("json"), false
}

// ContentType returns the content-type with base prefix.
func ContentType(subtype string) string {
	return strings.Join([]string{baseContentType, subtype}, "/")
}
