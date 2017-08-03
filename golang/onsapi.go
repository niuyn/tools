package ons

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	//"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	//"unicode/utf8"
	"encoding/json"
	//"fmt"
)

type AliOns struct {
	Topic           string
	AccessKey       string
	AccessKeySecret string
	Producer        string
	Consumer        string
	Tag             string
	Url             string
	BaseUrl         string
	Num             string
	SendChan        chan string
	ReceChan        chan string
	DelChan         chan string
}

type OnsRawMsg struct {
	Body           string `json:"body"`
	BornTime       string `json:"bornTime"`
	MsgHandle      string `json:"msgHandle"`
	MsgId          string `json:"msgId"`
	ReconsumeTimes int    `json:"reconsumeTimes"`
}

func NewProducer(topic, accessKey, accessKeySecret, tag, producer, url string) *AliOns {
	ons := AliOns{}
	ons.Topic = topic
	ons.AccessKey = accessKey
	ons.AccessKeySecret = accessKeySecret
	ons.Producer = producer
	ons.Tag = tag
	ons.Url = url
	return &ons
}

// 目前阿里云消费者不支持tag过滤
func NewConsumer(topic, accessKey, accessKeySecret, consumer, url, num string) *AliOns {
	ons := AliOns{}
	ons.Topic = topic
	ons.AccessKey = accessKey
	ons.AccessKeySecret = accessKeySecret
	ons.Consumer = consumer
	ons.Url = url
	ons.Num = num
	ons.BaseUrl = url + "?topic=" + topic + "&num=" + num
	return &ons
}

func (ons *AliOns) PushMsgToChan(msg string) bool {
	log.Debug("PushMsgToChan；", msg)
	//log.Info("PushMsgToChan111")
	if ons.SendChan == nil {
		log.Error("sendchan is not init")
		return false
	} else {
		//log.Info("PushMsgToChan222")
		select {
		case ons.SendChan <- msg:
		default:
			log.Debug("sendChan is overflow", <-ons.SendChan)
			return false
		}
	}
	return true
}

func (ons *AliOns) SendMsg() {
	log.Debug("start send message service ")
	ons.SendChan = make(chan string, 10000)
	requestSend, _ := http.NewRequest("POST", ons.Url, nil)
	requestSend.Header.Add("AccessKey", ons.AccessKey)
	requestSend.Header.Add("ProducerId", ons.Producer)

	go ons.startProducer(requestSend)
}

func (ons *AliOns) ReceviveMsg() {
	log.Debug("start receive message from ons ")
	ons.ReceChan = make(chan string, 10000)
	responseReceive, _ := http.NewRequest("GET", ons.BaseUrl, nil)
	responseReceive.Header.Set("AccessKey", ons.AccessKey)
	responseReceive.Header.Set("ConsumerId", ons.Consumer)
	ons.DeleteMsg()
	go ons.startConsumer(responseReceive)
}

func (ons *AliOns) startProducer(req *http.Request) {
	client := &http.Client{}
	for msg := range ons.SendChan {
		log.Debug("get a msg from send chan:", msg)
		//fmt.Println("get a msg from send chan ", msg)
		nowStr := getUnixTimeStr()
		signString := ons.Topic + "\n" + ons.Producer + "\n"
		signString = signString + getMD5Value(msg) + "\n"
		sign := ons.getSignture(signString, nowStr)
		req.Header.Set("Signature", sign)
		req.Body = ioutil.NopCloser(strings.NewReader(msg))
		rawurl := ons.Url + "?topic=" + ons.Topic + "&time=" + nowStr + "&tag=" + ons.Tag + "&key=" + ons.Tag
		req.URL, _ = url.Parse(rawurl)

		rsp, err := client.Do(req)
		if err != nil {
			log.Error("rsp, err := client.Do(req)：", req, " ERROR:", err)
			//放回队列
			log.Debug("send mq failed place back")
			ons.PushMsgToChan(msg)
			continue
		}

		//fmt.Println(rsp.Status)
		body, err := ioutil.ReadAll(rsp.Body)
		if err != nil {
			log.Error("body, err := ioutil.ReadAll(rsp.Body)：", " ", rsp.Body, err)
			rsp.Body.Close()
			return
		}
		bodystr := string(body)

		log.Debug("send mq success,bodystr：", bodystr, "  msg:", msg)
		rsp.Body.Close()
		//time.Sleep(5 * time.Second)
	}
}

func (ons *AliOns) startConsumer(req *http.Request) {
	client := &http.Client{}
	for {
		nowstr := getUnixTimeStr()
		signString := ons.Topic + "\n" + ons.Consumer + "\n"
		sign := ons.getSignture(signString, nowstr)

		extraStr := ons.BaseUrl + "&time=" + nowstr
		req.URL, _ = url.Parse(extraStr)
		log.Debug("req.URL.String()：", req.URL.String())
		req.Header.Set("Signature", sign)
		rsp, err := client.Do(req)
		if err != nil {
			log.Error("rsp, err := client.Do(req):", err)
			//fmt.Println(err)
			break
		}
		//	fmt.Println(rsp.Status)
		log.Debug("rsp.Status: ", rsp.Status)
		if rsp.StatusCode == 200 {
			body, _ := ioutil.ReadAll(rsp.Body)
			bodystr := string(body)
			//fmt.Println(bodystr)
			log.Debug("bodystr： ", bodystr)
			var onsMsgs []OnsRawMsg
			err := json.Unmarshal(body, &onsMsgs)
			if err == nil {
				for i := 0; i < len(onsMsgs); i++ {
					if ons.ReceChan != nil {
						select {
						case ons.ReceChan <- onsMsgs[i].Body:
							// 接收完成后需要删除
						default:
							log.Debug("ReceChan is overflow", <-ons.ReceChan)
						}

					}
					if ons.DelChan != nil {
						select {
						case ons.DelChan <- onsMsgs[i].MsgHandle:
							// 接收完成后需要删除
						default:
							log.Debug("Delete is overflow", <-ons.ReceChan)
						}

					}
				}
			}
		}
		rsp.Body.Close()
		time.Sleep(1 * time.Second)
	}

}

func getUnixTimeStr() string {
	now := time.Now().UnixNano() / 1000000
	nowstr := strconv.FormatInt(now, 10)
	return nowstr
}

func getMD5Value(msg string) string {
	md5Ctx := md5.New()
	md5Ctx.Write([]byte(msg))
	return hex.EncodeToString(md5Ctx.Sum(nil))
}

func (ons *AliOns) getSignture(sign, extraStr string) string {

	key := []byte(ons.AccessKeySecret)
	mac := hmac.New(sha1.New, key)
	sign = sign + extraStr
	//fmt.Println(sign)
	mac.Write([]byte(sign))

	raw := mac.Sum(nil)
	return base64.StdEncoding.EncodeToString(raw)

}

//消息get之后需要在15分钟之内delete，如果在15分钟之后delete，消息可能会被重新get到
func (ons *AliOns) DeleteMsg() {
	log.Debug("start receive message from ons ")
	ons.DelChan = make(chan string, 10000)
	reqDelete, _ := http.NewRequest("DELETE", ons.Url, nil)
	reqDelete.Header.Set("AccessKey", ons.AccessKey)
	reqDelete.Header.Set("ConsumerId", ons.Consumer)
	go ons.startDelete(reqDelete)
}

func (ons *AliOns) startDelete(req *http.Request) {
	client := &http.Client{}
	for msg := range ons.DelChan {
		log.Debug("get a msg ready to delete:", msg)
		//fmt.Println("get a msg from send chan ", msg)
		nowStr := getUnixTimeStr()
		signString := ons.Topic + "\n" + ons.Consumer + "\n" + msg + "\n"
		sign := ons.getSignture(signString, nowStr)
		req.Header.Set("Signature", sign)
		req.Body = ioutil.NopCloser(strings.NewReader(msg))
		rawurl := ons.Url + "?topic=" + ons.Topic + "&time=" + nowStr + "&msgHandle=" + msg
		req.URL, _ = url.Parse(rawurl)

		rsp, err := client.Do(req)
		if err != nil {
			log.Error("rsp, err := client.Do(req)：", req, " ERROR:", err)
			//放回队列
			log.Debug("delete mq failed")
			continue
		}
		body, err := ioutil.ReadAll(rsp.Body)
		if err != nil {
			log.Error("body, err := ioutil.ReadAll(rsp.Body)：", " ", rsp.Body, err)
			rsp.Body.Close()
			return
		}
		bodystr := string(body)
		if rsp.StatusCode != 204 {
			log.Error("delete msg failed ：", " ", bodystr)
		}

		rsp.Body.Close()
		//time.Sleep(5 * time.Second)
	}
}

func (ons *AliOns) GetResultSet() chan string {
	return ons.ReceChan
}
