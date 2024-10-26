import asyncio
#多线程
import threading
import my_globals

from flask import Flask, request, jsonify
import arrow

import sys
from my_log import logger

from mq_http_sdk.mq_exception import MQExceptionBase
from mq_http_sdk.mq_producer import *
from mq_http_sdk.mq_consumer import *
from mq_http_sdk.mq_client import *
from aiohttp import web
import time

app = Flask(__name__)


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "DELETE"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def hello_world(path):
    #  requestId = request.headers.get("x-fc-request-id")
    print("FC Invoke Start RequestId: ")

    response = jsonify(
        {
            "msg": "Hello, World!" + " at " + arrow.now().format("YYYY-MM-DD HH:mm:ss"),
            "request": {
                "query": str(request.query_string, "utf-8"),
                "path": path,
                "data": str(request.stream.read(), "utf-8"),
                "clientIp": request.headers.get("x-forwarded-for"),
            },
        }
    )

    print("FC Invoke End RequestId: ")
    return response


#@app.route("/sendmsg", methods=["GET", "POST", "PUT", "DELETE"])
async def sendmsg(request):
    #  requestId = request.headers.get("x-fc-request-id")
    print("FC Invoke Start RequestId: ")
    # 初始化client。
    mq_client = MQClient(
        # 设置HTTP协议客户端接入点，进入消息队列RocketMQ版控制台实例详情页面的接入点区域查看。
        "http://1924093879995655.mqrest.cn-hangzhou.aliyuncs.com",
        # 请确保环境变量ALIBABA_CLOUD_ACCESS_KEY_ID、ALIBABA_CLOUD_ACCESS_KEY_SECRET已设置。
        # AccessKey ID，阿里云身份验证标识。
        os.environ['AK'],
        # AccessKey Secret，阿里云身份验证密钥。
        os.environ['SK']
    )
    # 消息所属的Topic，在消息队列RocketMQ版控制台创建。
    topic_name = "test"
    # Topic所属的实例ID，在消息队列RocketMQ版控制台创建。
    # 若实例有命名空间，则实例ID必须传入；若实例无命名空间，则实例ID传入空字符串。实例的命名空间可以在消息队列RocketMQ版控制台的实例详情页面查看。
    instance_id = "MQ_INST_1924093879995655_BZKHx9vF"

    producer = mq_client.get_producer(instance_id, topic_name)

    # 循环发送4条消息。
    msg_count = 4
    print("%sPublish Message To %sTopicName:%s\nMessageCount:%s" % (10 * "=", 10 * "=", topic_name, msg_count))


    msg = TopicMessage(
        # 消息内容。
        "I am test message %s.hello 1",
        # 消息标签。
        "taga"
    )
    # 设置消息的Key。
    msg.set_message_key("msgkey")
    re_msg = producer.publish_message(msg)
    print("Publish Message Succeed. MessageID:%s, BodyMD5:%s" % (re_msg.message_id, re_msg.message_body_md5))
    print("==========Publish Message To end==========")

    return web.Response(text="send async success!")



@app.route("/msg", methods=["GET", "POST", "PUT", "DELETE"])
def msg():
    #  requestId = request.headers.get("x-fc-request-id")
    print("FC Invoke Start RequestId: ")
    # 初始化client。
    mq_client = MQClient(
        # 设置HTTP协议客户端接入点，进入消息队列RocketMQ版控制台实例详情页面的接入点区域查看。
        "http://1924093879995655.mqrest.cn-hangzhou.aliyuncs.com",
        # 请确保环境变量ALIBABA_CLOUD_ACCESS_KEY_ID、ALIBABA_CLOUD_ACCESS_KEY_SECRET已设置。
        # AccessKey ID，阿里云身份验证标识。
        os.environ['AK'],
        # AccessKey Secret，阿里云身份验证密钥。
        os.environ['SK']
    )
    # 消息所属的Topic，在消息队列RocketMQ版控制台创建。
    topic_name = "test"
    # Topic所属的实例ID，在消息队列RocketMQ版控制台创建。
    # 若实例有命名空间，则实例ID必须传入；若实例无命名空间，则实例ID传入空字符串。实例的命名空间可以在消息队列RocketMQ版控制台的实例详情页面查看。
    instance_id = "MQ_INST_1924093879995655_BZKHx9vF"
    group_id = "GID_test"


    consumer = mq_client.get_consumer(instance_id, topic_name, group_id)

    # 长轮询表示如果Topic没有消息，则客户端请求会在服务端挂起3秒，3秒内如果有消息可以消费则立即返回响应。
    # 长轮询时间3秒（最多可设置为30秒）。
    wait_seconds = 16
    # 一次最多消费3条（最多可设置为16条）。
    batch = 1
    print(("%sConsume And Ak Message From Topic%s\nTopicName:%s\nMQConsumer:%s\nWaitSeconds:%s\n" \
           % (10 * "=", 10 * "=", topic_name, group_id, wait_seconds)))
    while True:
        if my_globals.my_global_var > 90:
            break
        else:
            try:
                # 长轮询消费消息。
                recv_msgs = consumer.consume_message(batch, wait_seconds)
                for msg in recv_msgs:
                    print("==========Receive Message To start==========")
                    print(("Receive, MessageId: %s\nMessageBodyMD5: %s  MessageTag: %s\nConsumedTimes: %s  PublishTime: %s\nBody: %s \
                                  tNextConsumeTime: %s \
                                  tReceiptHandle: %s \
                                  tProperties: %s\n" % \
                       (msg.message_id, msg.message_body_md5,
                        msg.message_tag, msg.consumed_times,
                        msg.publish_time, msg.message_body,
                        msg.next_consume_time, msg.receipt_handle, msg.properties)))

                    time.sleep(3)  # 消费消息休息3s
                    print("==========Receive Message To end==========")
            except MQExceptionBase as e:
                # Topic中没有消息可消费。
                if e.type == "MessageNotExist":
                    print(("No new message! RequestId: %s" % e.req_id))
                    continue

                print(("Consume Message Fail! Exception:%s\n" % e))
                time.sleep(2)
                continue

        # msg.next_consume_time前若不确认消息消费成功，则消息会被重复消费。
        # 消息句柄有时间戳，同一条消息每次消费拿到的都不一样。
            try:
                receipt_handle_list = [msg.receipt_handle for msg in recv_msgs]
                consumer.ack_message(receipt_handle_list)
                print(("Ak %s Message Succeed.\n\n" % len(receipt_handle_list)))
            except MQExceptionBase as e:
                print(("\nAk Message Fail! Exception:%s" % e))
                # 某些消息的句柄可能超时，会导致消息消费状态确认不成功。
                if e.sub_errors:
                    for sub_error in e.sub_errors:
                        print(("\tErrorHandle:%s,ErrorCode:%s,ErrorMsg:%s" % \
                               (sub_error["ReceiptHandle"], sub_error["ErrorCode"], sub_error["ErrorMessage"])))
    print("end msg consumer")
    return "23"


async def hello(request):
    #time.sleep(10)  # 模拟异步操作，如数据库查询
  #  await asyncio.run(msg())
    logger.info("welcome hello world")
    print("hello world")
    print(request)
    request.body = await request.read()
    print(request.body)
    return web.Response(text="Hello, async world!")

async def delmsg(request):
    #time.sleep(10)  # 模拟异步操作，如数据库查询
    #  await asyncio.run(msg())
    my_globals.my_global_var = 100
    return web.Response(text="Hello, delmsg")

async def startmsg(request):
    consumer_thread = threading.Thread(target=msg)
    consumer_thread.daemon = True  # 设置为守护线程，主程序退出时会一起退出
    consumer_thread.start()
    my_globals.my_global_var = 50
    return web.Response(text="Hello, start msg")


async def initpy(hello):
    try :
        startmsg = os.environ['MSG']
        if startmsg == "msg":
            consumer_thread = threading.Thread(target=msg)
            consumer_thread.daemon = True  # 设置为守护线程，主程序退出时会一起退出
            consumer_thread.start()
    except Exception as e:
        print(f"An error occurred: {e}")
    return "hello world"


app = web.Application()
# 使用aiohttp的信号系统，在应用启动时绑定并调用my_startup_task
app.on_startup.append(initpy)

app.add_routes([web.get('/py/sendmsg', sendmsg),
                web.get('/py/msg', msg),
                web.get('/py/hello', hello),
                web.get('/py/delmsg', delmsg),
                web.get('/py/startmsg', startmsg),
                web.post("/py/post", hello)
                ])
# app.add_routes([web.get('/py/msg', msg)])
# app.add_routes([web.get('/py/hello', hello)])
# app.add_routes([web.get('/py/delmsg', delmsg)])

if __name__ == "__main__":
    #app.run(host="0.0.0.0", port=9000)
    web.run_app(app, host="0.0.0.0", port=9000)
