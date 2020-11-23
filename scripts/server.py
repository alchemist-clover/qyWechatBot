from tornado import web, ioloop, httpserver
from functools import wraps
from xml.etree.cElementTree import fromstring


from log import generalLogger
from crypt import verifyUrl, decryptMsg
from messager import chat, sendWechatMessage


def getConnectionCount():
    return connectionCount


def countConnection(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        global connectionCount
        connectionCount += 1
        func(self, *args, **kwargs)
        connectionCount -= 1
    return wrapper


class __callbackHandler(web.RequestHandler):
    @countConnection
    def get(self):
        msgSignature = self.get_query_argument("msg_signature", None)
        timestamp = self.get_query_argument("timestamp", None)
        nonce = self.get_query_argument("nonce", None)
        echoString = self.get_query_argument("echostr", None)
        responseString = verifyUrl(msgSignature, timestamp, nonce, echoString)
        if responseString is None:
            generalLogger.info("Input error, ignore this request.")
            self.set_status(200)
        else:
            generalLogger.info("Response successfully!")
            self.write(responseString)

    @countConnection
    def post(self):
        msgSignature = self.get_query_argument("msg_signature", None)
        timestamp = self.get_query_argument("timestamp", None)
        nonce = self.get_query_argument("nonce", None)
        xmlText = decryptMsg(self.request.body.decode(
            "utf-8"), msgSignature, timestamp, nonce)
        if xmlText is not None:
            xmlTree = fromstring(xmlText)
            fromId = xmlTree.find("FromUserName").text
            messageType = xmlTree.find("MsgType").text
            generalLogger.info("Message parsed successfully!")
            if messageType == "text":
                __ioLoop.add_callback(
                    chat, fromId, content=xmlTree.find("Content").text)
            else:
                __ioLoop.add_callback(
                    sendWechatMessage, content="暂不支持非文本类消息哦~", touser=fromId)
        else:
            generalLogger.debug("Input error, ignore this request.")
        self.set_status(200)


connectionCount = 0
__ioLoop = ioloop.IOLoop.current()
__application = web.Application([(r"/callback", __callbackHandler)])
httpServer = httpserver.HTTPServer(__application)
