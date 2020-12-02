import os
import yaml


def getValue(key, default=None):
    return globalState.get(key, default)


def setValue(key, value):
    globalState[key] = value


projectDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
configFilePath = os.path.join(projectDir, "config.yaml")
with open(configFilePath, encoding="utf-8") as configFile:
    config = yaml.safe_load(configFile)
dataBaseDir = os.path.join(
    projectDir, config["dataBaseDir"] if config["dataBaseDir"] is not None else "data")
weiboHeaders = {
    "Host": "m.weibo.cn",
    "Connection": "close",
    "Accept": "application/json, text/plain, */*",
    "MWeibo-Pwa": "1",
    "X-XSRF-TOKEN": config["weiboHeaderCookie"].split("XSRF-TOKEN=")[-1],
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": config["weiboHeaderUA"],
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://m.weibo.cn",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://m.weibo.cn/message/chat?uid=5175429989&name=msgbox",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7",
    "Cookie": config["weiboHeaderCookie"]
}
globalState = {
    "wechatToken": None,
    "wechatTokenAvailable": True,
    "pendingWechatMessages": [],
    "weiboToken": weiboHeaders["X-XSRF-TOKEN"]
}


logFilesRelativeDir = config["logFilesDir"] if config["logFilesDir"] is not None else "logs"
logFilesDir = os.path.join(projectDir, logFilesRelativeDir)
logConfigDict = {
    "version": 1,
    "formatters": {
        "customFormatter": {
            "()": "tornado.log.LogFormatter"
        }
    },
    "handlers": {
        "accessHandler": {
            "formatter": "customFormatter"
        },
        "applicationHandler": {
            "formatter": "customFormatter"
        },
        "generalHandler": {
            "formatter": "customFormatter"
        },
        "sqliteHandler": {
            "formatter": "customFormatter"
        },
        "schedulerHandler": {
            "formatter": "customFormatter"
        }
    },
    "loggers": {
        "tornado.access": {
            "handlers": ["accessHandler"],
            "propagate": False
        },
        "tornado.application": {
            "handlers": ["applicationHandler"],
            "propagate": False
        },
        "tornado.general": {
            "handlers": ["generalHandler"],
            "propagate": False
        },
        "aiosqlite": {
            "handlers": ["sqliteHandler"],
            "propagate": False
        },
        "scheduler": {
            "handlers": ["schedulerHandler"],
            "propagate": False
        }
    }
}


qywxApiUrl = "https://qyapi.weixin.qq.com/cgi-bin"
weiboApiUrl = "https://m.weibo.cn/api/chat"


cacheFilePath = os.path.join(dataBaseDir, "cache.db")
cacheTableStructure = "key varchar(50) primary key, value blob not null"
starting = 0
running = 1
stopping = 2


stateStopped = 0
stateRunning = 1
statePaused = 2
jobStoreRetryInterval = 3
jobStoreTableStructure = "id varchar(50) primary key, nextRunTime float(25), state blob not null"
timeoutMax = 2629800
tasksFilePath = os.path.join(dataBaseDir, "tasks.db")


blockSize = 32
passiveResponsePacket = '''<xml>
<Encrypt><![CDATA[{}]]></Encrypt>
<MsgSignature><![CDATA[{}]]></MsgSignature>
<TimeStamp>{}</TimeStamp>
<Nonce><![CDATA[{}]]></Nonce>
</xml>'''
