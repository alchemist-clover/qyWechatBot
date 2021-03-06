import asyncio
import aiohttp
from tornado import locks
from datetime import datetime, timedelta


from configs import config, getValue, setValue, qywxApiUrl, weiboApiUrl, weiboHeaders
from log import generalLogger
from scheduler import taskScheduler


def urljoin(base, *options):
    return "/".join((base,) + options)


async def chat(fromId, token=None, messageType="text", **args):
    if token is None:
        token = getValue("weiboToken")
    postDict = __getWeiboPostDict(messageType, **args)
    async with __chatLock:
        sendTimestamp = datetime.utcnow().timestamp()
        sendFlag = await __sendWeiboMessage(token, postDict)
        if sendFlag:
            responseMessages = await __getWeiboMessage(sendTimestamp)
        else:
            responseMessages = ["消息发送失败，请稍后重试~"]
    for index, message in enumerate(responseMessages):
        sendFlag = await sendWechatMessage(content=message, touser=fromId)
        if not sendFlag:
            generalLogger.warning("Network error, ignore the remaining {} message(s).".format(
                len(responseMessages)-index))
            return
    generalLogger.debug(
        "Send {} message(s) successfully!".format(len(responseMessages)))


async def sendWechatMessage(token=None, messageType="text", tokenInvalidSaved=False, **args):
    if token is None:
        token = getValue("wechatToken")
    if not (tokenInvalidSaved or getValue("wechatTokenAvailable")):
        generalLogger.info(
            "WechatToken cannot be gotten and tokenInvalidSaved is false, so ignoring this wechat message.")
        return False
    postDict = __getWechatPostDict(messageType, **args)
    if not getValue("wechatTokenAvailable"):
        __saveWechatMessage(postDict)
        return False
    return await __sendWechatMessage(token, postDict, tokenInvalidSaved)


def __getWeiboPostDict(messageType, **args):
    postDict = {
        "uid": 5175429989,
        "st": getValue("weiboToken")
    }
    if messageType == "text":
        postDict["content"] = args["content"]
    return postDict


def __getWechatPostDict(messageType, **args):
    postDict = {
        "msgtype": messageType,
        "agentid": config["agentId"],
        messageType: {}
    }
    if messageType == "text":
        postDict[messageType]["content"] = args.pop("content")
        postDict.update(args)
    return postDict


def __saveWechatMessage(postDict):
    pendingWechatMessages = getValue("pendingWechatMessages")
    if len(pendingWechatMessages) >= config["maxPendingWechatMessages"]:
        generalLogger.warning(
            "MaxPendingWechatMessages has been reached, ignoring this wechat message.")
    else:
        generalLogger.info(
            "WechatToken cannot be gotten temporarily, saved this wechat message until the token can be gotten.")
        pendingWechatMessages.append(postDict)


async def __sendWeiboMessage(token, postDict):
    getUrl = urljoin(weiboApiUrl, "send")
    tryCount = 0
    sendFlag = False
    async with aiohttp.ClientSession() as session:
        while tryCount < config["maxTryCount"]:
            errorType = 0
            try:
                response = await session.post(getUrl, headers=weiboHeaders, data=postDict)
            except Exception as e:
                errorMessage = "Network connection error, will retry in two seconds, here is the error message:\n{}".format(
                    e)
                errorType = 1
            else:
                responseDict = await response.json()
                if responseDict["ok"] == 1:
                    generalLogger.info("This weibo message has been sent!")
                    sendFlag = True
                elif responseDict["errno"] == "100006":
                    await __getWeiboToken()
                    if getValue("weiboToken") == token:
                        generalLogger.info(
                            "WeiboToken cannot be gotten, ignoring this weibo message.")
                    else:
                        generalLogger.info(
                            "Retry sending the message with the new token.")
                        errorType = 2
                else:
                    generalLogger.warning(
                        "An unresolved error occurred, here is the error number: {}".format(responseDict["errno"]))
            finally:
                tryCount += 1
                if errorType == 0:
                    break
                elif errorType == 1:
                    if tryCount != config["maxTryCount"]:
                        generalLogger.warning(errorMessage)
                        await asyncio.sleep(2)
                    else:
                        generalLogger.warning(
                            "MaxTryCount has been reached, ignoring this weibo message.")
                else:
                    tryCount -= 1
    return sendFlag


async def __sendWechatMessage(token, postDict, tokenInvalidSaved):
    getUrl = urljoin(qywxApiUrl, "message", "send?access_token={}")
    tryCount = 0
    sendFlag = False
    async with aiohttp.ClientSession() as session:
        while tryCount < config["maxTryCount"]:
            errorType = 0
            try:
                response = await session.post(getUrl.format(token), json=postDict)
            except Exception as e:
                errorMessage = "Network connection error, will retry in two seconds, here is the error message:\n{}".format(
                    e)
                errorType = 1
            else:
                responseDict = await response.json()
                if responseDict["errcode"] == 0:
                    generalLogger.info("This wechat message has been sent!")
                    sendFlag = True
                elif responseDict["errcode"] == -1:
                    errorMessage = "Wechat api system busy, will retry in two seconds."
                    errorType = 1
                elif responseDict["errcode"] == 40014:
                    retry = False
                    async with __getWechatTokenLock:
                        if getValue("wechatTokenAvailable"):
                            if token == getValue("wechatToken"):
                                await __getWechatToken()
                                retry = getValue("wechatTokenAvailable")
                            else:
                                retry = True
                    if retry:
                        generalLogger.info(
                            "Retry sending the message with the new token.")
                        token = getValue("wechatToken")
                        errorType = 2
                    elif tokenInvalidSaved:
                        __saveWechatMessage(postDict)
                    else:
                        generalLogger.info(
                            "WechatToken cannot be gotten and tokenInvalidSaved is false, so ignoring this wechat message.")
                else:
                    generalLogger.warning("An unresolved error occurred, here is the error code: {}".format(
                        responseDict["errcode"]))
            finally:
                tryCount += 1
                if errorType == 0:
                    break
                elif errorType == 1:
                    if tryCount != config["maxTryCount"]:
                        generalLogger.warning(errorMessage)
                        await asyncio.sleep(2)
                    else:
                        generalLogger.warning(
                            "MaxTryCount has been reached, ignoring this wechat message.")
                else:
                    tryCount -= 1
    return sendFlag


async def __getWeiboMessage(sendTimestamp):
    getUrl = urljoin(weiboApiUrl, "list?uid=5175429989&count=10&unfollowing=0")
    tryCount = 0
    responseMessages = []
    await asyncio.sleep(1.5)
    async with aiohttp.ClientSession() as session:
        while tryCount < config["maxTryCount"]:
            errorType = 0
            try:
                response = await session.get(getUrl, headers=weiboHeaders)
            except Exception as e:
                errorMessage = "Network connection error, will retry in one second, here is the error message:\n{}".format(
                    e)
                errorType = 1
            else:
                responseDict = await response.json()
                for message in responseDict["data"]["msgs"]:
                    receiveTimestamp = datetime.strptime(
                        message["created_at"], "%a %b %d %H:%M:%S %z %Y").timestamp()
                    if receiveTimestamp < sendTimestamp:
                        break
                    elif message["sender_id"] == 5175429989:
                        if message["media_type"] == 0:
                            responseMessages.append(message["text"])
                        else:
                            responseMessages.append("暂不支持显示非文本类消息哦~")
                if responseMessages:
                    generalLogger.info("Weibo message(s) gotten!")
                else:
                    errorMessage = "Weibo message(s) cannot be gotten temporarily, will retry in one second."
                    errorType = 1
            finally:
                tryCount += 1
                if errorType == 0:
                    break
                else:
                    if tryCount != config["maxTryCount"]:
                        generalLogger.warning(errorMessage)
                        await asyncio.sleep(1)
                    else:
                        generalLogger.info(
                            "MaxTryCount has been reached, weibo message(s) failed to get.")
                        responseMessages.append("获取消息失败，请稍后重试~")
    responseMessages.reverse()
    return responseMessages


async def __getWeiboToken():
    getUrl = urljoin(weiboApiUrl, "list?uid=5175429989&count=10&unfollowing=0")
    tryCount = 0
    async with aiohttp.ClientSession() as session:
        while tryCount < config["maxTryCount"]:
            errorType = 0
            try:
                response = await session.get(getUrl, headers=weiboHeaders)
            except Exception as e:
                errorMessage = "Network connection error, will retry in two seconds, here is the error message:\n{}".format(
                    e)
                errorType = 1
            else:
                generalLogger.info("WeiboToken gotten!")
                oldWeiboToken = getValue("weiboToken")
                newWeiboToken = response.cookies["XSRF-TOKEN"].value
                setValue("weiboToken", newWeiboToken)
                weiboHeaders["X-XSRF-TOKEN"] = newWeiboToken
                weiboHeaders["Cookie"] = weiboHeaders["Cookie"].replace(
                    "XSRF-TOKEN=" + oldWeiboToken, "XSRF-TOKEN=" + newWeiboToken)
            finally:
                tryCount += 1
                if errorType == 0:
                    break
                else:
                    if tryCount != config["maxTryCount"]:
                        generalLogger.warning(errorMessage)
                        await asyncio.sleep(2)
                    else:
                        generalLogger.info(
                            "MaxTryCount has been reached, weiboToken cannot be gotten.")


async def __getWechatToken():
    getUrlParameters = "gettoken?corpid={}&corpsecret={}".format(
        config["corpId"], config["secret"])
    getUrl = urljoin(qywxApiUrl, getUrlParameters)
    tryCount = 0
    async with aiohttp.ClientSession() as session:
        while tryCount < config["maxTryCount"]:
            errorType = 0
            try:
                response = await session.get(getUrl)
            except Exception as e:
                errorMessage = "Network connection error, will retry in two seconds, here is the error message:\n{}".format(
                    e)
                errorType = 1
            else:
                responseDict = await response.json()
                if responseDict["errcode"] == 0:
                    generalLogger.info("WechatToken gotten!")
                    setValue("wechatToken", responseDict["access_token"])
                    setValue("wechatTokenAvailable", True)
                    pendingWechatMessages = getValue("pendingWechatMessages")
                    if pendingWechatMessages:
                        asyncio.gather(
                            *[__sendWechatMessage(responseDict["access_token"], postDict, True) for postDict in pendingWechatMessages])
                        setValue("pendingWechatMessages", [])
                elif responseDict["errcode"] == -1:
                    errorMessage = "Wechat api system busy, will retry in two seconds."
                    errorType = 1
                else:
                    generalLogger.warning("An unresolved error occurred, here is the error code: {}".format(
                        responseDict["errcode"]))
                    setValue("wechatTokenAvailable", False)
            finally:
                tryCount += 1
                if errorType == 0:
                    break
                else:
                    setValue("wechatTokenAvailable", False)
                    if tryCount != config["maxTryCount"]:
                        generalLogger.warning(errorMessage)
                        await asyncio.sleep(2)
                    else:
                        generalLogger.warning(
                            "MaxTryCount has been reached, will retry getting wechatToken in five minutes.")
                        await taskScheduler.addJob("getToken", __getWechatToken, description="Try to get token", triggerName="date", runDate=(datetime.utcnow() + timedelta(minutes=5)))


__getWechatTokenLock = locks.Lock()
__chatLock = locks.Lock()
