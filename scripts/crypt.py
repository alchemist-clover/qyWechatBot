import base64
from hashlib import sha1
from struct import pack, unpack
from socket import htonl, ntohl
from Crypto.Random import get_random_bytes
from Crypto.Cipher import AES
from datetime import datetime, timedelta
from xml.etree.cElementTree import fromstring


from configs import config, passiveResponsePacket, blockSize
from log import generalLogger


def verifyUrl(msgSignature, timestamp, nonce, echoString):
    if not all([msgSignature, timestamp, nonce, echoString]):
        return
    signature = __getSignature(
        config["cryptToken"], timestamp, nonce, echoString)
    if signature is None or signature != msgSignature:
        return
    return __decrypt(echoString, config["corpId"])


def encryptMsg(replyMsg, nonce, timestamp=None):
    encrypt = __encrypt(replyMsg, config["corpId"])
    if encrypt is None:
        return
    encrypt = encrypt.decode("utf-8")
    if timestamp is None:
        timestamp = str(
            int((datetime.utcnow() + timedelta(hours=8)).timestamp()))
    signature = __getSignature(config["cryptToken"], timestamp, nonce, encrypt)
    if signature is None:
        return
    return passiveResponsePacket.format(encrypt, signature, timestamp, nonce)


def decryptMsg(postData, msgSignature, timestamp, nonce):
    if not all([postData, msgSignature, timestamp, nonce]):
        return
    encrypt = __extract(postData)
    if encrypt is None:
        return
    signature = __getSignature(config["cryptToken"], timestamp, nonce, encrypt)
    if signature is None or signature != msgSignature:
        return
    return __decrypt(encrypt, config["corpId"])


def __getSignature(token, timestamp, nonce, msgEncrypt):
    stringList = [token, timestamp, nonce, msgEncrypt]
    stringList.sort()
    try:
        sha = sha1("".join(stringList).encode("utf-8"))
        return sha.hexdigest()
    except Exception as e:
        generalLogger.warning(
            "Get signature error, here is the error message:\n{}".format(e))


def __extract(xmlText):
    try:
        xmlTree = fromstring(xmlText)
        encrypt = xmlTree.find("Encrypt")
        return encrypt.text
    except Exception as e:
        generalLogger.warning(
            "Parse xml error, here is the error message:\n{}".format(e))


def __encrypt(msg, receiveId):
    msg = msg.encode("utf-8")
    msg = get_random_bytes(16) + pack("I", htonl(len(msg))
                                      ) + msg + receiveId.encode("utf-8")
    amountToPad = blockSize - (len(msg) % blockSize)
    pad = chr(amountToPad)
    msg = msg + (pad * amountToPad).encode("utf-8")
    try:
        msgEncrypt = cryptor.encrypt(msg)
        return base64.b64encode(msgEncrypt)
    except Exception as e:
        generalLogger.warning(
            "Encryption error, here is the error message:\n{}".format(e))


def __decrypt(msgEncrypt, receiveId):
    try:
        msgEncrypt = cryptor.decrypt(base64.b64decode(msgEncrypt))
    except Exception as e:
        generalLogger.warning(
            "Decryption error, here is the error message:\n{}".format(e))
        return
    try:
        pad = msgEncrypt[-1]
        content = msgEncrypt[16: - pad]
        msgLength = ntohl(unpack("I", content[:4])[0])
        msg = content[4:msgLength + 4]
        fromReceiveId = content[msgLength + 4:]
    except Exception as e:
        generalLogger.warning(
            "Illegal input, here is the error message:\n{}".format(e))
        return
    if fromReceiveId.decode("utf-8") != receiveId:
        generalLogger.warning("ReceiveId error.")
        return
    return msg


AESKey = base64.b64decode(config["cryptKey"] + "=")
cryptor = AES.new(AESKey, AES.MODE_CBC, AESKey[:16])
