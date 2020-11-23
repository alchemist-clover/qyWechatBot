import os
import logging
import logging.config
from tornado.log import access_log, app_log, gen_log
from aiosqlite.core import LOG


from configs import config, logConfigDict, logFilesDir


def loadLogConfig():
    if config["logEnable"]:
        nameArray = ["access", "application", "general", "sqlite", "scheduler"]
        loggerNameArray = ["tornado.access", "tornado.application",
                           "tornado.general", "aiosqlite", "scheduler"]
        if not os.path.exists(logFilesDir):
            os.mkdir(logFilesDir)
        for name in nameArray:
            logFilePath = os.path.join(logFilesDir, name + "Log")
            if not os.path.exists(logFilePath):
                os.mkdir(logFilePath)
        logLevel = config["logLevel"].upper()
        if config["logFormat"] is not None:
            logConfigDict["formatters"]["customFormatter"]["fmt"] = config["logFormat"]
        if config["logDateFormat"] is not None:
            logConfigDict["formatters"]["customFormatter"]["datefmt"] = config["logDateFormat"]
        for name in nameArray:
            handlerDict = logConfigDict["handlers"][name + "Handler"]
            handlerDict["level"] = logLevel
            handlerDict["filename"] = os.path.join(
                logFilesDir, name + "Log", name + ".log")
            handlerDict["backupCount"] = config["logBackupCount"]
        if config["logRotateMode"] == "time":
            for name in nameArray:
                handlerDict = logConfigDict["handlers"][name + "Handler"]
                handlerDict["class"] = "logging.handlers.TimedRotatingFileHandler"
                handlerDict["when"] = config["logWhen"]
                handlerDict["utc"] = config["logUtc"]
                handlerDict["interval"] = config["logInterval"]
        if config["logRotateMode"] == "size":
            for name in nameArray:
                handlerDict = logConfigDict["handlers"][name + "Handler"]
                handlerDict["class"] = "logging.handlers.RotatingFileHandler"
                handlerDict["maxBytes"] = config["logMaxBytes"]
        for loggerName in loggerNameArray:
            loggerDict = logConfigDict["loggers"][loggerName]
            loggerDict["level"] = logLevel
        try:
            logging.config.dictConfig(logConfigDict)
        except Exception as e:
            print(
                "Log configuration loading error, please confirm the configurations are correct.")
            print("Here is the error message:")
            print(e)
            raise SystemExit
    else:
        loggerArray = [access_log, app_log, gen_log, LOG, schedulerLogger]
        for logger in loggerArray:
            logger.disabled = True


sqliteLogger = LOG
generalLogger = gen_log
schedulerLogger = logging.getLogger("scheduler")
