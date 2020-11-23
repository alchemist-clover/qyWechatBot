import random
from datetime import timedelta, datetime
from abc import ABCMeta, abstractmethod
from math import ceil


from configs import config
from log import schedulerLogger


def createTrigger(triggerName, triggerArgs):
    if triggerName == "date":
        return dateTrigger(**triggerArgs)
    elif triggerName == "interval":
        return intervalTrigger(**triggerArgs)
    else:
        schedulerLogger.error(
            "Unsupported trigger type {}".format(triggerName))
        raise TypeError("Unsupported trigger type {}".format(triggerName))


def convertToUtctime(input, utc):
    if input is None:
        return
    elif isinstance(input, datetime):
        return datetime - timedelta(hours=utc)
    elif isinstance(input, str):
        return datetime.strptime(input, "%Y-%m-%d %H:%M:%S") - timedelta(hours=utc)
    else:
        schedulerLogger.error(
            "Unsupported type for {}".format(input.__class__.__name__))
        raise TypeError("Unsupported type for {}".format(
            input.__class__.__name__))


def utctimeToString(utctime):
    return (utctime + timedelta(hours=config["utc"])).strftime(config["logDateFormat"])


class baseTrigger(metaclass=ABCMeta):
    @abstractmethod
    def getNextFireTime(self, previousFireTime, now):
        pass

    def __applyJitter(self, nextFireTime, jitter, now):
        if nextFireTime is None or not jitter:
            return nextFireTime
        nextFireTimeWithJitter = nextFireTime + \
            timedelta(seconds=random.uniform(-jitter, jitter))
        if nextFireTimeWithJitter < now:
            return nextFireTime
        return nextFireTimeWithJitter


class dateTrigger(baseTrigger):
    def __init__(self, runDate=None, utc=config["utc"]):
        if runDate is not None:
            self.runDate = convertToUtctime(runDate, utc)
        else:
            self.runDate = datetime.utcnow()

    def __str__(self):
        return "date[{}]".format(utctimeToString(self.runDate))

    def __getstate__(self):
        return {"runDate": self.runDate}

    def __setstate__(self, state):
        self.runDate = state["runDate"]

    def getNextFireTime(self, previousFireTime, now):
        return self.runDate if previousFireTime is None else None


class intervalTrigger(baseTrigger):
    def __init__(self, weeks=0, days=0, hours=0, minutes=0, seconds=0, startDate=None, endDate=None, utc=config["utc"], jitter=None):
        self.interval = timedelta(
            weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
        if startDate is None:
            self.startDate = datetime.utcnow() + self.interval
        else:
            self.startDate = convertToUtctime(startDate, utc)
        self.endDate = convertToUtctime(endDate, utc)
        self.jitter = jitter

    def __str__(self):
        return "interval[{}]".format(self.interval)

    def __getstate__(self):
        return {
            "interval": self.interval,
            "startDate": self.startDate,
            "endDate": self.endDate,
            "jitter": self.jitter
        }

    def __setstate__(self, state):
        self.interval = state["interval"]
        self.startDate = state["startDate"]
        self.endDate = state["endDate"]
        self.jitter = state["jitter"]

    def getNextFireTime(self, previousFireTime, now):
        if previousFireTime is not None:
            nextFireTime = previousFireTime + self.interval
        elif now < self.startDate:
            nextFireTime = self.startDate
        else:
            timeDiff = now - self.startDate
            nextIntervalNum = ceil(timeDiff / self.interval)
            nextFireTime = self.startDate + self.interval * nextIntervalNum
        nextFireTime = self.__applyJitter(nextFireTime, self.jitter, now)
        if self.endDate is None or nextFireTime <= self.endDate:
            return nextFireTime
