import pickle
import aiosqlite
import asyncio
from tornado.ioloop import IOLoop
from tornado.locks import Lock
from datetime import datetime, timedelta
from collections import defaultdict
from functools import wraps


from configs import config, stateStopped, stateRunning, statePaused, jobStoreTableStructure, jobStoreRetryInterval, timeoutMax, tasksFilePath
from log import schedulerLogger
from dataBase import syncDataBase
from trigger import createTrigger


def restoreJob(jobStateBytes):
    jobState = pickle.loads(jobStateBytes)
    return job(jobState)


def setLock(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        async with self.lock:
            result = await func(self, *args, **kwargs)
        return result
    return wrapper


class job():
    def __init__(self, **kwargs):
        self.state = {}
        self.update(**kwargs)

    def __str__(self):
        if self.state["nextRunTime"] is not None:
            status = (self.state["nextRunTime"] + timedelta(
                hours=config["utc"])).strftime(config["logDateFormat"])
        else:
            status = "paused"
        status = "next run at: " + status
        return "{} (trigger: {}, {})".format(self.state["description"], self.state["trigger"], status)

    def __getstate__(self):
        return self.state

    def __setstate__(self, state):
        self.state = state

    def update(self, **kwargs):
        self.state.update(kwargs)

    def getRunTimes(self, now):
        runTimes = []
        nextRunTime = self.state["nextRunTime"]
        while nextRunTime is not None and nextRunTime <= now:
            runTimes.append(nextRunTime)
            nextRunTime = self.state["trigger"].getNextFireTime(
                nextRunTime, now)
        return runTimes


class jobStore():
    def __init__(self, dataBaseFilePath, tableName, pickleProtocol=pickle.HIGHEST_PROTOCOL):
        self.tableName = tableName
        self.__dataBaseFilePath = dataBaseFilePath
        self.__pickleProtocol = pickleProtocol

    def start(self):
        with syncDataBase(self.__dataBaseFilePath) as dataBase:
            tableNames = dataBase.tableNames()
            if self.tableName not in tableNames:
                dataBase.createTable(self.tableName, jobStoreTableStructure)

    def syncAddJob(self, job):
        with syncDataBase(self.__dataBaseFilePath) as dataBase:
            dataBase.insertRow(self.tableName, job.state["id"], job.state["nextRunTime"].timestamp(
            ) if job.state["nextRunTime"] is not None else None, pickle.dumps(job.state, self.__pickleProtocol))

    async def addJob(self, job):
        sql = "insert into {} values (?,?,?)".format(self.tableName)
        variables = (job.state["id"], job.state["nextRunTime"].timestamp(
        ) if job.state["nextRunTime"] is not None else None, pickle.dumps(job.state, self.__pickleProtocol))
        await self.__execute(sql, *variables)

    async def removeJob(self, jobId):
        sql = "delete from {} where id = '{}'".format(self.tableName, jobId)
        await self.__execute(sql)

    async def removeJobs(self):
        sql = "delete from {}".format(self.tableName)
        await self.__execute(sql)

    async def getJob(self, jobId):
        async with aiosqlite.connect(self.__dataBaseFilePath) as dataBase:
            async with dataBase.cursor() as cursor:
                sql = "select state from {} where id = '{}'".format(
                    self.tableName, jobId)
                await cursor.execute(sql)
                resultRow = await cursor.fetchone()
                if resultRow is not None:
                    job = restoreJob(resultRow[0])
                else:
                    job = None
        return job

    async def getJobs(self):
        return await self.__getJobs("order by case when nextRunTime is null then 1 else 0 end, nextRunTime asc")

    async def getDueJobs(self, now):
        return await self.__getJobs("where nextRunTime <= {} order by nextRunTime asc".format(now.timestamp()))

    async def getNextRunTime(self):
        async with aiosqlite.connect(self.__dataBaseFilePath) as dataBase:
            async with dataBase.cursor() as cursor:
                sql = "select nextRunTime from {} where nextRunTime is not null order by nextRunTime asc limit 1".format(
                    self.tableName)
                await cursor.execute(sql)
                resultRow = await cursor.fetchone()
                if resultRow is not None:
                    nextRunTime = datetime.fromtimestamp(resultRow[0])
                else:
                    nextRunTime = None
        return nextRunTime

    async def updateJob(self, job):
        sql = "update {} set nextRunTime=(?), state=(?) where id = '{}'".format(
            self.tableName, job.state["id"])
        variables = (job.state["nextRunTime"].timestamp() if job.state["nextRunTime"] is not None else None,
                     pickle.dumps(job.state, self.__pickleProtocol))
        await self.__execute(sql, *variables)

    async def __execute(self, sql, *variables):
        async with aiosqlite.connect(self.__dataBaseFilePath) as dataBase:
            async with dataBase.cursor() as cursor:
                await cursor.execute(sql, variables)
                await dataBase.commit()

    async def __getJobs(self, *conditions):
        jobs = []
        failedJobIds = set()
        async with aiosqlite.connect(self.__dataBaseFilePath) as dataBase:
            async with dataBase.cursor() as cursor:
                sql = "select id, state from {}".format(self.tableName)
                sql = " ".join((sql,) + conditions)
                await cursor.execute(sql)
                async for row in cursor:
                    try:
                        jobs.append(restoreJob(row[1]))
                    except Exception as e:
                        schedulerLogger.warning(
                            "Unable to restore job {} -- removing it. Here is the error message:\n{}".format(row[0], e))
                        failedJobIds.add((row[0],))
                if failedJobIds:
                    sql = "delete from {} where id = (?)".format(self.tableName)
                    await cursor.executemany(sql, failedJobIds)
                    await cursor.commit()
        return jobs


class tornadoScheduler():
    __ioLoop = IOLoop.current()

    def __init__(self, dataBaseFilePath, tableNames):
        self.state = stateStopped
        self.jobStoreNames = ["static", "custom", "temporary"]
        self.lock = Lock()
        self.__jobStores = {}
        self.__timeout = None
        self.__instances = defaultdict(lambda: 0)
        self.__pendingJobs = []
        for index, jobStoreName in enumerate(self.jobStoreNames):
            self.__jobStores[jobStoreName] = jobStore(
                dataBaseFilePath, tableNames[index])

    def start(self, paused=False):
        if self.state != stateStopped:
            schedulerLogger.error("Scheduler already running.")
            raise RuntimeError("Scheduler already running.")
        for jobStoreName in self.jobStoreNames:
            self.__jobStores[jobStoreName].start()
        for job, jobStoreName in self.__pendingJobs:
            self.__jobStores[jobStoreName].syncAddJob(job)
        del self.__pendingJobs[:]
        self.state = statePaused if paused else stateRunning
        schedulerLogger.info("Scheduler started.")
        if not paused:
            tornadoScheduler.__ioLoop.add_callback(self.__wakeup)

    def getPendingJobs(self):
        return self.__pendingJobs

    def addPendingJobs(self, pendingJobs):
        self.__pendingJobs.extend(pendingJobs)

    @setLock
    async def shutdown(self):
        if self.state == stateStopped:
            schedulerLogger.error("Scheduler not running.")
            raise RuntimeError("Scheduler not running.")
        if self.__timeout is not None:
            tornadoScheduler.__ioLoop.remove_timeout(self.__timeout)
            self.__timeout = None
        self.state = stateStopped
        schedulerLogger.info("Scheduler has been shutdown.")

    @setLock
    async def pause(self):
        if self.state == stateStopped:
            schedulerLogger.error("Scheduler not running.")
            raise RuntimeError("Scheduler not running.")
        elif self.state == stateRunning:
            if self.__timeout is not None:
                tornadoScheduler.__ioLoop.remove_timeout(self.__timeout)
                self.__timeout = None
            self.state = statePaused
            schedulerLogger.info("Paused scheduler job processing.")

    @setLock
    async def resume(self):
        if self.state == stateStopped:
            schedulerLogger.error("Scheduler not running.")
            raise RuntimeError("Scheduler not running.")
        elif self.state == statePaused:
            self.state = stateRunning
            tornadoScheduler.__ioLoop.add_callback(self.__wakeup)
            schedulerLogger.info("Resumed scheduler job processing.")

    @setLock
    async def addJob(self, jobId, func, args=None, kwargs=None, description="undefined", jobStoreName="temporary",
                     triggerName="date", misfireGraceTime=60, coalesce=True, maxInstances=1, nextRunTime="undefined", **triggerArgs):
        trigger = createTrigger(triggerName, triggerArgs)
        jobKwargs = {
            "id": jobId,
            "func": func,
            "args": tuple(args) if args is not None else(),
            "kwargs": kwargs if kwargs is not None else{},
            "description": description,
            "trigger": trigger,
            "misfireGraceTime": misfireGraceTime,
            "coalesce": coalesce,
            "maxInstances": maxInstances,
            "nextRunTime": nextRunTime if nextRunTime != "undefined" else trigger.getNextFireTime(None, datetime.utcnow())
        }
        job = job(**jobKwargs)
        if self.state == stateStopped:
            self.__pendingJobs.append((job, jobStoreName))
            schedulerLogger.info(
                "Adding job tentatively -- it will be properly scheduled when the scheduler starts.")
        else:
            await self.__jobStores[jobStoreName].addJob(job)
            schedulerLogger.info("Added job '{}' to table '{}'.".format(
                job, self.__jobStores[jobStoreName].tableName))
            if self.state == stateRunning:
                tornadoScheduler.__ioLoop.add_callback(self.__wakeup)

    @setLock
    async def removeJob(self, jobId, jobStoreName="temporary"):
        await self.__removeJob()

    @setLock
    async def removeJobs(self, jobStoreName=None):
        if self.state == stateStopped:
            if jobStoreName is not None:
                self.__pendingJobs = [
                    pending for pending in self.__pendingJobs if pending[1] != jobStoreName]
            else:
                self.__pendingJobs = []
        else:
            for job_store_name in self.jobStoreNames:
                if jobStoreName is None or job_store_name == jobStoreName:
                    await self.__jobStores[job_store_name].removeJobs()

    @setLock
    async def getJob(self, jobId, jobStoreName="temporary"):
        return await self.__getJob(jobId, jobStoreName)

    @setLock
    async def getJobs(self, jobStoreName=None):
        jobs = []
        if self.state == stateStopped:
            for job, job_store_name in self.__pendingJobs:
                if jobStoreName is None or job_store_name == jobStoreName:
                    jobs.append(job)
        else:
            for job_store_name in self.jobStoreNames:
                if jobStoreName is None or job_store_name == jobStoreName:
                    jobs.extend(await self.__jobStores[job_store_name].getJobs())
        return jobs

    @setLock
    async def updateJob(self, jobId, jobStoreName="temporary", **changes):
        await self.__modifyJob(jobId, jobStoreName, **changes)

    @setLock
    async def pauseJob(self, jobId, jobStoreName="temporary"):
        await self.__modifyJob(jobId, jobStoreName, nextRunTime=None)

    @setLock
    async def resumeJob(self, jobId, jobStoreName="temporary"):
        job = await self.__getJob(jobId, jobStoreName)
        nextRunTime = job.state["trigger"].getNextFireTime(
            None, datetime.utcnow())
        if nextRunTime is not None:
            await self.__modifyJob(jobId, jobStoreName, nextRunTime=nextRunTime)
        else:
            await self.__removeJob(jobId, jobStoreName)

    @setLock
    async def rescheduleJob(self, jobId, jobStoreName="temporary", triggerName="date", **triggerArgs):
        trigger = createTrigger(triggerName, triggerArgs)
        nextRunTime = trigger.getNextFireTime(None, datetime.utcnow())
        await self.__modifyJob(jobId, jobStoreName, trigger=trigger, nextRunTime=nextRunTime)

    def submitJob(self, job, runTimes):
        if self.__instances[job.state["id"]] >= job.state["maxInstances"]:
            schedulerLogger.warning("Execution of job '{}' skipped: maximum number of running instances reached ({})".format(
                job, job.state["maxInstances"]))
        else:
            def callback(future):
                self.__instances[job.state["id"]] -= 1
                if self.__instances[job.state["id"]] == 0:
                    del self.__instances[job.state["id"]]
            graceTime = timedelta(seconds=job.state["misfireGraceTime"])
            tasks = []
            for runTime in runTimes:
                difference = datetime.utcnow() - runTime
                if difference > graceTime:
                    schedulerLogger.warning(
                        "Run time of job '{}' was missed by '{}'".format(job, difference))
                    continue
                tasks.append(job.state["func"](
                    *job.state["args"], **job.state["kwargs"]))
            if tasks:
                tasks = asyncio.gather(*tasks)
                tasks.add_done_callback(callback)
                self.__instances[job.state["id"]] += 1
                schedulerLogger.info(
                    "Submit job '{}' successfully.".format(job))

    async def __removeJob(self, jobId, jobStoreName="temporary"):
        if self.state == stateStopped:
            for index, (job, job_store_name) in enumerate(self.__pendingJobs):
                if job.state["id"] == jobId and job_store_name == jobStoreName:
                    del self.__pendingJobs[index]
                    break
        else:
            await self.__jobStores[jobStoreName].removeJob(jobId)
        schedulerLogger.info("Removed job {}".format(jobId))

    async def __getJob(self, jobId, jobStoreName="temporary"):
        if self.state == stateStopped:
            for job, job_store_name in self.__pendingJobs:
                if job.state["id"] == jobId and job_store_name == jobStoreName:
                    return job
        else:
            return await self.__jobStores[jobStoreName].getJob(jobId)
        return None

    async def __modifyJob(self, jobId, jobStoreName="temporary", **changes):
        job = await self.__getJob(jobId, jobStoreName)
        job.update(**changes)
        if self.state != stateStopped:
            await self.__jobStores[jobStoreName].updateJob(job)
        if self.state == stateRunning:
            tornadoScheduler.__ioLoop.add_callback(self.__wakeup)

    @setLock
    async def __wakeup(self):
        if self.state != stateRunning:
            schedulerLogger.info(
                "Scheduler is not running -- not processing jobs.")
            return
        if self.__timeout is not None:
            tornadoScheduler.__ioLoop.remove_timeout(self.__timeout)
            self.__timeout = None
        nextWakeupTime = await self.__processJobs()
        if nextWakeupTime is not None:
            self.__timeout = tornadoScheduler.__ioLoop.add_timeout(
                nextWakeupTime - datetime.utcnow(), self.__wakeup)

    async def __processJobs(self):
        schedulerLogger.debug("Looking for jobs to run.")
        nextWakeupTime = None
        now = datetime.utcnow()
        for jobStoreName in self.jobStoreNames:
            try:
                dueJobs = await self.__jobStores[jobStoreName].getDueJobs(now)
            except Exception as e:
                schedulerLogger.warning("Error getting due jobs from table {}, here is the error message:\n{}".format(
                    self.__jobStores[jobStoreName].tableName, e))
                retryWakeupTime = now + \
                    timedelta(seconds=jobStoreRetryInterval)
                if nextWakeupTime is None or retryWakeupTime < nextWakeupTime:
                    nextWakeupTime = retryWakeupTime
                continue
            for job in dueJobs:
                runTimes = job.getRunTimes()
                runTimes = runTimes[-1:] if job.state["coalesce"] else runTimes
                self.submitJob(job, runTimes)
                jobNextRunTime = job.state["trigger"].getNextFireTime(
                    runTimes[-1], now)
                if jobNextRunTime is not None:
                    job.state["nextRunTime"] = jobNextRunTime
                    await self.__jobStores[jobStoreName].updateJob(job)
                else:
                    await self.__jobStores[jobStoreName].removeJob(job.state["id"])
            jobStoreNextRunTime = await self.__jobStores[jobStoreName].getNextRunTime()
            if jobStoreNextRunTime is not None and (nextWakeupTime is None or jobStoreNextRunTime < nextWakeupTime):
                nextWakeupTime = jobStoreNextRunTime
        if nextWakeupTime is not None:
            nextWakeupTime = min(nextWakeupTime, now +
                                 timedelta(seconds=timeoutMax))
            schedulerLogger.debug("Next wakeup is due at {}.".format(
                (nextWakeupTime+timedelta(hours=config["utc"])).strftime(config["logDateFormat"])))
        else:
            schedulerLogger.debug("No jobs, waiting until a job is added.")
        return nextWakeupTime


taskScheduler = tornadoScheduler(tasksFilePath, config["jobTableNames"])
