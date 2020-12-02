import pickle
import os
import asyncio
import signal
from tornado.options import options
from tornado.ioloop import IOLoop


from configs import config, globalState, dataBaseDir, cacheFilePath, cacheTableStructure, setValue, starting, running, stopping
from log import loadLogConfig, generalLogger
from dataBase import syncDataBase
from scheduler import taskScheduler
from server import getConnectionCount, httpServer


def main():
    global state
    state = starting
    start()
    state = running
    ioLoop.start()


def start():
    options.parse_command_line()
    loadLogConfig()
    if not os.path.exists(dataBaseDir):
        os.mkdir(dataBaseDir)
    with syncDataBase(cacheFilePath) as dataBase:
        tableNames = dataBase.tableNames()
        if config["cacheTableName"] not in tableNames:
            dataBase.createTable(config["cacheTableName"], cacheTableStructure)
            for key in globalState:
                dataBase.insertRow(config["cacheTableName"], key, pickle.dumps(
                    globalState[key], pickle.HIGHEST_PROTOCOL))
            dataBase.insertRow(config["cacheTableName"], "pendingJobs",
                               pickle.dumps([], pickle.HIGHEST_PROTOCOL))
        else:
            cacheList = dataBase.queryTable("*", config["cacheTableName"])
            for key, value in cacheList:
                if key != "pendingJobs":
                    setValue(key, pickle.loads(value))
                else:
                    taskScheduler.addPendingJobs(pickle.loads(value))
    taskScheduler.start()
    httpServer.listen(config["botListenPort"])
    httpServer.start()
    generalLogger.info("wechatBot start successfully!")


async def close():
    httpServer.stop()
    while getConnectionCount() != 0:
        await asyncio.sleep(1)
    await httpServer.close_all_connections()
    await taskScheduler.shutdown()
    while len(asyncio.all_tasks()) != 1:
        await asyncio.sleep(1)
    with syncDataBase(cacheFilePath) as dataBase:
        for key in globalState:
            dataBase.updateCol(config["cacheTableName"], "where key = '{}'".format(
                key), value=pickle.dumps(globalState[key], pickle.HIGHEST_PROTOCOL))
        dataBase.updateCol(config["cacheTableName"], "where key = 'pendingJobs'", value=pickle.dumps(
            taskScheduler.getPendingJobs(), pickle.HIGHEST_PROTOCOL))
    ioLoop.stop()


def exitHandler(signum, frame):
    global state
    if state is None:
        raise SystemExit
    elif state == starting:
        print("请等待starting完成")
    elif state == stopping:
        print("程序正在退出，请耐心等待")
    else:
        print("程序准备退出")
        state = stopping
        ioLoop.add_callback_from_signal(close)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, exitHandler)
    state = None
    ioLoop = IOLoop.current()
    main()