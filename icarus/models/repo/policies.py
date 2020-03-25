from __future__ import division

import time
from collections import deque, defaultdict
import random
import abc
import copy

import numpy as np

from icarus.util import inheritdoc, apportionment
from icarus.registry import register_repo_policy

__all__ = [
        'RepoStorage',
           ]


@register_repo_policy('REPO_STORAGE')
class RepoStorage(object):
    def __init__(self, node, contents, storageSize, compressionRate):
        """Constructor

        TODO: Need to revise this, to include AT LEAST ONE CACHE POLICY, AS WELL!
            OR IMPLEMENT THIS AS ANOTHER< REPO_POLICY INSTEAD!

        Parameters
        ----------
        maxlen : int
            The maximum number of items the cache can store
        """
        self.host = node
        # self.messages = new
        # Collection
        self.staticMessages = list
        self.processMessages = list
        self.processedMessages = list
        self.storedMessages = list
        self.storageSize = storageSize
        self.processSize = 0
        self.staticSize = 0
        self.processedSize = 0
        self.mFresh = 0
        self.mStale = 0
        self.mOvertime = 0
        self.mSatisfied = 0
        self.mUnSatisfied = 0
        self.mUnProcessed = 0
        self.mStorTimeNo = 0
        self.mStorTimeAvg = 0
        self.mStorTimeMax = 0
        self.mStorTime = 0
        self.nrofDeletedMessages = 0
        self.deletedMessagesSize = 0
        self.totalReceivedMessages = 0
        self.totalReceivedMessagesSize = 0
        self.depletedProcMessages = 0
        self.oldDepletedProcMessagesSize = 0
        self.depletedProcMessagesSize = 0
        self.depletedCloudProcMessages = 0
        self.oldDepletedCloudProcMessagesSize = 0
        self.depletedCloudProcMessagesSize = 0
        self.depletedUnProcMessages = 0
        self.depletedUnProcMessagesSize = 0
        self.depletedPUnProcMessages = 0
        self.depletedPUnProcMessagesSize = 0
        self.oldDepletedUnProcMessagesSize = 0
        self.depletedStaticMessages = 0
        self.depletedStaticMessagesSize = 0
        self.oldDepletedStaticMessagesSize = 0
        self.depletedCloudStaticMessages = 0
        self.oldDepletedCloudStaticMessagesSize = 0
        self.depletedCloudStaticMessagesSize = 0
        self.cachedMessages = 0
        if (self.getHost.hasProcessingCapability):
            # self.processSize = processSize
            self.compressionRate = compressionRate
            processedRatio = self.compressionRate * 2
            self.processedSize = self.storageSize / processedRatio
        if contents is not None:
            for content in contents:
                self.addToStoredMessages(content)

    def getTotalStorageSpace(self):
        return self.storageSize

    def getTotalProcessedSpace(self):
        return self.processedSize

    def getStoredMessagesCollection(self):
        self.messages = self.staticMessages
        self.messages.addAll(self.processMessages)
        return self.messages

    def getStoredMessages(self):
        self.storedMessages = self.staticMessages
        self.storedMessages.addAll(self.processMessages)
        return self.storedMessages

    def getProcessedMessages(self):
        return self.processedMessages

    def getProcessMessages(self):
        return self.processMessages

    def getStaticMessages(self):
        return self.staticMessages

    def addToStoredMessages(self, sm):
        """
           TODO: Check indentation herenot (in original, java implementation)
            Also, check the "selfs" in the parantheses. Those should mostly
            be the associated objects for which the functions are called.
            Does the cache have to have a host, or is IT the host? Should
            the simulator reference a host, for routing, or the cache itself?
        """

        if (sm is not None):
            if sm["service_type"].equalsIgnoreCase("nonproc"):
                if (self.host.hasStorageCapability()):
                    self.staticMessages.add(sm)
                    self.staticSize += sm.getSize()

            elif sm["service_type"].equalsIgnoreCase("proc"):
                if (self.getHost().hasProcessingCapability()):
                    self.processMessages.add(sm)
                    self.processSize += sm.getSize()

            elif sm["service_type"].equalsIgnoreCase("processed"):
                self.processedMessages.add(sm)

            elif (sm["service_type"]).equalsIgnoreCase("unprocessed"):
                if (self.host.hasStorageCapability()):
                    self.staticMessages.add(sm)
                    self.staticSize += sm.getSize()

                else:
                    self.addToDeplStaticMessages(sm)
            self.totalReceivedMessages += 1
            self.totalReceivedMessagesSize += sm.getSize()
        # add space used in the storage space * /
        # System.out.println("There is " + self.getStaticMessagesSize() + " storage used");

        if (self.staticSize + self.processSize) >= self.storageSize:
            for app in self.getHost().getRouter(self.getHost).getApplications("ProcApplication"):
                self.procApp = app
            # System.out.println("App ID is: " + self.procApp.getAppID());

            self.procApp.updateDeplBW(self.getHost())
            self.procApp.deplStorage(self.getHost())

    def addToDeplStaticMessages(self, sm):
        if sm is not None:
            self.depletedStaticMessages += 1
            self.depletedStaticMessagesSize += sm.getSize()
        if sm.getProperty("overtime"):
            self.mOvertime += 1
        if sm["service_type"] == "unprocessed":
            self.mUnProcessed += 1
            self.depletedUnProcMessages += 1
            self.depletedUnProcMessagesSize += sm.getSize()
        if sm.getProperty("satisfied"):
            self.mSatisfied += 1
        else:
            self.mUnSatisfied += 1
        if sm.getProperty("storTime") is not None:
            self.mStorTimeNo += 1
            self.mStorTime += sm.getProperty("storTime")
        if self.mStorTimeMax < sm.getProperty("storTime"):
            self.mStorTimeMax = sm.getProperty("storTime")

        else:
            curTime = time.time()
            sm.addProperty("storTime", curTime - sm.getReceiveTime(sm))
            self.mStorTimeNo += 1
            self.mStorTime += sm.getProperty("storTime")
        if self.mStorTimeMax < sm.getProperty("storTime"):
            self.mStorTimeMax = sm.getProperty("storTime")

    def addToDeplProcMessages(self, sm):
        if (sm is not None):
            self.depletedProcMessages += 1
            self.depletedProcMessagesSize += sm.getSize()
            if (sm.getProperty("overtime")):
                self.mOvertime += 1
                self.mUnSatisfied += 1
            if (sm["service_type"] == "unprocessed"):
                self.mUnProcessed += 1
                self.depletedUnProcMessages += 1
                self.depletedUnProcMessagesSize += sm.getSize()

                if (sm.getProperty("storTime") is not None):
                    self.mStorTimeNo += 1
                    self.mStorTime += sm.getProperty("storTime")
                if (self.mStorTimeMax < sm.getProperty("storTime")):
                    self.mStorTimeMax = sm.getProperty("storTime")

            else:
                curTime = time.time()
                sm.addProperty("storTime", curTime - sm.getReceiveTime(sm))
                self.mStorTimeNo += 1
                self.mStorTime += sm.getProperty("storTime")
                if (self.mStorTimeMax < sm.getProperty("storTime")):
                    self.mStorTimeMax = sm.getProperty("storTime")

    def addToCloudDeplStaticMessages(self, sm):
        if (sm is not None):
            self.depletedCloudStaticMessages += 1
            self.depletedCloudStaticMessagesSize += sm.getSize()
            if (sm.getProperty("overtime")):
                self.mOvertime += 1
            if (sm["service_type"] == "unprocessed"):
                self.mUnProcessed += 1
                self.depletedUnProcMessages += 1
                self.depletedUnProcMessagesSize += sm.getSize()

            if (sm.getProperty("satisfied")):
                self.mSatisfied += 1
            else:
                self.mUnSatisfied += 1
            if (sm.getProperty("storTime") is not None):
                self.mStorTimeNo += 1
                self.mStorTime += sm.getProperty("storTime")
            if (self.mStorTimeMax < sm.getProperty("storTime")):
                self.mStorTimeMax = sm.getProperty("storTime")

            else:
                curTime = time.time()
                sm.addProperty("storTime", curTime - sm.getReceiveTime(sm))
                self.mStorTimeNo += 1
                self.mStorTime += sm.getProperty("storTime")
            if (self.mStorTimeMax < sm.getProperty("storTime")):
                self.mStorTimeMax = sm.getProperty("storTime")

    def addToDeplUnProcMessages(self, sm):
        sm.updateProperty("type", "unprocessed")
        self.host.getStorageSystem().addToStoredMessages(sm)

    def addToDepletedUnProcMessages(self, sm):
        if (sm is not None):
            if (sm["service_type"] == "unprocessed"):
                self.depletedUnProcMessages += 1
            self.depletedUnProcMessagesSize += sm.getSize()
            self.mUnProcessed += 1

    def getStaticMessage(self, MessageId):
        staticMessage = None
        for temp in self.staticMessages:
            if (temp['content'] == MessageId):
                i = self.staticMessages.indexOf(temp)
                staticMessage = self.staticMessages.get(i)
        return staticMessage

    def getProcessedMessage(self, MessageId):
        processedMessage = None
        for temp in self.processedMessages:
            if (temp['content'] == MessageId):
                i = self.processedMessages.indexOf(temp)
                processedMessage = self.processedMessages.get(i)
        return processedMessage

    def getProcessMessage(self, MessageId):
        processMessage = None
        for temp in self.processMessages:
            if (temp['content'] == MessageId):
                i = self.processMessages.indexOf(temp)
                processMessage = self.processMessages.get(i)
        return processMessage

    def getStorTimeNo(self):
        return self.mStorTimeNo

    def getStorTimeAvg(self):
        self.mStorTimeAvg = self.mStorTime / self.mStorTimeNo
        return self.mStorTimeAvg

    def getStorTimeMax(self):
        return self.mStorTimeMax

    def getNrofMessages(self):
        return self.staticMessages.size()

    def getNrofProcessMessages(self):
        return self.processMessages.size()

    def getNrofProcessedMessages(self):
        return self.processedMessages.size()

    def getStaticMessagesSize(self):
        return self.staticSize

    def getStaleStaticMessagesSize(self):
        curTime = time.time()
        size = 0
        for m in self.staticMessages:
            if m.getProperty("shelfLife") is not None and (m.getProperty("shelfLife")) <= curTime - m.getReceiveTime(m):
                size += m.getSize()
                return size

    def getProcMessagesSize(self):
        return self.processSize

    def getProcessedMessagesSize(self):
        processedUsed = 0
        for msg in self.processedMessages:
            processedUsed += msg.getSize()
            return processedUsed

    def getFullCachedMessagesNo(self):
        """
            Need to add the feedback "functions" and interfacing
            TODO: Add the outward feedback message generation definitions,
                like the gets below, under them.
        """
        proc = self.cachedMessages
        self.cachedMessages = 0
        return proc

    """
    *Returns the host this repo storage system is in
    * @ return The host object
    """

    def getHost(self):
        return self.host

    def hasMessage(self, MessageId, labels): # TODO: Implement labels lookup!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        answer = None
        for j in range(0, self.processedMessages.size()):
            if (self.processedMessages.get(j)['content'] == MessageId):
                answer = self.processedMessages.get(j)
        for i in range(0, self.staticMessages.size()):
            if (self.staticMessages.get(i)['content'] == MessageId):
                answer = self.staticMessages.get(i)
        for i in range(0, self.processMessages.size()):
            if (self.processMessages.get(i)['content'] == MessageId):
                answer = self.processMessages.get(i)
        return answer

    def deleteStaticMessage(self, MessageId):
        for i in range(0, self.staticMessages.size):
            if (self.staticMessages.get(i).getId == MessageId):
                self.staticSize -= self.staticMessages.get(i).getSize
                self.staticMessages.remove(i)
                return True
        return False

    """
    *Method
    for deleting specific message to be processed
    * @ param MessageId ID of message to be deleted
    * @
    return successful
    deletion
    status
    """

    def deleteProcMessage(self, MessageId):
        for i in (0, self.processMessages.size):
            if (self.processMessages.get(i)['content'] == MessageId):
                self.processSize -= self.processMessages.get(i).getSize()
                self.processMessages.remove(i)
                return True

        return False

    """
    *Method
    for deleting specific message from storage
    * @ param MessageId ID of message to be deleted
    * @
    return successful
    deletion
    status
    """

    def deleteMessage(self, MessageId):
        m = self.hasMessage(MessageId)
        if m is not None:
            if m["service_type"].equalsIgnoreCase("proc") and self.deleteProcMessage(MessageId):
                return True

            elif m["service_type"].equalsIgnoreCase("nonproc") and self.deleteStaticMessage(MessageId):
                return True

            elif m["service_type"].equalsIgnoreCase("unprocessed") and self.deleteStaticMessage(MessageId):
                return True

            if not self.getHost().hasStorageCapability(self.getHost()):
                self.nrofDeletedMessages += 1
                self.deletedMessagesSize += m.getSize()

        return False

    """
    *Method
    for deleting specific processed message
    * @ param MessageId ID of message to be deleted
    * @
    return successful
    deletion
    status
    """

    def deleteProcessedMessage(self, MessageId, report):
        """
        *To
        be
        used in event,
        for deleting processed messages
        *after
        "sending" / depleting
        them.

        TODO: Check the ifs in original code - make code right. did not report every message.
            Reporting is not done right.
        """
        for i in range(0, self.processedMessages.size()):
            if (self.processedMessages.get(i)['content'] == MessageId):
                self.depletedCloudProcMessages += 1
                self.depletedCloudProcMessagesSize += self.processedMessages.get(i).getSize()

                if (self.processedMessages.get(i).getProperty("overtime") is not None):
                    if (self.processedMessages.get(i).getProperty("overtime")):
                        self.mOvertime += 1

                if (self.processedMessages.get(i).getProperty("satisfied") is not None):
                    if (self.processedMessages.get(i).getProperty("satisfied")):
                        self.mSatisfied += 1
                else:
                    self.mUnSatisfied += 1

                if (self.processedMessages.get(i).getProperty("Fresh") is not None):
                    if (self.processedMessages.get(i).getProperty("Fresh")):
                        self.mFresh += 1
                    elif (not self.processedMessages.get(i).getProperty("Fresh")):
                        self.mStale += 1

                self.processedMessages.remove(i)
                return True
        return False

    def getNrofDeletedMessages(self):
        return self.nrofDeletedMessages

    def getSizeofDeletedMessages(self):
        return self.deletedMessagesSize

    def getNrofDepletedCloudProcMessages(self):
        return self.depletedCloudProcMessages

    def getNrofFreshMessages(self):
        return self.mFresh

    def getNrofStaleMessages(self):
        return self.mStale

    def getNrofSatisfiedMessages(self):
        return self.mSatisfied

    def getNrofUnSatisfiedMessages(self):
        return self.mUnSatisfied

    def getNrofOvertimeMessages(self):
        return self.mOvertime

    def getNrofUnProcessedMessages(self):
        return self.mUnProcessed

    def getNrofDepletedUnProcMessages(self):
        return self.depletedUnProcMessages

    def getNrofDepletedPUnProcMessages(self):
        return self.depletedPUnProcMessages

    def getNrofDepletedStaticMessages(self):
        return self.depletedStaticMessages

    def getNrofDepletedCloudStaticMessages(self):
        return self.depletedCloudStaticMessages

    def getOverallMeanIncomingMesssageNo(self):
        return (self.totalReceivedMessages / time.time())

    def getOverallMeanIncomingSpeed(self):
        return (self.totalReceivedMessagesSize / time.time())

    """
    *Method
    that
    returns
    depletion
    BW
    used
    for processed messages.
    * @ param reporting Whether the function is used for reporting,
    * as a final method of the update, or for checking BW usage.
    * @
    return the
    processed
    depletion
    BW
    used
    upstream
    """

    def getDepletedCloudProcMessagesBW(self, reporting):

        procBW = self.depletedCloudProcMessagesSize - self.oldDepletedCloudProcMessagesSize
        if (reporting):
            self.oldDepletedCloudProcMessagesSize = self.depletedCloudProcMessagesSize

        return (procBW)

    """
    *Method
    that
    returns
    depletion
    BW
    used in off - loading
    static
    messages
    to
    the
    cloud.
    * @ param
    reporting
    Whether
    the
    function is used
    for reporting,
    * as a final method of the update, or for checking BW usage.
    * @
    return the
    static
    depletion
    BW
    used
    upstream
    """

    def getDepletedUnProcMessagesBW(self, reporting):

        procBW = self.depletedUnProcMessagesSize - self.oldDepletedUnProcMessagesSize
        if (reporting):
            self.oldDepletedUnProcMessagesSize = self.depletedUnProcMessagesSize
        return (procBW)

    """
    *Method
    that
    returns
    depletion
    BW
    used in off - loading
    unprocessed
    messages
    to
    the
    cloud.
    * @ param
    reporting
    Whether
    the
    function is used
    for reporting,
    * as a final method of the update, or for checking BW usage.
    * @
    return the
    unprocessed
    depletion
    BW
    used
    upstream
    """

    def getDepletedPUnProcMessagesBW(self, reporting):

        procBW = self.depletedPUnProcMessagesSize - self.oldDepletedPUnProcMessagesSize
        if (reporting):
            self.oldDepletedPUnProcMessagesSize = self.depletedPUnProcMessagesSize

        return (procBW)

    """
    *Method
    that
    returns
    depletion
    BW
    used
    for non - processing messages.
    * @ param reporting Whether the function is used for reporting,
    * as a final method of the update, or for checking BW usage.
    * @
    return the
    non - processing
    depletion
    BW
    used
    upstream
    """

    def getDepletedCloudStaticMessagesBW(self, reporting):
        statBW = self.depletedCloudStaticMessagesSize - self.oldDepletedCloudStaticMessagesSize
        if (reporting):
            self.oldDepletedCloudStaticMessagesSize = self.depletedCloudStaticMessagesSize

        return (statBW)

    """
    *Method
    that
    returns
    depletion
    BW
    used
    for processed messages.
    * @ param reporting Whether the function is used for reporting,
    * as a final method of the update, or for checking BW usage.
    * @
    return the
    processed
    depletion
    BW
    used
    upstream
    """

    def getDepletedProcMessagesBW(self, reporting):
        procBW = self.depletedProcMessagesSize - self.oldDepletedProcMessagesSize
        if (reporting):
            self.oldDepletedProcMessagesSize = self.depletedProcMessagesSize

        return (procBW)

    """
    *Method
    that
    returns
    depletion
    BW
    used
    for non - processing messages.
    * @ param reporting Whether the function is used for reporting,
    * as a final method of the update, or for checking BW usage.
    * @
    return the
    non - processing
    depletion
    BW
    used
    upstream
    """

    def getDepletedStaticMessagesBW(self, reporting):
        statBW = self.depletedStaticMessagesSize - self.oldDepletedStaticMessagesSize
        if (reporting):
            self.oldDepletedStaticMessagesSize = self.depletedStaticMessagesSize
        return (statBW)

    def clearAllStaticMessages(self):
        self.staticMessages.clear()
        return True

    def getFreeStorageSpace(self):
        usedStorage = self.getStaticMessagesSize()
        # System.out.prln("There is " + usedStorage + " storage used in " + self.getHost())
        freeStorage = self.storageSize - usedStorage
        # System.out.prln("There is " + freeStorage + " free storage space in " + self.getHost())
        return freeStorage

    """

    isStorageFull(self):
    usedStorage = self.getStaticMessagesSize()
                        if (usedStorage >= self.storageSize - 100000000):
                            return True

                        else:
                        return False





                        isProcessingFull()
                        :

                        usedProc = self.getProcMessagesSize()
                        if (usedProc >= self.processSize - 2000000)
                        :
                            #
                        System.out.prln("There is enough storage space: " + freeStorage)
                        return True

                        else:
                        # System.out.prln("There is not enough storage space: " + freeStorage)
                        return False

    """

    def isProcessingEmpty(self):
        # try:
        # System.setOut(new PrStream(new FileOutputStream("log.txt")))
        # catch(Exception e):
        if (self.processSize <= 2000000):
            # System.out.prln("There is enough storage space: " + freeStorage)
            return True

        else:
            # System.out.prln("There is not enough storage space: " + freeStorage)
            return False

    def isProcessedFull(self):
        usedProcessed = self.getProcessedMessagesSize()
        # try:
        # System.setOut(new Stream(new FileOutputStream("log.txt")))
        # catch(Exception e):
        if (usedProcessed >= self.processedSize - 500000):
            # System.out.prln("There is enough storage space: " + freeStorage)
            return True

        else:
            # System.out.prln("There is not enough storage space: " + freeStorage)
            return False

    def isProcessedEmpty(self):
        usedProc = self.getProcessedMessagesSize()
        # try:
        # System.setOut(new PrStream(new FileOutputStream("log.txt")))
        # catch(Exception e):
        if (usedProc <= 2000000):
            # System.out.prln("There is enough storage space: " + freeStorage)
            return True

        else:
            # System.out.prln("There is not enough storage space: " + freeStorage)
            return False

    @property
    def getOldestProcessMessage(self):
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                oldest = m

            elif (oldest.getReceiveTime() > m.getReceiveTime(m)):
                oldest = m

        return oldest

    @property
    def getOldestValidProcessMessage(self):
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                if (m["service_type"] is not None and m.getProperty("Fresh") is None):
                    if (not (m["service_type"]).equalsIgnoreCase("unprocessed") and not (
                    m["service_type"]).equalsIgnoreCase("processed")):
                        oldest = m

                elif oldest.getReceiveTime() > m.getReceiveTime(m) and m["service_type"] is not None and m.getProperty(
                        "Fresh") is None:
                    if not (m["service_type"]).equalsIgnoreCase("unprocessed") and not (
                    m["service_type"]).equalsIgnoreCase("processed"):
                        oldest = m

        return oldest

    @property
    def getOldestInvalidProcessMessage(self):
        curTime = time.time()
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                if (m["service_type"] is not None and m.getProperty("shelfLife") is not None):
                    if ((m["service_type"]).equalsIgnoreCase("proc") and (
                    m.getProperty("shelfLife")) <= curTime - m.getReceiveTime(m)):
                        oldest = m

                elif (oldest.getReceiveTime() > m.getReceiveTime(m) and m["service_type"] is not None and m.getProperty(
                        "shelfLife") is not None):
                    if ((m["service_type"]).equalsIgnoreCase("proc") and (
                    m.getProperty("shelfLife")) <= curTime - m.getReceiveTime(m)):
                        oldest = m

        return oldest

    @property
    def getOldestDeplUnProcMessage(self):
        oldest = None
        for m in self.staticMessages:
            if (oldest is None):
                if (m["service_type"] is not None):
                    if ((m["service_type"]).equalsIgnoreCase("unprocessed")):
                        oldest = m
                elif (oldest.getReceiveTime() > m.getReceiveTime(m) and m["service_type"] is not None):
                    if ((m["service_type"]).equalsIgnoreCase("unprocessed")):
                        oldest = m

        return oldest

    @property
    def getNewestProcessMessage(self):
        newest = None
        for m in self.processMessages:
            if (newest is None):
                if (m["service_type"] is not None and m.getProperty("Fresh") is None):
                    if ((m["service_type"]).equalsIgnoreCase("proc")):
                        newest = m
                elif (newest.getReceiveTime() < m.getReceiveTime(m) and m["service_type"] is not None and m.getProperty(
                        "Fresh") is None):
                    if ((m["service_type"]).equalsIgnoreCase("proc")):
                        newest = m

        return newest

    @property
    def getOldestProcessedMessage(self):
        oldest = None
        for m in self.processedMessages:
            if (oldest is None):
                oldest = m

            elif (oldest.getReceiveTime() > m.getReceiveTime(m)):
                oldest = m

        return oldest

    @property
    def getOldestFreshMessage(self):

        oldest = None
        for m in self.processedMessages:
            if (oldest is None):
                if (m.getProperty("Fresh") is not None and m["service_type"] is not None):
                    if ((m.getProperty("Fresh")) and (m["service_type"]).equalsIgnoreCase("processed")):
                        oldest = m
                elif (oldest.getReceiveTime() > m.getReceiveTime(m) and m.getProperty("Fresh") is not None and m[
                    "service_type"] is not None):
                    if ((m.getProperty("Fresh")) and (m["service_type"]).equalsIgnoreCase("processed")):
                        oldest = m

        return oldest

    @property
    def GetNewestFreshMessage(self):

        newest = None
        for m in self.processedMessages:
            if newest is None:
                if (m.getProperty("Fresh") is not None and m["service_type"] is not None):
                    if ((m.getProperty("Fresh")) and (m["service_type"]).equalsIgnoreCase("processed")):
                        newest = m
                elif (newest.getReceiveTime() < m.getReceiveTime(m) and m.getProperty("Fresh") is not None and m[
                    "service_type"] is not None):
                    if ((m.getProperty("Fresh")) and (m["service_type"]).equalsIgnoreCase("processed")):
                        newest = m

        return newest

    @property
    def getOldestShelfMessage(self):

        oldest = None
        for m in self.processedMessages:
            if (oldest is None):
                if (m.getProperty("Fresh") is not None and m["service_type"] is not None):
                    if (not (m.getProperty("Fresh")) and (m["service_type"]).equalsIgnoreCase("processed")):
                        oldest = m
                elif (oldest.getReceiveTime() > m.getReceiveTime(m) and m.getProperty("Fresh") is not None and m[
                    "service_type"] is not None):
                    if (not (m.getProperty("Fresh")) and (m["service_type"]).equalsIgnoreCase("processed")):
                        oldest = m

        return oldest

    @property
    def getNewestShelfMessage(self):

        newest = None
        for m in self.processedMessages:
            if (newest is None):
                if (m.getProperty("Fresh") is not None and m.getProperty("procTime") is not None):
                    if (not (m.getProperty("Fresh")) and (m["service_type"]).equalsIgnoreCase("processed")):
                        newest = m
                elif (newest.getReceiveTime() < m.getReceiveTime(m) and m.getProperty("Fresh") is not None and m[
                    "service_type"] is not None):
                    if (not (m.getProperty("Fresh") and (m["service_type"]).equalsIgnoreCase("processed"))):
                        newest = m

        return newest

    @property
    def getOldestQueueFreshMessage(self):
        curTime = time.time()
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                if (m.getProperty("Fresh") is not None and m.getProperty("procTime") is not None):
                    if ((m.getProperty("Fresh")) and m.getProperty("procTime") <= curTime):
                        oldest = m
                elif (oldest.getReceiveTime() > m.getReceiveTime(m) and m.getProperty(
                        "Fresh") is not None and m.getProperty("procTime") is not None):
                    if ((m.getProperty("Fresh")) and m.getProperty("procTime") <= curTime):
                        oldest = m

        return oldest

    @property
    def getNewestQueueFreshMessage(self):
        curTime = time.time()
        newest = None
        for m in self.processMessages:
            if (newest is None):
                if (m.getProperty("Fresh") is not None and m.getProperty("procTime") is not None):
                    if ((m.getProperty("Fresh")) and m.getProperty("procTime") <= curTime):
                        newest = m

                elif (newest.getReceiveTime() < m.getReceiveTime(m) and m.getProperty(
                        "Fresh") is not None and m.getProperty("procTime") is not None):
                    if ((m.getProperty("Fresh")) and m.getProperty("procTime") <= curTime):
                        newest = m

        return newest

    @property
    def getOldestQueueShelfMessage(self):
        curTime = time.time()
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                if (m.getProperty("Fresh") is not None and m.getProperty("procTime") is not None):
                    if (not (m.getProperty("Fresh")) and m.getProperty("procTime") <= curTime):
                        oldest = m

                elif (oldest.getReceiveTime() > m.getReceiveTime(m) and m.getProperty(
                        "Fresh") is not None and m.getProperty("procTime") is not None):
                    if (not (m.getProperty("Fresh")) and m.getProperty("procTime") <= curTime):
                        oldest = m

        return oldest

    @property
    def getNewestQueueShelfMessage(self):
        curTime = time.time()
        newest = None
        for m in self.processMessages:
            if newest is None:
                if m.getProperty("Fresh") is not None and m.getProperty("procTime") is not None:
                    if not (m.getProperty("Fresh")) and m.getProperty("procTime") <= curTime:
                        newest = m

                elif newest.getReceiveTime() < m.getReceiveTime(m) and m.getProperty(
                        "Fresh") is not None and m.getProperty("procTime") is not None:
                    if not (m.getProperty("Fresh")) and m.getProperty("procTime") <= curTime:
                        newest = m

        return newest

    @property
    def getOldestStaticMessage(self):
        oldest = None
        for m in self.staticMessages:
            if (oldest is None):
                oldest = m

            elif (oldest.getReceiveTime() > m.getReceiveTime(m)):
                oldest = m

        return oldest

    @property
    def getOldestStaleStaticMessage(self):
        curTime = time.time()
        oldest = None
        for m in self.staticMessages:
            if (oldest is None):
                if (m.getProperty("shelfLife") is not None):
                    if ((m.getProperty("shelfLife")) <= curTime - m.getReceiveTime(m)):
                        oldest = m

                elif (oldest.getReceiveTime() > m.getReceiveTime(m) and m.getProperty("shelfLife") is not None):
                    if ((m.getProperty("shelfLife")) <= curTime - m.getReceiveTime(m)):
                        oldest = m

        return oldest

    def getCompressionRate(self):
        return self.compressionRate