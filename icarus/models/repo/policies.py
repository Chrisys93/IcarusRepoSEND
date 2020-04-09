from __future__ import division

import time
from collections import deque, defaultdict
import random
import abc
import copy

import numpy as np

from icarus.util import inheritdoc, apportionment
from icarus.registry import register_repo_policy
from typing import Sized


class Class(Sized):
    def __len__(self) -> int:
        return 1


x = Class()

__all__ = [
    'RepoStorage',
]


# noinspection PyTypeChecker
@register_repo_policy('REPO_STORAGE')
class RepoStorage(object):
    def __init__(self, node, view, contents, storageSize, compressionRate):
        """
		Constructor

		Parameters
		----------
		maxlen :
			The maximum number of items the cache can store
		"""
        self.node = node
        self.view = view
        # self.messages =
        # Collection
        self.Messages = []
        self.processMessages = []
        self.processedMessages = []
        self.storedMessages = []
        self.storageSize = storageSize
        self.processSize = 0
        self.Size = 0
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
        self.depletedMessages = 0
        self.depletedMessagesSize = 0
        self.oldDepletedMessagesSize = 0
        self.depletedCloudMessages = 0
        self.oldDepletedCloudMessagesSize = 0
        self.depletedCloudMessagesSize = 0
        self.cachedMessages = 0
        if self.view.has_computationalSpot(node):
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
        self.messages = self.Messages
        self.messages.extend(self.processMessages)
        return self.messages

    def getStoredMessages(self):
        self.storedMessages = self.Messages
        self.storedMessages.extend(self.processMessages)
        return self.storedMessages

    def getProcessedMessages(self):
        return self.processedMessages

    def getProcessMessages(self):
        return self.processMessages

    def getMessages(self):
        return self.Messages

    def addToStoredMessages(self, sm):
        """
		   TODO: Check indentation herenot (in original, java implementation)
			Also, check the "selfs" in the parantheses. Those should mostly
			be the associated objects for which the functions are called.
			Does the cache have to have a node, or is IT the node? Should
			the simulator reference a node, for routing, or the cache itself?
		"""

        if (sm is not None):
            if sm["service_type"].lower == "nonproc":
                if (self.view.hasStorageCapability(self.node)):
                    self.Messages.append(sm)
                    self.Size += sm.getSize

            elif sm["service_type"].lower == "proc":
                if (self.view.has_computationalSpot(self.node)):
                    self.processMessages.append(sm)
                    self.processSize += sm.getSize

            elif sm["service_type"].lower == "processed":
                self.processedMessages.append(sm)

            elif (sm["service_type"]).lower == "unprocessed":
                if (self.self.view.hasStorageCapability(self.node)):
                    self.Messages.append(sm)
                    self.Size += sm.getSize

                else:
                    self.addToDeplMessages(sm)
            self.totalReceivedMessages += 1
            self.totalReceivedMessagesSize += sm.getSize
        # add space used in the storage space """
        # System.out.prln("There is " + self.getMessagesSize + " storage used")

        if (self.Size + self.processSize) >= self.storageSize:
            for app in self.node.getRouter(self.node).getApplications("ProcApplication"):
                self.procApp = app
            # System.out.prln("App ID is: " + self.procApp.getAppID)

            self.procApp.updateDeplBW(self.node)
            self.procApp.deplStorage(self.node)

    def addToDeplMessages(self, sm):
        if sm is not None:
            self.depletedMessages += 1
            self.depletedMessagesSize += sm.getSize
        if sm['overtime']:
            self.mOvertime += 1
        if sm["service_type"] == "unprocessed":
            self.mUnProcessed += 1
            self.depletedUnProcMessages += 1
            self.depletedUnProcMessagesSize += sm.getSize
        if sm['satisfied']:
            self.mSatisfied += 1
        else:
            self.mUnSatisfied += 1
        if sm['storTime'] is not None:
            self.mStorTimeNo += 1
            self.mStorTime += sm['storTime']
        if self.mStorTimeMax < sm['storTime']:
            self.mStorTimeMax = sm['storTime']

        else:
            curTime = time.time()
            sm['storTime'] = curTime - sm['receiveTime'](sm)
            self.mStorTimeNo += 1
            self.mStorTime += sm['storTime']
        if self.mStorTimeMax < sm['storTime']:
            self.mStorTimeMax = sm['storTime']

    def addToDeplProcMessages(self, sm):
        if (sm is not None):
            self.depletedProcMessages += 1
            self.depletedProcMessagesSize += sm.getSize
            if (sm['overtime']):
                self.mOvertime += 1
                self.mUnSatisfied += 1
            if (sm["service_type"] == "unprocessed"):
                self.mUnProcessed += 1
                self.depletedUnProcMessages += 1
                self.depletedUnProcMessagesSize += sm.getSize

                if (sm['storTime'] is not None):
                    self.mStorTimeNo += 1
                    self.mStorTime += sm['storTime']
                if (self.mStorTimeMax < sm['storTime']):
                    self.mStorTimeMax = sm['storTime']

            else:
                curTime = time.time()
                sm['storTime'] = curTime - sm['receiveTime'](sm)
                self.mStorTimeNo += 1
                self.mStorTime += sm['storTime']
                if (self.mStorTimeMax < sm['storTime']):
                    self.mStorTimeMax = sm['storTime']

    def addToCloudDeplMessages(self, sm):
        if (sm is not None):
            self.depletedCloudMessages += 1
            self.depletedCloudMessagesSize += sm.getSize
            if (sm['overtime']):
                self.mOvertime += 1
            if (sm["service_type"] == "unprocessed"):
                self.mUnProcessed += 1
                self.depletedUnProcMessages += 1
                self.depletedUnProcMessagesSize += sm.getSize

            if (sm['satisfied']):
                self.mSatisfied += 1
            else:
                self.mUnSatisfied += 1
            if (sm['storTime'] is not None):
                self.mStorTimeNo += 1
                self.mStorTime += sm['storTime']
            if (self.mStorTimeMax < sm['storTime']):
                self.mStorTimeMax = sm['storTime']

            else:
                curTime = time.time()
                sm['storTime'] = curTime - sm['receiveTime'](sm)
                self.mStorTimeNo += 1
                self.mStorTime += sm['storTime']
            if (self.mStorTimeMax < sm['storTime']):
                self.mStorTimeMax = sm['storTime']

    def addToDeplUnProcMessages(self, sm):
        sm.update("type", "unprocessed")
        self.self.view.repoStorage[self.node].addToStoredMessages(sm)

    def addToDepletedUnProcMessages(self, sm):
        if (sm is not None):
            if (sm["service_type"] == "unprocessed"):
                self.depletedUnProcMessages += 1
            self.depletedUnProcMessagesSize += sm.getSize
            self.mUnProcessed += 1

    def getMessage(self, MessageId):
        Message = None
        for temp in self.Messages:
            if (temp['content'] == MessageId):
                i = self.Messages.index(temp)
                Message = self.Messages[i]
        return Message

    def getProcessedMessage(self, MessageId):
        processedMessage = None
        for temp in self.processedMessages:
            if (temp['content'] == MessageId):
                i = self.processedMessages.index(temp)
                processedMessage = self.processedMessages[i]
        return processedMessage

    def getProcessMessage(self, MessageId):
        processMessage = None
        for temp in self.processMessages:
            if (temp['content'] == MessageId):
                i = self.processMessages.index(temp)
                processMessage = self.processMessages[i]
        return processMessage

    def getStorTimeNo(self):
        return self.mStorTimeNo

    def getStorTimeAvg(self):
        self.mStorTimeAvg = self.mStorTime / self.mStorTimeNo
        return self.mStorTimeAvg

    def getStorTimeMax(self):
        return self.mStorTimeMax

    @property
    def getNrofMessages(self):
        return len(self.Messages)

    def getNrofProcessMessages(self):
        return len(self.processMessages)

    def getNrofProcessedMessages(self):
        return len(self.processedMessages)

    def getMessagesSize(self):
        return self.Size

    def getStaleMessagesSize(self):
        curTime = time.time()
        size = 0
        for m in self.Messages:
            if m['shelfLife'] is not None and (m['shelfLife']) <= curTime - m['receiveTime'](m):
                size += m.getSize
                return size

    def getProcMessagesSize(self):
        return self.processSize

    def getProcessedMessagesSize(self):
        processedUsed = 0
        for msg in self.processedMessages:
            processedUsed += msg.getSize
            return processedUsed

    def getFullCachedMessagesNo(self):
        """
			Need to add the feedback "functions" and erfacing
			TODO: Add the outward feedback message generation definitions,
				like the gets below, under them.
		"""
        proc = self.cachedMessages
        self.cachedMessages = 0
        return proc

    """
	*Returns the node self repo storage system is in
	* @ return The node object
	"""

    def getnode(self):
        return self.node

    def hasMessage(self, MessageId, labels):
        answer = None
        for j in range(0, len(self.processedMessages)):
            if MessageId is not None and self.processedMessages[j]['content'] == MessageId:
                answer = self.processedMessages[j]
            else:
                j_labels = []
                for label, count in self.processedMessages[j]['labels']:
                    if label in labels:
                        j_labels.append(label)
                if (j_labels == labels):
                    answer = self.processedMessages[j]
        for i in range(0, len(self.Messages)):
            if MessageId is not None and self.Messages[i]['content'] == MessageId:
                answer = self.Messages[i]
            else:
                j_labels = []
                for label, count in self.Messages[j]['labels']:
                    if label in labels:
                        j_labels.append(label)
                if (j_labels == labels):
                    answer = self.Messages[j]
        for i in range(0, len(self.processMessages)):
            if MessageId is not None and self.processMessages[i]['content'] == MessageId:
                answer = self.processMessages[i]
            else:
                j_labels = []
                for label, count in self.processMessages[j]['labels']:
                    if label in labels:
                        j_labels.append(label)
                if (j_labels == labels):
                    answer = self.processMessages[j]
        return answer

    def getProcessedMessages(self, labels):
        answer = None
        for j in range(0, len(self.processedMessages)):
            if MessageId is not None and self.processedMessages[j]['content'] == MessageId:
                answer = self.processedMessages[j]
            else:
                j_labels = []
                for label, count in self.processedMessages[j]['labels']:
                    if label in labels:
                        j_labels.append(label)
                if (j_labels == labels):
                    answer = self.processedMessages[j]
        for i in range(0, len(self.Messages)):
            if MessageId is not None and self.Messages[i]['content'] == MessageId:
                answer = self.Messages[i]
            else:
                j_labels = []
                for label, count in self.Messages[j]['labels']:
                    if label in labels:
                        j_labels.append(label)
                if (j_labels == labels):
                    answer = self.Messages[j]
        for i in range(0, len(self.processMessages)):
            if MessageId is not None and self.processMessages[i]['content'] == MessageId:
                answer = self.processMessages[i]
            else:
                j_labels = []
                for label, count in self.processMessages[j]['labels']:
                    if label in labels:
                        j_labels.append(label)
                if (j_labels == labels):
                    answer = self.processMessages[j]
        return answer

    def deleteMessage(self, MessageId):
        for i in range(0, len(self.Messages)):
            if (self.Messages[i]["content"] == MessageId):
                self.Size -= self.Messages[i].getSize
                self.Messages.remove(i)
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
        for i in (0, len(self.processMessages)):
            if (self.processMessages[i]['content'] == MessageId):
                self.processSize -= self.processMessages[i].getSize
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
            if m["service_type"].lower == "proc" and self.deleteProcMessage(MessageId):
                return True

            elif m["service_type"].lower == "nonproc" and self.deleteMessage(MessageId):
                return True

            elif m["service_type"].lower == "unprocessed" and self.deleteMessage(MessageId):
                return True

            if not self.getself.view.hasStorageCapability(self.node):
                self.nrofDeletedMessages += 1
                self.deletedMessagesSize += m.getSize

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
        for i in range(0, len(self.processedMessages)):
            if self.processedMessages[i]['content'] == MessageId:
                self.depletedCloudProcMessages += 1
                self.depletedCloudProcMessagesSize += self.processedMessages[i].getSize
                if (report):
                    if (self.processedMessages[i]['overtime'] is not None):
                        if (self.processedMessages[i]['overtime']):
                            self.mOvertime += 1

                    if (self.processedMessages[i]['satisfied'] is not None):
                        if (self.processedMessages[i]['satisfied']):
                            self.mSatisfied += 1
                        else:
                            self.mUnSatisfied += 1

                    if (self.processedMessages[i]['Fresh'] is not None):
                        if (self.processedMessages[i]['Fresh']):
                            self.mFresh += 1
                        elif (not self.processedMessages[i]['Fresh']):
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

    def getNrofDepletedMessages(self):
        return self.depletedMessages

    def getNrofDepletedCloudMessages(self):
        return self.depletedCloudMessages

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
	* as a  method of the update, or for checking BW usage.
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
	* as a  method of the update, or for checking BW usage.
	* @
	return the
	
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
	* as a  method of the update, or for checking BW usage.
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
	* as a  method of the update, or for checking BW usage.
	* @
	return the
	non - processing
	depletion
	BW
	used
	upstream
	"""

    def getDepletedCloudMessagesBW(self, reporting):
        statBW = self.depletedCloudMessagesSize - self.oldDepletedCloudMessagesSize
        if (reporting):
            self.oldDepletedCloudMessagesSize = self.depletedCloudMessagesSize

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
	* as a  method of the update, or for checking BW usage.
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
	* as a  method of the update, or for checking BW usage.
	* @
	return the
	non - processing
	depletion
	BW
	used
	upstream
	"""

    def getDepletedMessagesBW(self, reporting):
        statBW = self.depletedMessagesSize - self.oldDepletedMessagesSize
        if (reporting):
            self.oldDepletedMessagesSize = self.depletedMessagesSize
        return (statBW)

    def clearAllMessages(self):
        self.Messages.clear
        return True

    def getFreeStorageSpace(self):
        usedStorage = self.getMessagesSize
        # System.out.prln("There is " + usedStorage + " storage used in " + self.getnode)
        freeStorage = self.storageSize - usedStorage
        # System.out.prln("There is " + freeStorage + " free storage space in " + self.getnode)
        return freeStorage

    """

	isStorageFull(self):
	usedStorage = self.getMessagesSize
						if (usedStorage >= self.storageSize - 100000000):
							return True

						else:
						return False





						isProcessingFull
						:

						usedProc = self.getProcMessagesSize
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
        # System.setOut( PrStream( FileOutputStream("log.txt")))
        # catch(Exception e):
        if (self.processSize <= 2000000):
            # System.out.prln("There is enough storage space: " + freeStorage)
            return True

        else:
            # System.out.prln("There is not enough storage space: " + freeStorage)
            return False

    def isProcessedFull(self):
        usedProcessed = self.getProcessedMessagesSize
        # try:
        # System.setOut( Stream( FileOutputStream("log.txt")))
        # catch(Exception e):
        if (usedProcessed >= self.processedSize - 500000):
            # System.out.prln("There is enough storage space: " + freeStorage)
            return True

        else:
            # System.out.prln("There is not enough storage space: " + freeStorage)
            return False

    def isProcessedEmpty(self):
        usedProc = self.getProcessedMessagesSize
        # try:
        # System.setOut( PrStream( FileOutputStream("log.txt")))
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

            elif (oldest['receiveTime'] > m['receiveTime'](m)):
                oldest = m

        return oldest

    @property
    def getOldestValidProcessMessage(self):
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                if (m["service_type"] is not None and m['Fresh'] is None):
                    if (not (m["service_type"]).lower == "unprocessed" and not (
                                                                                       m[
                                                                                           "service_type"]).lower == "processed"):
                        oldest = m

                elif oldest['receiveTime'] > m['receiveTime'](m) and m["service_type"] is not None and m[
                    'Fresh'] is None:
                    if not (m["service_type"]).lower == "unprocessed" and not (
                                                                                      m[
                                                                                          "service_type"]).lower == "processed":
                        oldest = m

        return oldest

    @property
    def getOldestInvalidProcessMessage(self):
        curTime = time.time()
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                if (m["service_type"] is not None and m['shelfLife'] is not None):
                    if ((m["service_type"]).lower == "proc" and (
                            m['shelfLife']) <= curTime - m['receiveTime'](m)):
                        oldest = m

                elif (oldest['receiveTime'] > m['receiveTime'](m) and m["service_type"] is not None and m.getProperty(
                        "shelfLife") is not None):
                    if ((m["service_type"]).lower == "proc" and (
                            m['shelfLife']) <= curTime - m['receiveTime'](m)):
                        oldest = m

        return oldest

    @property
    def getOldestDeplUnProcMessage(self):
        oldest = None
        for m in self.Messages:
            if (oldest is None):
                if (m["service_type"] is not None):
                    if ((m["service_type"]).lower == "unprocessed"):
                        oldest = m
                elif (oldest['receiveTime'] > m['receiveTime'](m) and m["service_type"] is not None):
                    if ((m["service_type"]).lower == "unprocessed"):
                        oldest = m

        return oldest

    @property
    def getNewestProcessMessage(self):
        newest = None
        for m in self.processMessages:
            if (newest is None):
                if (m["service_type"] is not None and m['Fresh'] is None):
                    if ((m["service_type"]).lower == "proc"):
                        newest = m
                elif (newest['receiveTime'] < m['receiveTime'](m) and m["service_type"] is not None and m[
                    'Fresh'] is None):
                    if ((m["service_type"]).lower == "proc"):
                        newest = m

        return newest

    @property
    def getOldestProcessedMessage(self):
        oldest = None
        for m in self.processedMessages:
            if (oldest is None):
                oldest = m

            elif (oldest['receiveTime'] > m['receiveTime'](m)):
                oldest = m

        return oldest

    @property
    def getOldestFreshMessage(self):

        oldest = None
        for m in self.processedMessages:
            if (oldest is None):
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if ((m['Fresh']) and (m["service_type"]).lower == "processed"):
                        oldest = m
                elif (oldest['receiveTime'] > m['receiveTime'](m) and m['Fresh'] is not None and m[
                    "service_type"] is not None):
                    if ((m['Fresh']) and (m["service_type"]).lower == "processed"):
                        oldest = m

        return oldest

    @property
    def GetNewestFreshMessage(self):

        newest = None
        for m in self.processedMessages:
            if newest is None:
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if ((m['Fresh']) and (m["service_type"]).lower == "processed"):
                        newest = m
                elif (newest['receiveTime'] < m['receiveTime'](m) and m['Fresh'] is not None and m[
                    "service_type"] is not None):
                    if ((m['Fresh']) and (m["service_type"]).lower == "processed"):
                        newest = m

        return newest

    @property
    def getOldestShelfMessage(self):

        oldest = None
        for m in self.processedMessages:
            if (oldest is None):
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if not (m['Fresh']) and (m["service_type"]).lower == "processed":
                        oldest = m
                elif (oldest['receiveTime'] > m['receiveTime'](m) and m['Fresh'] is not None and m[
                    "service_type"] is not None):
                    if (not (m['Fresh']) and (m["service_type"]).lower == "processed"):
                        oldest = m

        return oldest

    @property
    def getNewestShelfMessage(self):

        newest = None
        for m in self.processedMessages:
            if (newest is None):
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if (not (m['Fresh']) and (m["service_type"]).lower == "processed"):
                        newest = m
                elif (newest['receiveTime'] < m['receiveTime'](m) and m['Fresh'] is not None and m[
                    "service_type"] is not None):
                    if (not (m['Fresh'] and (m["service_type"]).lower == "processed")):
                        newest = m

        return newest

    """
    @property
    def getOldestQueueFreshMessage(self):
        curTime = time.time()
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if ((m['Fresh']) and m["service_type"] <= curTime):
                        oldest = m
                elif (oldest['receiveTime'] > m['receiveTime'](m) and m['Fresh'] is not None and m.getProperty(
                        "procTime") is not None):
                    if ((m['Fresh']) and m["service_type"] <= curTime):
                        oldest = m

        return oldest

    @property
    def getNewestQueueFreshMessage(self):
        curTime = time.time()
        newest = None
        for m in self.processMessages:
            if (newest is None):
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if ((m['Fresh']) and m["service_type"] <= curTime):
                        newest = m

                elif newest['receiveTime'] < m['receiveTime'](m) and m['Fresh'] is not None and m.getProperty(
                        "procTime") is not None:
                    if (m['Fresh']) and m["service_type"] <= curTime:
                        newest = m

        return newest

    @property
    def getOldestQueueShelfMessage(self):
        curTime = time.time()
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if (not (m['Fresh']) and m["service_type"] <= curTime):
                        oldest = m

                elif (oldest['receiveTime'] > m['receiveTime'](m) and m['Fresh'] is not None and m.getProperty(
                        "procTime") is not None):
                    if (not (m['Fresh']) and m["service_type"] <= curTime):
                        oldest = m

        return oldest

    @property
    def getNewestQueueShelfMessage(self):
        curTime = time.time()
        newest = None
        for m in self.processMessages:
            if newest is None:
                if m['Fresh'] is not None and m["service_type"] is not None:
                    if not (m['Fresh']) and m["service_type"] <= curTime:
                        newest = m

                elif newest['receiveTime'] < m['receiveTime'](m) and m['Fresh'] is not None and m.getProperty(
                        "procTime") is not None:
                    if not (m['Fresh']) and m["service_type"] <= curTime:
                        newest = m

        return newest
    """
    @property
    def getOldestMessage(self):
        oldest = None
        for m in self.Messages:
            if (oldest is None):
                oldest = m

            elif (oldest['receiveTime'] > m['receiveTime'](m)):
                oldest = m

        return oldest

    @property
    def getOldestStaleMessage(self):
        curTime = time.time()
        oldest = None
        for m in self.Messages:
            if (oldest is None):
                if (m['shelfLife'] is not None):
                    if ((m['shelfLife']) <= curTime - m['receiveTime'](m)):
                        oldest = m

                elif (oldest['receiveTime'] > m['receiveTime'](m) and m['shelfLife'] is not None):
                    if ((m['shelfLife']) <= curTime - m['receiveTime'](m)):
                        oldest = m

        return oldest

    def getCompressionRate(self):
        return self.compressionRate


# TODO: NEED TO REVIEW AND REVISE ALL OF THE CODE BELOWnot not not not not not not not not not not not
#  \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/


class ProcApplication(object):
    """
	Run in passive mode - don't process messages, but store
	"""

    def __init__(self, s, view):

        """
		TODO: REVISE VARIABLE FETCHING!
			(CHECK HOW TO GET Settings - s - from the config file, through the orchestrator)

		s: dict of settings
			Settings dict imported from config file, through orchestrator
		view: object
			Descriptive view of network status via graph
		"""

        PROC_PASSIVE = "passive"
        """ Run in storage mode - store non-proc messages for as 
		* as possible before depletion
		* Possible settings: store or compute """

        STOR_MODE = "storageMode"
        """ If
		running in storage
		mode - store
		non - proc
		messages
		not er
			* than
		the
		maximum
		storage
		time. """

        MAX_STOR_TIME = "maxStorTime"
        """ Percentage(below
		unity) of
		maximum
		storage
		occupied """

        MAX_STOR = "maxStorage"
        """ Percentage(below
		unity) of
		minimum
		storage
		occupied
		before
		any
		depletion """

        MIN_STOR = "minStorage"
        """ Depletion
		rate """

        DEPL_RATE = "depletionRate"
        """ Cloud
		Max
		Update
		rate """

        CLOUD = "cloudRate"
        """ Numbers
		of
		processing
		cores """

        PROC_NO = "coreNo"

        """ Application
		ID """

        APP_ID = "ProcApplication"

        # vars

        lastDepl = 0

        cloudEmptyLoop = True

        deplEmptyLoop = True

        upEmptyLoop = True

        passive = False

        lastCloudUpload = 0

        deplBW = 0

        cloudBW = 0

        procMinI = 0
        # processedSize = self.procSize * self.proc_ratio
        if s.contains(PROC_PASSIVE):
            self.passive = s.get(PROC_PASSIVE)

        if s.contains(STOR_MODE):
            self.storMode = s.get(STOR_MODE)

        else:
            self.storMode = False

        if (s.contains(MAX_STOR_TIME)):
            self.maxStorTime = s.get(MAX_STOR_TIME)
        else:
            self.maxStorTime = 2000

        if (s.contains(DEPL_RATE)):
            self.depl_rate = s.get(DEPL_RATE)

        if (s.contains(CLOUD)):
            self.cloud_lim = s.get(CLOUD)

        if (s.contains(MAX_STOR)):
            self.max_stor = s.get(MAX_STOR)

        if (s.contains(MIN_STOR)):
            self.min_stor = s.get(MIN_STOR)

        self.view = view
        self.appID = APP_ID

        if (self.storMode):
            self.maxStorTime = self.maxStorTime
        else:
            self.maxStorTime = 0

    # self.processedSize = a.getProcessedSize

    def getAppID(self):
        return self.appID

    """ 
	 * Sets the application ID. Should only set once when the application is
	 * created. Changing the value during simulation runtime is not recommended
	 * unless you really know what you're doing.
	 * 
	 * @param appID
	"""

    def setAppID(self, appID):
        self.appID = appID

    def replicate(self, ProcApplication):
        return ProcApplication(self)

    def handle(self, msg, node):
        if (self.view.hasStorageCapability(node) and node.hasProcessingCapability):
            self.view.repoStorage[node].addToStoredMessages(msg)

        elif not self.view.hasStorageCapability(node) and node.hasProcessingCapability and (
                msg['type']).lower == "nonproc":
            curTime = time.time()
            self.view.repoStorage[node].deleteMessage(msg["content"])
            storTime = curTime - msg['receiveTime']
            msg['storTime'] = storTime
            if (msg['storTime'] < msg['shelfLife']):
                msg['satisfied'] = False
                msg['overtime'] = False
            self.view.repoStorage[node].addToDeplMessages(msg)

        else:
            msg['satisfied'] = True
            msg['overtime'] = False
            self.view.repoStorage[node].addToDeplMessages(msg)

        return msg

    def process_event(self, node):

        # System.out.prln("processor update is accessed")

        """
	* DEPLETION PART HERE
	*
	* Depletion has to be done in the following order, and ONLY WHEN STORAGE IS FULL up to a certain lower limitnot
	* non-processing with shelf-life expired, processing with shelf-life expired, non-processing with shelf-life,
	* processing with shelf-life
	*
	* A different depletion pipeline must be made, so that on each update, the processed messages are also sent through,
	* but self should be limited by a specific namespace / erface limit per second...
	*
	* With these, messages also have to be tagged (tags added HERE, at depletion) with: storage time, for shelf - life
	* correlations, in -time processing tags, for analytics, overtime  tags, shelf-life processing confirmation
	*  tags
	* The other tags should be deleted AS THE MESSAGES ARE PROCESSED / COMPRESSED / DELETEDnot
	*
		"""

        if self.view.hasStorageCapability(node):

            self.updateCloudBW(node)
            self.deplCloud(node)
            self.updateDeplBW(node)
            self.deplStorage(node)

        elif not self.view.hasStorageCapability(node) and self.view.has_computationalSpot(node):
            self.updateUpBW(node)
            self.deplUp(node)

    def updateCloudBW(self, node):
        self.cloudBW = self.view.repoStorage[node].getDepletedCloudProcMessagesBW(False) + \
                       self.view.repoStorage[node].getDepletedUnProcMessagesBW(False) + \
                       self.view.repoStorage[node].getDepletedCloudMessagesBW(False)

    def updateUpBW(self, node):
        self.cloudBW = self.view.repoStorage[node].getDepletedCloudProcMessagesBW(False) + \
                       self.view.repoStorage[node].getDepletedUnProcMessagesBW(False) + \
                       self.view.repoStorage[node].getDepletedMessagesBW(False)

    def updateDeplBW(self, node):
        self.deplBW = self.view.repoStorage[node].getDepletedProcMessagesBW(False) + \
                      self.view.repoStorage[node].getDepletedUnProcMessagesBW(False) + \
                      self.view.repoStorage[node].getDepletedMessagesBW(False)

    def deplCloud(self, node):
        curTime = time.time()
        if (self.view.repoStorage[node].getProcessedMessagesSize +
                self.view.repoStorage[node].getStaleMessagesSize >
                (self.view.repoStorage[node].getTotalStorageSpace * self.min_stor)):
            self.cloudEmptyLoop = True

            """
			* self for loop stops processed messages from being deleted within later time frames,
			* if there are satisfied non-processing messages to be uploaded.
			*
			* OK, so main problem

			*At the moment, the mechanism is fine, but because of it, the perceived "processing performance" 
			* is degraded, due to the fact that some messages processed in time may not be shown in the
			*"fresh" OR "stale" message counts. 
			*Well...it actually doesn't influence it that much, so it would mostly be just for correctness, really...
			"""

            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:
                """
				* Oldest processed message is depleted (as a FIFO type of storage,
				* and a  message for processing is processed
				"""

                if not self.view.repoStorage[node].isProcessedEmpty():
                    self.processedDepletion(node)
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.repoStorage[node].getOldestDeplUnProcMessage() is not None:
                    self.oldestUnProcDepletion(node)

                elif not self.storMode and self.view.repoStorage[node].getOldestStaleMessage()() is not None:
                    self.oldestSatisfiedDepletion(node)

                elif self.storMode and curTime - self.lastCloudUpload >= self.maxStorTime:
                    self.storMode = False

                else:
                    self.cloudEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.cloudBW)

                # Revise:
                self.updateCloudBW(node)

                # System.out.prln("Depletion is at: " + deplBW)
                self.lastDepl = curTime
                """System.out.prln("self.cloudBW is at " +
					  self.cloudBW +
					  " self.cloud_lim is at " +
					  self.cloud_lim +
					  " self.view.repoStorage[node].getTotalStorageSpace*self.min_stor equal " +
					  (self.view.repoStorage[node].getTotalStorageSpace * self.min_stor) +
					  " Total space is " + self.view.repoStorage[node].getTotalStorageSpace) """
            # System.out.prln("Depleted  messages: " + sdepleted)
        elif (self.view.repoStorage[node].getProcessedMessagesSize +
                self.view.repoStorage[node].getStaleMessagesSize) > \
                (self.view.repoStorage[node].getTotalStorageSpace * self.max_stor):
            self.cloudEmptyLoop = True
            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                if (self.view.repoStorage[node].getOldestStaleMessage() is not None and
                        self.cloudBW < self.cloud_lim):
                    self.oldestSatisfiedDepletion(node)

                    """
					* Oldest unprocessed messages should be given priority for depletion
					* at a certain po.
					"""

                elif (self.view.repoStorage[node].getOldestDeplUnProcMessage is not None):
                    self.oldestUnProcDepletion(node)

                    """
				* Oldest processed message is depleted (as a FIFO type of storage,
				* and a  message for processing is processed
					"""
                elif (not self.view.repoStorage[node].isProcessedEmpty):
                    self.processedDepletion(node)

                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateCloudBW(node)
                    # System.out.prln("Depletion is at: " + deplBW)
                    self.lastDepl = curTime
                    """System.out.prln("self.cloudBW is at " +
					  self.cloudBW +
					  " self.cloud_lim is at " +
					  self.cloud_lim +
					  " self.view.repoStorage[node].getTotalStorageSpace*self.min_stor equal " +
					  (self.view.repoStorage[node].getTotalStorageSpace * self.min_stor) +
					  " Total space is " + self.view.repoStorage[node].getTotalStorageSpace) """
                # System.out.prln("Depleted  messages: " + sdepleted)

    def deplUp(self, node):
        curTime = time.time()
        if (self.view.repoStorage[node].getProcessedMessagesSize >
                (self.view.repoStorage[node].getTotalStorageSpace * self.min_stor)):

            self.cloudEmptyLoop = True

            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """
			
				* Oldest processed	message is depleted(as a FIFO type of storage,
				* and a	message for processing is processed
				
				"""
                # TODO: NEED TO add COMPRESSED PROCESSED messages to storage AFTER normal servicing
                if (not self.view.repoStorage[node].isProcessedEmpty):
                    self.processedDepletion(node)

                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                # elif (self.view.repoStorage[node].getOldestDeplUnProcMessage is not None)
                # oldestUnProcDepletion(node)
                #
                #				elif (not self.storMod e and self.view.repoStorage[node].getOldestStaleMessage() is not None)
                #					oldestSatisfiedDepletion(node)
                #
                #				elif (self.storMod e and curTime - self.lastCloudUpload >= self.maxStorTime)
                #					self.storMode = False
                #
                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateUpBW(node)

            # System.out.prln("Depletion is at : "+ deplBW)
            self.lastDepl = curTime
            """System.out.prln("self.cloudBW is at " +
					 self.cloudBW +
					 " self.cloud_lim is at " +
					 self.cloud_lim +
					 " self.view.repoStorage[node].getTotalStorageSpace*self.min_stor equa l "+
					 (self.view.repoStorage[node].getTotalStorageSpac e *self.min_st or)+
					 " Total space i s  "+self.view.repoStorage[node].getTotalStorageSpace( ) )"""
            # System.out.prln("Depleted  messages : "+ sdepleted)

            self.upEmptyLoop = True
        for i in range(0, 50) and self.cloudBW > self.cloud_lim and \
                 not self.view.repoStorage[node].isProcessingEmpty() and self.upEmptyLoop:
            if (not self.view.repoStorage[node].isProcessedEmpty):
                self.processedDepletion(node)

            elif (not self.view.repoStorage[node].isProcessingEmpty):
                self.view.repoStorage[node].deleteMessage(self.view.repoStorage[node].getOldestProcessMessage["content"])
            else:
                self.upEmptyLoop = False

    # System.out.prln("Depletion is at: "+ self.cloudBW)

    def deplStorage(self, node):
        curTime = time.time()
        if (self.view.repoStorage[node].getProcMessagesSize +
                self.view.repoStorage[node].getMessagesSize >
                self.view.repoStorage[node].getTotalStorageSpace * self.max_stor):
            self.deplEmptyLoop = True
            for i in range(0, 50) and self.deplBW < self.depl_rate and self.deplEmptyLoop:
                if (self.view.repoStorage[node].getOldestDeplUnProcMessage is not None):
                    self.oldestUnProcDepletion(node)
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                elif (self.view.repoStorage[node].getOldestStaleMessage() is not None):
                    self.oldestSatisfiedDepletion(node)


                elif (self.view.repoStorage[node].getOldestInvalidProcessMessage is not None):
                    self.oldestInvalidProcDepletion(node)

                elif (self.view.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.repoStorage[node].getOldestMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp["content"])
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.repoStorage[node].addToDeplMessages(ctemp)
                    if (ctemp['type']).lower == "unprocessed":
                        self.view.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    else:
                        self.view.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.repoStorage[node].getNewestProcessMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp["content"])
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.repoStorage[node].addToDeplProcMessages(ctemp)
                if storTime <= ctemp['shelfLife'] + 1:
                    ctemp['overtime'] = False

                elif storTime > ctemp['shelfLife'] + 1:
                    ctemp['overtime'] = True

                if (ctemp['type']).lower == "unprocessed":
                    self.view.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                else:
                    self.view.repoStorage[node].addToDeplMessages(ctemp)
            else:
                self.deplEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.deplBW)

                self.updateDeplBW(node)
                self.updateCloudBW(node)
            # Revise:
            self.lastDepl = curTime

    def processedDepletion(self, node):
        curTime = time.time()
        if (self.view.repoStorage[node].getOldestFreshMessage is not None):
            if (self.view.repoStorage[node].getOldestFreshMessage["service_type"] is None):
                temp = self.view.repoStorage[node].getOldestFreshMessage
                report = False
                self.view.repoStorage[node].deleteProcessedMessage(temp["content"], report)
                """
				* Make sure here that the added message to the cloud depletion 
				* tracking is also tracked by whether it 's Fresh or Stale.
				"""
                report = True
                self.view.repoStorage[node].addToStoredMessages(temp)
                self.view.repoStorage[node].deleteProcessedMessage(temp["content"], report)



            elif (self.view.repoStorage[node].getOldestShelfMessage() is not None):
                if (self.view.repoStorage[node].getOldestShelfMessage()["service_type"] is None):
                    temp = self.view.repoStorage[node].getOldestShelfMessage()
                    report = False
                    self.view.repoStorage[node].deleteProcessedMessage(temp["content"], report)
                    """
					* Make sure here that the added message to the cloud depletion
					* tracking is also tracked by whether it's Fresh or Stale.
					"""
                    """  storTime = curTime - temp['receiveTime']
						temp['storTime'] =  storTime)
						# temp['satisfied'] =  False)
						if (storTime == temp['shelfLife']) 
						temp['overtime'] =  False)

						elif (storTime > temp['shelfLife']) 
						temp['overtime'] =  True)
					 """
                    report = True
                    self.view.repoStorage[node].addToStoredMessages(temp)
                    self.view.repoStorage[node].deleteProcessedMessage(temp["content"], report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.repoStorage[node].getOldestStaleMessage()
        self.view.repoStorage[node].deleteMessage(ctemp["content"])
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.repoStorage[node].addToCloudDeplMessages(ctemp)

        self.lastCloudUpload = curTime

    def oldestInvalidProcDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.repoStorage[node].getOldestInvalidProcessMessage
        self.view.repoStorage[node].deleteMessage(ctemp["content"])
        ctemp = self.compressMessage(node, ctemp)
        self.view.repoStorage[node].deleteMessage(ctemp["content"])
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False

        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        ctemp['satisfied'] = False
        self.view.repoStorage[node].addToDeplProcMessages(ctemp)

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.repoStorage[node].getOldestDeplUnProcMessage
        self.view.repoStorage[node].deleteMessage(temp["content"])
        self.view.repoStorage[node].addToDepletedUnProcMessages(temp)

    """
	* @
	return the
	lastProc
	"""

    def getLastProc(self):
        return self.lastProc

    """
	* @ param
	lastProc
	the
	lastProc
	to
	set
	"""

    def setLastProc(self, lastProc):
        self.lastProc = lastProc

    """
	* @ return the
	lastProc
	"""

    def getLastDepl(self):
        return self.lastDepl

    """
	* @ return the
	lastProc
	"""

    def getMaxStorTime(self):
        return self.maxStorTime

    """
	* @ return the
	lastProc
	"""

    def setLastDepl(self, lastDepl):
        self.lastDepl = lastDepl

    """
	* @ return the
	passive
	"""

    def isPassive(self):
        return self.passive

    """
	* @ return storMode
	"""

    def inStorMode(self):
        return self.storMode

    """
	* @ param
	passive 
	the passive to set
	"""

    def setPassive(self, passive):
        self.passive = passive

    """
	* @ return depletion
	rate
	"""

    def getMaxStor(self):
        return self.max_stor

    """
	* @ return depletion
	rate
	"""

    def getMinStor(self):
        return self.min_stor

    """
	* @ return depletion
	rate
	"""

    def getDeplRate(self):
        return self.depl_rate

    """
	* @ return depletion
	rate
	"""

    def getCloudLim(self):
        return self.cloud_lim
