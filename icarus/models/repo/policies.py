# -*- coding: utf-8 -*-
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
    'RepoNull'
]


# noinspection PyTypeChecker
@register_repo_policy('REPO_STORAGE')
class RepoStorage(object):
    def __init__(self, node, model, contents, storageSize, compressionRate=0.5):
        """
		Constructor

		Parameters
		----------
		maxlen :
			The maximum number of items the cache can store
		"""
        self.node = node
        self.model = model
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
        if self.model.comp_size[node]:
            # self.processSize = processSize
            self.compressionRate = compressionRate
            processedRatio = self.compressionRate * 2
            self.processedSize = self.storageSize / processedRatio
        if contents is not None:
            for content in contents:
                self.addToStoredMessages(contents[content])

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
    # @profile
    def addToStoredMessages(self, sm):
        """
		   TODO: Check indentation herenot (in original, java implementation)
			Also, check the "selfs" in the parantheses. Those should mostly
			be the associated objects for which the functions are called.
			Does the cache have to have a node, or is IT the node? Should
			the simulator reference a node, for routing, or the cache itself?
		"""

        if (sm is not None):
            if sm["service_type"].lower() == "non-proc":
                self.Messages.append(sm)
                self.Size += sm['msg_size']

            elif sm["service_type"].lower() == "proc":
                self.processMessages.append(sm)
                self.processSize += sm['msg_size']

            elif sm["service_type"].lower() == "processed":
                self.processedMessages.append(sm)

            elif (sm["service_type"]).lower() == "unprocessed":
                self.Messages.append(sm)
                self.Size += sm['msg_size']

            else:
                self.addToDeplMessages(sm)
            self.totalReceivedMessages += 1
            self.totalReceivedMessagesSize += sm['msg_size']
        # add space used in the storage space """
        # System.out.prln("There is " + self.getMessagesSize + " storage used")

        # if (self.Size + self.processSize) >= self.storageSize:
        #     for app in self.node.getRouter(self.node).getApplications("ProcApplication"):
        #         self.procApp = app
        #     # System.out.prln("App ID is: " + self.procApp.getAppID)
        #
        #     self.procApp.updateDeplBW(self.node)
        #     self.procApp.deplStorage(self.node)

    def addToDeplMessages(self, sm):
        if sm is not None:
            self.depletedMessages += 1
            self.depletedMessagesSize += sm['msg_size']
        if sm['overtime']:
            self.mOvertime += 1
        if sm["service_type"] == "unprocessed":
            self.mUnProcessed += 1
            self.depletedUnProcMessages += 1
            self.depletedUnProcMessagesSize += sm['msg_size']
        if sm['satisfied']:
            self.mSatisfied += 1
        else:
            self.mUnSatisfied += 1
        if 'storTime' in sm:
            self.mStorTimeNo += 1
            self.mStorTime += sm['storTime']
            if self.mStorTimeMax < sm['storTime']:
                self.mStorTimeMax = sm['storTime']

        else:
            curTime = time.time()
            sm['storTime'] = curTime - sm['receiveTime']
            self.mStorTimeNo += 1
            self.mStorTime += sm['storTime']
        if self.mStorTimeMax < sm['storTime']:
            self.mStorTimeMax = sm['storTime']

    def addToDeplProcMessages(self, sm):
        if (sm is not None):
            self.depletedProcMessages += 1
            self.depletedProcMessagesSize += sm['msg_size']
            if (sm['overtime']):
                self.mOvertime += 1
                self.mUnSatisfied += 1
            if (sm["service_type"] == "unprocessed"):
                self.mUnProcessed += 1
                self.depletedUnProcMessages += 1
                self.depletedUnProcMessagesSize += sm['msg_size']

                if (sm['storTime'] is not None):
                    self.mStorTimeNo += 1
                    self.mStorTime += sm['storTime']
                if (self.mStorTimeMax < sm['storTime']):
                    self.mStorTimeMax = sm['storTime']

            else:
                curTime = time.time()
                sm['storTime'] = curTime - sm['receiveTime']
                self.mStorTimeNo += 1
                self.mStorTime += sm['storTime']
                if (self.mStorTimeMax < sm['storTime']):
                    self.mStorTimeMax = sm['storTime']

    def addToCloudDeplMessages(self, sm):
        if (sm is not None):
            self.depletedCloudMessages += 1
            self.depletedCloudMessagesSize += sm['msg_size']
            if (sm['overtime']):
                self.mOvertime += 1
            if (sm["service_type"] == "unprocessed"):
                self.mUnProcessed += 1
                self.depletedUnProcMessages += 1
                self.depletedUnProcMessagesSize += sm['msg_size']

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
                sm['storTime'] = curTime - sm['receiveTime']
                self.mStorTimeNo += 1
                self.mStorTime += sm['storTime']
            if (self.mStorTimeMax < sm['storTime']):
                self.mStorTimeMax = sm['storTime']
            self.deleteAnyMessage(sm['content'])

    def addToDeplUnProcMessages(self, sm):
        sm.update("type", "unprocessed")
        self.self.model.repoStorage[self.node].addToStoredMessages(sm)

    def addToDepletedUnProcMessages(self, sm):
        if (sm is not None):
            if (sm["service_type"] == "unprocessed"):
                self.depletedUnProcMessages += 1
            self.depletedUnProcMessagesSize += sm['msg_size']
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
            retrieval = m['receiveTime']
            if m['shelf_life'] is not None and m['shelf_life'] <= curTime - retrieval:
                size += m['msg_size']
        return size

    def getProcMessagesSize(self):
        return self.processSize

    def getProcessedMessagesSize(self):
        processedUsed = 0
        for msg in self.processedMessages:
            processedUsed += msg['msg_size']
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
        for i in range(0, len(self.processedMessages)):
            if MessageId is not None and self.processedMessages[i]['content'] == MessageId:
                answer = self.processedMessages[i]
            elif labels:
                j_labels = []
                for label in self.processedMessages[i]['labels']:
                    if label in labels:
                        j_labels.append(label)
                if (j_labels == labels):
                    answer = self.processedMessages[i]
        for i in range(0, len(self.Messages)):
            if MessageId is not None and self.Messages[i]['content'] == MessageId:
                answer = self.Messages[i]
            elif labels:
                j_labels = []
                for label in self.Messages[i]['labels']:
                    if label in labels:
                        j_labels.append(label)
                if (j_labels == labels):
                    answer = self.Messages[i]
        for i in range(0, len(self.processMessages)):
            if MessageId is not None and self.processMessages[i]['content'] == MessageId:
                answer = self.processMessages[i]
            elif labels:
                j_labels = []
                for label in self.processMessages[i]['labels']:
                    if label in labels:
                        j_labels.append(label)
                if (j_labels == labels):
                    answer = self.processMessages[i]
        return answer

    def getProcessedMessages(self, labels):
        answer = None
        for i in range(0, len(self.processedMessages)):
            j_labels = []
            for label in self.processedMessages[i]['labels']:
                if label in labels:
                    j_labels.append(label)
            if (j_labels == labels):
                answer = self.processedMessages[i]
        for i in range(0, len(self.Messages)):
            j_labels = []
            for label in self.Messages[i]['labels']:
                if label in labels:
                    j_labels.append(label)
            if (j_labels == labels):
                answer = self.Messages[i]
        for i in range(0, len(self.processMessages)):
            j_labels = []
            for label in self.processMessages[i]['labels']:
                if label in labels:
                    j_labels.append(label)
            if (j_labels == labels):
                answer = self.processMessages[i]
        return answer

    def deleteMessage(self, MessageId):
        for i in range(0, len(self.Messages)-1):
            if (self.Messages[i]["content"] == MessageId):
                self.Size -= self.Messages[i]['msg_size']
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
        for i in (0, len(self.processMessages)-1):
            if (self.processMessages[i]['content'] == MessageId):
                self.processSize -= self.processMessages[i]['msg_size']
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

    def deleteAnyMessage(self, MessageId):
        m = self.hasMessage(MessageId, [])
        if m is not None:
            if m["service_type"].lower() == "proc" and self.deleteProcMessage(MessageId):
                return True

            elif m["service_type"].lower() == "nonproc" and self.deleteMessage(MessageId):
                return True

            elif m["service_type"].lower() == "unprocessed" and self.deleteMessage(MessageId):
                return True

            if not self.model.repoStorage[self.node]:
                self.nrofDeletedMessages += 1
                self.deletedMessagesSize += m['msg_size']

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
        for i in range(0, len(self.processedMessages)-1):
            if self.processedMessages[i]['content'] == MessageId:
                self.depletedCloudProcMessages += 1
                self.depletedCloudProcMessagesSize += self.processedMessages[i]['msg_size']
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

            elif (oldest['receiveTime'] > m['receiveTime']):
                oldest = m

        return oldest

    @property
    def getOldestValidProcessMessage(self):
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                if (m["service_type"] is not None and m['Fresh'] is None):
                    if not (m["service_type"]).lower() == "unprocessed" and not (m["service_type"]).lower() == "processed":
                        oldest = m

                elif oldest['receiveTime'] > m['receiveTime'] and m["service_type"] is not None and m['Fresh'] is None:
                    if not m["service_type"].lower() == "unprocessed" and not (m["service_type"]).lower() == "processed":
                        oldest = m

        return oldest

    @property
    def getOldestInvalidProcessMessage(self):
        curTime = time.time()
        oldest = None
        for m in self.processMessages:
            if (oldest is None):
                if (m["service_type"] is not None and m['shelf_life'] is not None):
                    if ((m["service_type"]).lower() == "proc" and (
                            m['shelf_life']) <= curTime - m['receiveTime']):
                        oldest = m

                elif (oldest['receiveTime'] > m['receiveTime'] and m["service_type"] is not None and m.getProperty(
                        "shelfLife") is not None):
                    if ((m["service_type"]).lower() == "proc" and (
                            m['shelf_life']) <= curTime - m['receiveTime']):
                        oldest = m

        return oldest

    @property
    def getOldestDeplUnProcMessage(self):
        oldest = None
        for m in self.Messages:
            if (oldest is None):
                if (m["service_type"] is not None):
                    if ((m["service_type"]).lower() == "unprocessed"):
                        oldest = m
                elif (oldest['receiveTime'] > m['receiveTime'] and m["service_type"] is not None):
                    if ((m["service_type"]).lower() == "unprocessed"):
                        oldest = m

        return oldest

    @property
    def getNewestProcessMessage(self):
        newest = None
        for m in self.processMessages:
            if (newest is None):
                if (m["service_type"] is not None and m['Fresh'] is None):
                    if ((m["service_type"]).lower() == "proc"):
                        newest = m
                elif (newest['receiveTime'] < m['receiveTime'] and m["service_type"] is not None and m[
                    'Fresh'] is None):
                    if ((m["service_type"]).lower() == "proc"):
                        newest = m

        return newest

    @property
    def getOldestProcessedMessage(self):
        oldest = None
        for m in self.processedMessages:
            if (oldest is None):
                oldest = m

            elif (oldest['receiveTime'] > m['receiveTime']):
                oldest = m

        return oldest

    @property
    def getOldestFreshMessage(self):

        oldest = None
        for m in self.processedMessages:
            if (oldest is None):
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if ((m['Fresh']) and (m["service_type"]).lower() == "processed"):
                        oldest = m
                elif (oldest['receiveTime'] > m['receiveTime'] and m['Fresh'] is not None and m[
                    "service_type"] is not None):
                    if ((m['Fresh']) and (m["service_type"]).lower() == "processed"):
                        oldest = m

        return oldest

    @property
    def GetNewestFreshMessage(self):

        newest = None
        for m in self.processedMessages:
            if newest is None:
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if ((m['Fresh']) and (m["service_type"]).lower() == "processed"):
                        newest = m
                elif (newest['receiveTime'] < m['receiveTime'] and m['Fresh'] is not None and m[
                    "service_type"] is not None):
                    if ((m['Fresh']) and (m["service_type"]).lower() == "processed"):
                        newest = m

        return newest

    @property
    def getOldestShelfMessage(self):

        oldest = None
        for m in self.processedMessages:
            if (oldest is None):
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if not (m['Fresh']) and (m["service_type"]).lower() == "processed":
                        oldest = m
                elif (oldest['receiveTime'] > m['receiveTime'] and m['Fresh'] is not None and m[
                    "service_type"] is not None):
                    if (not (m['Fresh']) and (m["service_type"]).lower() == "processed"):
                        oldest = m

        return oldest

    @property
    def getNewestShelfMessage(self):

        newest = None
        for m in self.processedMessages:
            if (newest is None):
                if (m['Fresh'] is not None and m["service_type"] is not None):
                    if (not (m['Fresh']) and (m["service_type"]).lower() == "processed"):
                        newest = m
                elif (newest['receiveTime'] < m['receiveTime'] and m['Fresh'] is not None and m[
                    "service_type"] is not None):
                    if (not (m['Fresh'] and (m["service_type"]).lower() == "processed")):
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
                elif (oldest['receiveTime'] > m['receiveTime'] and m['Fresh'] is not None and m.getProperty(
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

                elif newest['receiveTime'] < m['receiveTime'] and m['Fresh'] is not None and m.getProperty(
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

                elif (oldest['receiveTime'] > m['receiveTime'] and m['Fresh'] is not None and m.getProperty(
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

                elif newest['receiveTime'] < m['receiveTime'] and m['Fresh'] is not None and m.getProperty(
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

            elif (oldest['receiveTime'] > m['receiveTime']):
                oldest = m

        return oldest

    @property
    def getOldestStaleMessage(self):
        curTime = time.time()
        oldest = None
        for m in self.Messages:
            if (oldest is None):
                if (m['shelf_life'] is not None):
                    if ((m['shelf_life']) <= curTime - m['receiveTime']):
                        oldest = m

                elif (oldest['receiveTime'] > m['receiveTime'] and m['shelf_life'] is not None):
                    if ((m['shelf_life']) <= curTime - m['receiveTime']):
                        oldest = m

        return oldest

    def getCompressionRate(self):
        return self.compressionRate


# TODO: NEED TO REVIEW AND REVISE ALL OF THE CODE BELOWnot not not not not not not not not not not not
#  \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/




@register_repo_policy('NULL_REPO')
class RepoNull(RepoStorage):
    """Implementation of a null cache.

    This is a dummy cache which never stores anything. It is functionally
    identical to a cache with max size equal to 0.
    """

    def __init__(self, maxlen=0, *args, **kwargs):
        """
        Constructor

        Parameters
        ----------
        maxlen : int, optional
            The max length of the cache. This parameters is always ignored
        """
        pass

    def getTotalStorageSpace(self):
        return 0

    def getTotalProcessedSpace(self):
        return 0

    def getStoredMessagesCollection(self):
        return 0

    def getStoredMessages(self):
        return 0

    def getProcessedMessages(self):
        return 0

    def getProcessMessages(self):
        return 0

    def getMessages(self):
        return 0

    def addToStoredMessages(self, sm):

       return 0

    def addToDeplMessages(self, sm):
       return 0

    def addToDeplProcMessages(self, sm):
       return 0

    def addToCloudDeplMessages(self, sm):
        return 0
    def addToDeplUnProcMessages(self, sm):
        return 0

    def addToDepletedUnProcMessages(self, sm):
        return 0

    def getMessage(self, MessageId):
        return 0

    def getProcessedMessage(self, MessageId):
        return 0

    def getProcessMessage(self, MessageId):
        return 0

    def getStorTimeNo(self):
        return 0

    def getStorTimeAvg(self):
        return 0

    def getStorTimeMax(self):
        return 0

    @property
    def getNrofMessages(self):

        return 0

    def getNrofProcessMessages(self):
        return 0

    def getNrofProcessedMessages(self):
        return 0

    def getMessagesSize(self):
        return 0

    def getStaleMessagesSize(self):
        return 0

    def getProcMessagesSize(self):
        return 0

    def getProcessedMessagesSize(self):
        return 0

    def getFullCachedMessagesNo(self):
        return 0

    def getnode(self):
        return 0

    def hasMessage(self, MessageId, labels):
        return 0

    def getProcessedMessages(self, labels):
        return 0

    def deleteMessage(self, MessageId):
        return 0

    def deleteProcMessage(self, MessageId):
        return 0

    def deleteMessage(self, MessageId):
        return 0

    def deleteProcessedMessage(self, MessageId, report):
        return 0

    def getNrofDeletedMessages(self):
        return 0

    def getSizeofDeletedMessages(self):
        return 0

    def getNrofDepletedCloudProcMessages(self):
        return 0

    def getNrofFreshMessages(self):
        return 0

    def getNrofStaleMessages(self):
        return 0

    def getNrofSatisfiedMessages(self):
        return 0

    def getNrofUnSatisfiedMessages(self):
        return 0

    def getNrofOvertimeMessages(self):
        return 0

    def getNrofUnProcessedMessages(self):
        return 0

    def getNrofDepletedUnProcMessages(self):
        return 0

    def getNrofDepletedPUnProcMessages(self):
        return 0

    def getNrofDepletedMessages(self):
        return 0

    def getNrofDepletedCloudMessages(self):
        return 0

    def getOverallMeanIncomingMesssageNo(self):
        return 0

    def getOverallMeanIncomingSpeed(self):
        return 0

    def getDepletedCloudProcMessagesBW(self, reporting):

        return 0

    def getDepletedUnProcMessagesBW(self, reporting):

        return 0

    def getDepletedPUnProcMessagesBW(self, reporting):

        return 0

    def getDepletedCloudMessagesBW(self, reporting):
        return 0

    def getDepletedProcMessagesBW(self, reporting):
        return 0

    def getDepletedMessagesBW(self, reporting):
        return 0

    def clearAllMessages(self):
        return 0

    def getFreeStorageSpace(self):
        return 0

    def isProcessingEmpty(self):
        return 0

    def isProcessedFull(self):
        return 0

    def isProcessedEmpty(self):
        return 0

    @property
    def getOldestProcessMessage(self):
        return 0

    @property
    def getOldestValidProcessMessage(self):
        return 0
    @property
    def getOldestInvalidProcessMessage(self):
        return 0

    @property
    def getOldestDeplUnProcMessage(self):
        return 0

    @property
    def getNewestProcessMessage(self):
        return 0
    @property
    def getOldestProcessedMessage(self):

        return 0

    @property
    def getOldestFreshMessage(self):

        return 0

    @property
    def GetNewestFreshMessage(self):

        return 0

    @property
    def getOldestShelfMessage(self):

        return 0

    @property
    def getNewestShelfMessage(self):

        return 0

    @property
    def getOldestMessage(self):
        return 0

    @property
    def getOldestStaleMessage(self):
        return 0

    def getCompressionRate(self):
        return 0

    # TODO: NEED TO REVIEW AND REVISE ALL OF THE CODE BELOWnot not not not not not not not not not not not
    #  \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

