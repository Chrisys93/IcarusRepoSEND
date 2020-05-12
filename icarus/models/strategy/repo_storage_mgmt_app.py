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

import networkx as nx
import random
import sys

from math import ceil
from icarus.registry import register_strategy
from icarus.util import inheritdoc, path_links
from .base import Strategy
from icarus.models.service import Task, VM

__all__ = [
    'GenRepoStorApp',
    'HServRepoStorApp',
    'HServProStorApp',
    'HServReStorApp',
    'HServSpecStorApp'
]

# TODO: Implement storage checks WITH DELAYS, when data is not present in local cache!
#   (this is the main reason why storage could be useful, anyway)
#   ALSO CHECK FOR STORAGE IN THE ELIFs WHEN MESSAGES ARE ADDED TO STORAGE!
#   (or not...think about the automatic eviction policy implemented in last - java - implementation)

# TODO: ACTUALLY LOOK FOR LABEL LOCATIONS, DISTRIBUTION AND EFFICIENCY IN PROCESSING (MEASURABLE EFFICIENCY METRIC!)!!!!
#  make sure that SERVICES, CONTENTS AND THEIR LABELS are MATCHED !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# Status codes
REQUEST = 0
RESPONSE = 1
TASK_COMPLETE = 2
# Admission results:
DEADLINE_MISSED = 0
CONGESTION = 1
SUCCESS = 2
CLOUD = 3
NO_INSTANCES = 4

@register_strategy('REPO_STORAGE_APP')
class GenRepoStorApp(Strategy):
    """
    Run in passive mode - don't process messages, but store
    """

    def __init__(self, s, view, controller, **kwargs):
        super(GenRepoStorApp, self).__init__(view, controller)

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
        msg['type']).equalsIgnoreCase("nonproc"):
            curTime = time.time()
            self.view.repoStorage[node].deleteMessage(msg.getId)
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

    @inheritdoc(Strategy)
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
            feedback = True
        else:
            feedback = False

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

                elif not self.storMode and self.view.repoStorage[node].getOldestStaleMessage() is not None:
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
        elif (self.view.repoStorage[node].getProcessedMessagesSize + self.view.repoStorage[node].getStaleMessagesSize)\
                > (self.view.repoStorage[node].getTotalStorageSpace * self.max_stor):
            self.cloudEmptyLoop = True
            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                if (self.view.repoStorage[node].getOldestStaleMessage is not None and
                        self.cloudBW < self.cloud_lim):
                    self.oldestSatisfiedDepletion(self, node)

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
                #   elif (not self.storMod e and self.view.repoStorage[node].getOldestStaleMessage is not None)
                #	    oldestSatisfiedDepletion(node)
                #
                #   elif (self.storMod e and curTime - self.lastCloudUpload >= self.maxStorTime)
                #	    self.storMode = False
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
                self.view.repoStorage[node].deleteMessage(self.view.repoStorage[node].getOldestProcessMessage.getId)
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

                elif (self.view.repoStorage[node].getOldestStaleMessage is not None):
                    self.oldestSatisfiedDepletion(node)


                elif (self.view.repoStorage[node].getOldestInvalidProcessMessage is not None):
                    self.oldestInvalidProcDepletion(node)

                elif (self.view.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.repoStorage[node].getOldestMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp.getId)
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.repoStorage[node].addToDeplMessages(ctemp)
                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    else:
                        self.view.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.repoStorage[node].getNewestProcessMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp.getId)
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.repoStorage[node].addToDeplProcMessages(ctemp)
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
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
            if (self.view.repoStorage[node].getOldestFreshMessage.getProperty("procTime") is None):
                temp = self.view.repoStorage[node].getOldestFreshMessage
                report = False
                self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)
                """
                * Make sure here that the added message to the cloud depletion 
                * tracking is also tracked by whether it 's Fresh or Stale.
                """
                report = True
                self.view.repoStorage[node].addToStoredMessages(temp)
                self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)



            elif (self.view.repoStorage[node].getOldestShelfMessage is not None):
                if (self.view.repoStorage[node].getOldestShelfMessage.getProperty("procTime") is None):
                    temp = self.view.repoStorage[node].getOldestShelfMessage
                    report = False
                    self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)
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
                    self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.repoStorage[node].getOldestStaleMessage
        self.view.repoStorage[node].deleteMessage(ctemp.getId)
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.repoStorage[node].addToCloudDeplMessages(ctemp)

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
        temp = self.view.repoStorage[node].getOldestInvalidProcessMessage
        self.view.repoStorage[node].deleteMessage(temp.getId)
        if (temp['comp'] is not None):
            if (temp['comp']):
                ctemp = self.compressMessage(node, temp)
                self.view.repoStorage[node].deleteMessage(ctemp.getId)
                storTime = curTime - ctemp['receiveTime']
                ctemp['storTime'] = storTime
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                self.view.repoStorage[node].addToDeplProcMessages(ctemp)

            elif (temp['comp'] is None):
                storTime = curTime - temp['receiveTime']
                temp['storTime'] = storTime
                temp['satisfied'] = False
                if (storTime <= temp['shelfLife'] + 1):
                    temp['overtime'] = False

                elif (storTime > temp['shelfLife'] + 1):
                    temp['overtime'] = True

                self.view.repoStorage[node].addToDeplProcMessages(temp)

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.repoStorage[node].getOldestDeplUnProcMessage
        self.view.repoStorage[node].deleteMessage(temp.getId)
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

    def getProcEndTimes(self):
        return self.procEndTimes

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




@register_strategy('HYBRIDS_REPO_APP')
class HServRepoStorApp(Strategy):
    """
    Run in passive mode - don't process messages, but store
    """

    def __init__(self, s, view, controller, replacement_interval=10, debug=False, n_replacements=1, **kwargs):
        super(HServRepoStorApp, self).__init__(view, controller)

        self.replacement_interval = replacement_interval
        self.n_replacements = n_replacements
        self.last_replacement = 0
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        # metric to rank each VM of Comp. Spot
        self.deadline_metric = {x: {} for x in range(0, self.num_nodes)}
        self.cand_deadline_metric = {x: {} for x in range(0, self.num_nodes)}
        self.replacements_so_far = 0
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0



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

    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            cs.running_requests = [0 for x in range(0, self.num_services)]
            cs.missed_requests = [0 for x in range(0, self.num_services)]
            cs.scheduler.idleTime = 0.0
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0

    # HYBRID
    def replace_services1(self, time):
        for node, cs in self.compSpots.items():
            n_replacements = 0
            if cs.is_cloud:
                continue
            runningServiceResidualTimes = self.deadline_metric[node]
            missedServiceResidualTimes = self.cand_deadline_metric[node]
            # service_residuals = []
            running_services_utilisation_normalised = []
            missed_services_utilisation = []
            delay = {}
            # runningServicesUtil = {}
            # missedServicesUtil = {}

            if len(cs.scheduler.upcomingTaskQueue) > 0:
                print("Printing upcoming task queue at node: " + str(cs.node))
                for task in cs.scheduler.upcomingTaskQueue:
                    task.print_task()

            util = []
            util_normalised = {}

            if self.debug:
                print("Replacement at node " + repr(node))
            for service in range(0, self.num_services):
                if cs.numberOfVMInstances[service] != len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                    print("Error: number of vm instances do not match for service: " + str(service) + " node: " + str(
                        cs.node))
                    print("numberOfInstances = " + str(cs.numberOfVMInstances[service]))
                    print("Total VMs: " + str(
                        len(cs.scheduler.idleVMs[service]) + len(cs.scheduler.busyVMs[service]) + len(
                            cs.scheduler.startingVMs[service])))
                    print("\t Idle: " + str(len(cs.scheduler.idleVMs[service])) + " Busy: " + str(
                        len(cs.scheduler.busyVMs[service])) + " Starting: " + str(
                        len(cs.scheduler.startingVMs[service])))
                d_metric = 0.0
                u_metric = 0.0
                util.append([service, (cs.missed_requests[service] + cs.running_requests[service]) * cs.services[
                    service].service_time])
                if cs.numberOfVMInstances[service] == 0:
                    # No instances
                    if cs.missed_requests[service] > 0:
                        d_metric = 1.0 * missedServiceResidualTimes[service] / cs.missed_requests[service]
                    else:
                        d_metric = float('inf')
                    delay[service] = d_metric
                    u_metric = cs.missed_requests[service] * cs.services[service].service_time
                    if u_metric > self.replacement_interval:
                        u_metric = self.replacement_interval
                    # missedServiceResidualTimes[service] = d_metric
                    # missedServicesUtil[service] = u_metric
                    missed_services_utilisation.append([service, u_metric])
                    # service_residuals.append([service, d_metric])
                elif cs.numberOfVMInstances[service] > 0:
                    # At least one instance
                    if cs.running_requests[service] > 0:
                        d_metric = 1.0 * runningServiceResidualTimes[service] / cs.running_requests[service]
                    else:
                        d_metric = float('inf')
                    runningServiceResidualTimes[service] = d_metric
                    u_metric_missed = (cs.missed_requests[service]) * cs.services[service].service_time * 1.0
                    if u_metric_missed > self.replacement_interval:
                        u_metric_missed = self.replacement_interval
                    # missedServicesUtil[service] = u_metric_missed
                    u_metric_served = (1.0 * cs.running_requests[service] * cs.services[service].service_time) / \
                                      cs.numberOfVMInstances[service]
                    # runningServicesUtil[service] = u_metric_served
                    missed_services_utilisation.append([service, u_metric_missed])
                    # running_services_latency.append([service, d_metric])
                    running_services_utilisation_normalised.append(
                        [service, u_metric_served / cs.numberOfVMInstances[service]])
                    # service_residuals.append([service, d_metric])
                    delay[service] = d_metric
                else:
                    print("This should not happen")
            running_services_utilisation_normalised = sorted(running_services_utilisation_normalised,
                                                             key=lambda x: x[1])  # smaller to larger
            missed_services_utilisation = sorted(missed_services_utilisation, key=lambda x: x[1],
                                                 reverse=True)  # larger to smaller
            # service_residuals = sorted(service_residuals, key=lambda x: x[1]) #smaller to larger
            exit_loop = False
            for service_missed, missed_util in missed_services_utilisation:
                if exit_loop:
                    break
                for indx in range(len(running_services_utilisation_normalised)):
                    service_running = running_services_utilisation_normalised[indx][0]
                    running_util = running_services_utilisation_normalised[indx][1]
                    if running_util > missed_util:
                        exit_loop = True
                        break
                    if service_running == service_missed:
                        continue
                    if missed_util >= running_util and delay[service_missed] < delay[service_running] and delay[
                        service_missed] > 0:
                        self.controller.reassign_vm(time, cs, service_running, service_missed, self.debug)
                        # cs.reassign_vm(self.controller, time, service_running, service_missed, self.debug)
                        if self.debug:
                            print("Missed util: " + str(missed_util) + " running util: " + str(
                                running_util) + " Adequate time missed: " + str(
                                delay[service_missed]) + " Adequate time running: " + str(delay[service_running]))
                        del running_services_utilisation_normalised[indx]
                        n_replacements += 1
                        break
            if self.debug:
                print(str(n_replacements) + " replacements at node:" + str(cs.node) + " at time: " + str(time))
                for node in self.compSpots.keys():
                    cs = self.compSpots[node]
                    if cs.is_cloud:
                        continue
                    if cs.node != 14 and cs.node != 6:
                        continue
                    for service in range(0, self.num_services):
                        if cs.numberOfVMInstances[service] > 0:
                            print("Node: " + str(node) + " has " + str(
                                cs.numberOfVMInstances[service]) + " instance of " + str(service))

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

    def handle(self, curTime, msg, node, log, feedback, flow_id, rtt_delay):
        msg['receiveTime'] = time.time()
        if self.view.hasStorageCapability(node):
            if node in self.view.labels_sources(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_storage_labels_to_node(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, False)
            elif node is self.view.all_labels_main_source(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_storage_labels_to_node(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, False)
            else:
                edr = self.view.all_labels_main_source(msg["labels"])
                self.controller.add_request_labels_to_node(node, msg)
                self.controller.start_session(curTime, node, msg, log, feedback, flow_id, msg['shelf_life'])
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                          msg['freshness_per'], rtt_delay, REQUEST)

        elif not self.view.hasStorageCapability(node) and msg['service_type'] is "nonproc":
            curTime = time.time()
            self.view.repoStorage[node].deleteMessage(msg.getId)
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

    @inheritdoc(Strategy)
    def process_event(self, curTime, receiver, content, log, labels, node, flow_id, deadline, rtt_delay, status, task=None):
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

    TODO: HAVE TO IMPLEMENT STORAGE LOOKUP STAGE, AFTER CACHE LOOKUP, AND INCLUDE IT IN THE PROCESS, BEFORE
        CONSIDERING THAT THE DATA IS NOT AVAILABLE IN CURRENT NODE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        """

        if self.view.hasStorageCapability(node):
            feedback = True
        else:
            feedback = False

        if self.view.hasStorageCapability(node):

            self.updateCloudBW(node)
            self.deplCloud(node)
            self.updateDeplBW(node)
            self.deplStorage(node)

        elif not self.view.hasStorageCapability(node) and self.view.has_computationalSpot(node):
            self.updateUpBW(node)
            self.deplUp(node)

        """
                response : True, if this is a response from the cloudlet/cloud
                deadline : deadline for the request 
                flow_id : Id of the flow that the request/response is part of
                node : the current node at which the request/response arrived

                TODO: Maybe could even implement the old "application" storage
                    space and message services management in here, as well!!!!

                """
        # self.debug = False
        # if node == 12:
        #    self.debug = True
        service = None
        if content["service_type"] is "proc":
            service = content if content is not '' else self.view.all_labels_main_source(labels)['content']
        self.handle(curTime, content, node, log, feedback, flow_id, rtt_delay)
        
        source = self.view.content_source(content)

        if curTime - self.last_replacement > self.replacement_interval:
            # self.print_stats()
            print("Replacement time: " + repr(curTime))
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, curTime)
            self.replace_services1(curTime)
            self.last_replacement = curTime
            self.initialise_metrics()

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        if service is not None:
            if source == node and status == REQUEST:
                self.controller.add_request_labels_to_node(receiver, service)
                ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver, rtt_delay,
                                                  self.controller, self.debug)

                if ret == False:
                    print("This should not happen in Hybrid.")
                    raise ValueError("Task should not be rejected at the cloud.")
                return

            #  Request at the receiver
            if receiver == node and status == REQUEST:
                self.controller.add_request_labels_to_node(receiver, service)
                self.controller.start_session(curTime, receiver, service, log, feedback, feedback, flow_id, deadline)
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                rtt_delay += delay * 2
                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, REQUEST)
                return

            if self.debug:
                print("\nEvent\n time: " + repr(curTime) + " receiver  " + repr(receiver) + " service " + repr(
                    service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(
                    deadline) + " status " + repr(status))

            if status == RESPONSE:
                # response is on its way back to the receiver
                if node == receiver:
                    self.controller.end_session(True, curTime, flow_id)  # TODO add flow_time
                    return
                else:
                    path = self.view.shortest_path(node, receiver)
                    next_node = path[1]
                    delay = self.view.link_delay(node, next_node)
                    path_del = self.view.path_delay(node, receiver)
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, RESPONSE)
                    if path_del + curTime > deadline:
                        compSpot.missed_requests[service['content']] += 1

            elif status == TASK_COMPLETE:
                self.controller.complete_task(task, curTime)
                if service['freshness_per'] > curTime - service['receiveTime']:
                    service['Fresh'] = True
                    service['Shelf'] = True
                elif service['shelf_life'] > curTime - service['receiveTime']:
                    service['Fresh'] = False
                    service['Shelf'] = True
                else:
                    service['Fresh'] = False
                    service['Shelf'] = False
                if node != source:
                    newTask = compSpot.scheduler.schedule(curTime)
                    # schedule the next queued task at this node
                    if newTask is not None:
                        self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.service,
                                                  newTask.labels, node, newTask.flow_id,
                                                  newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask)

                # forward the completed task
                if task.taskType == Task.TASK_TYPE_VM_START:
                    return
                path = self.view.shortest_path(node, receiver)
                path_delay = self.view.path_delay(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.view.hasStorageCapability(node):
                    service['service_type'] = None
                    service['service_type'] = "processed"
                    if all(elem in labels for elem in self.view.model.node_labels(node)["request_labels"]):
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_request_labels_to_storage(node, service)
                    else:
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_storage_labels_to_node(node, service)

                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, RESPONSE)
                if (node != source and curTime + path_delay > deadline):
                    print("Error in HYBRID strategy: Request missed its deadline\nResponse at receiver at time: " + str(
                        curTime + path_delay) + " deadline: " + str(deadline))
                    task.print_task()
                    # raise ValueError("This should not happen: a task missed its deadline after being executed at an edge node.")

            elif status == REQUEST:
                self.controller.add_request_labels_to_node(receiver, service)
                # Processing a request
                source, cache_ret = self.view.closest_source(node, service)

                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                cache_delay = 0
                if node == source:
                    if not cache_ret and self.view.has_cache(node):
                        if self.controller.put_content(source, content):
                            cache_delay = 0.01
                        else:
                            self.controller.put_content_local_cache(source, content)
                            cache_delay = 0.01
                else:
                    if not cache_ret and self.view.has_cache(source):
                        if self.controller.put_content(source, content):
                            pass
                        else:
                            self.controller.put_content_local_cache(source, content)
                deadline_metric = (
                            deadline - curTime - rtt_delay - cache_delay - compSpot.services[service['content']].service_time)  # /deadline
                if self.debug:
                    print("Deadline metric: " + repr(deadline_metric))
                if self.view.has_service(node, service) and service["service_type"] is "proc":
                    if self.debug:
                        print("Calling admit_task")
                    ret, reason = compSpot.admit_task(service['content'], curTime, flow_id, deadline, receiver, rtt_delay,
                                                      self.controller, self.debug)
                    if self.debug:
                        print("Done Calling admit_task")
                    if ret is False:
                        # Pass the Request upstream
                        rtt_delay += delay * 2
                        # delay += 0.01 # delay associated with content fetch from storage
                        # WE DON'T INCLUDE THIS BECAUSE THE CONTENT MAY BE PRE-FETCHED INTO THE CACHE FROM STORAGE
                        self.controller.add_event(curTime + delay, receiver, service, labels,  next_node,
                                                  flow_id, deadline, rtt_delay, REQUEST)
                        if deadline_metric > 0:
                            self.cand_deadline_metric[node][service['content']] += deadline_metric
                        if self.debug:
                            print("Pass upstream to node: " + repr(next_node))
                        # compSpot.missed_requests[service] += 1 # Added here
                    else:
                        if deadline_metric > 0:
                            self.deadline_metric[node][service['content']] += deadline_metric
                else:  # Not running the service
                    # compSpot.missed_requests[service] += 1
                    if self.debug:
                        print("Not running the service: Pass upstream to node: " + repr(next_node))
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, REQUEST)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service['content']] += deadline_metric
            else:
                print("Error: unrecognised status value : " + repr(status))

    # TODO: UPDATE BELOW WITH COLLECTORS INSTEAD OF PREVIOUS OUTPUT FILES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

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

                elif not self.storMode and self.view.repoStorage[node].getOldestStaleMessage() is not None:
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

                if (self.view.repoStorage[node].getOldestStaleMessage is not None and
                        self.cloudBW < self.cloud_lim):
                    self.oldestSatisfiedDepletion(self, node)

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
                #				elif (not self.storMod e and self.view.repoStorage[node].getOldestStaleMessage is not None)
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
                self.view.repoStorage[node].deleteMessage(self.view.repoStorage[node].getOldestProcessMessage.getId)
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

                elif (self.view.repoStorage[node].getOldestStaleMessage is not None):
                    self.oldestSatisfiedDepletion(node)


                elif (self.view.repoStorage[node].getOldestInvalidProcessMessage is not None):
                    self.oldestInvalidProcDepletion(node)

                elif (self.view.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.repoStorage[node].getOldestMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp.getId)
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.repoStorage[node].addToDeplMessages(ctemp)
                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    else:
                        self.view.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.repoStorage[node].getNewestProcessMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp.getId)
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.repoStorage[node].addToDeplProcMessages(ctemp)
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
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
            if (self.view.repoStorage[node].getOldestFreshMessage.getProperty("procTime") is None):
                temp = self.view.repoStorage[node].getOldestFreshMessage
                report = False
                self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)
                """
                * Make sure here that the added message to the cloud depletion 
                * tracking is also tracked by whether it 's Fresh or Stale.
                """
                report = True
                self.view.repoStorage[node].addToStoredMessages(temp)
                self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)



            elif (self.view.repoStorage[node].getOldestShelfMessage is not None):
                if (self.view.repoStorage[node].getOldestShelfMessage.getProperty("procTime") is None):
                    temp = self.view.repoStorage[node].getOldestShelfMessage
                    report = False
                    self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)
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
                    self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.repoStorage[node].getOldestStaleMessage
        self.view.repoStorage[node].deleteMessage(ctemp.getId)
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.repoStorage[node].addToCloudDeplMessages(ctemp)

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
        temp = self.view.repoStorage[node].getOldestInvalidProcessMessage
        self.view.repoStorage[node].deleteMessage(temp.getId)
        if (temp['comp'] is not None):
            if (temp['comp']):
                ctemp = self.compressMessage(node, temp)
                self.view.repoStorage[node].deleteMessage(ctemp.getId)
                storTime = curTime - ctemp['receiveTime']
                ctemp['storTime'] = storTime
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                self.view.repoStorage[node].addToDeplProcMessages(ctemp)

            elif (temp['comp'] is None):
                storTime = curTime - temp['receiveTime']
                temp['storTime'] = storTime
                temp['satisfied'] = False
                if (storTime <= temp['shelfLife'] + 1):
                    temp['overtime'] = False

                elif (storTime > temp['shelfLife'] + 1):
                    temp['overtime'] = True

                self.view.repoStorage[node].addToDeplProcMessages(temp)

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.repoStorage[node].getOldestDeplUnProcMessage
        self.view.repoStorage[node].deleteMessage(temp.getId)
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

    def getProcEndTimes(self):
        return self.procEndTimes

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




@register_strategy('HYBRIDS_PRO_REPO_APP')
class HServProStorApp(Strategy):
    """
    Run in passive mode - don't process messages, but store
    """

    def __init__(self, s, view, controller, replacement_interval=10, debug=False, n_replacements=1, **kwargs):
        super(HServProStorApp, self).__init__(view, controller)

        self.replacement_interval = replacement_interval
        self.n_replacements = n_replacements
        self.last_replacement = 0
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        # metric to rank each VM of Comp. Spot
        self.deadline_metric = {x: {} for x in range(0, self.num_nodes)}
        self.cand_deadline_metric = {x: {} for x in range(0, self.num_nodes)}
        self.replacements_so_far = 0
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0



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

    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            cs.running_requests = [0 for x in range(0, self.num_services)]
            cs.missed_requests = [0 for x in range(0, self.num_services)]
            cs.scheduler.idleTime = 0.0
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0

    # HYBRID
    def replace_services1(self, time):
        for node, cs in self.compSpots.items():
            n_replacements = 0
            if cs.is_cloud:
                continue
            runningServiceResidualTimes = self.deadline_metric[node]
            missedServiceResidualTimes = self.cand_deadline_metric[node]
            # service_residuals = []
            running_services_utilisation_normalised = []
            missed_services_utilisation = []
            delay = {}
            # runningServicesUtil = {}
            # missedServicesUtil = {}

            if len(cs.scheduler.upcomingTaskQueue) > 0:
                print("Printing upcoming task queue at node: " + str(cs.node))
                for task in cs.scheduler.upcomingTaskQueue:
                    task.print_task()

            util = []
            util_normalised = {}

            if self.debug:
                print("Replacement at node " + repr(node))
            for service in range(0, self.num_services):
                if cs.numberOfVMInstances[service] != len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                    print("Error: number of vm instances do not match for service: " + str(service) + " node: " + str(
                        cs.node))
                    print("numberOfInstances = " + str(cs.numberOfVMInstances[service]))
                    print("Total VMs: " + str(
                        len(cs.scheduler.idleVMs[service]) + len(cs.scheduler.busyVMs[service]) + len(
                            cs.scheduler.startingVMs[service])))
                    print("\t Idle: " + str(len(cs.scheduler.idleVMs[service])) + " Busy: " + str(
                        len(cs.scheduler.busyVMs[service])) + " Starting: " + str(
                        len(cs.scheduler.startingVMs[service])))
                d_metric = 0.0
                u_metric = 0.0
                util.append([service, (cs.missed_requests[service] + cs.running_requests[service]) * cs.services[
                    service].service_time])
                if cs.numberOfVMInstances[service] == 0:
                    # No instances
                    if cs.missed_requests[service] > 0:
                        d_metric = 1.0 * missedServiceResidualTimes[service] / cs.missed_requests[service]
                    else:
                        d_metric = float('inf')
                    delay[service] = d_metric
                    u_metric = cs.missed_requests[service] * cs.services[service].service_time
                    if u_metric > self.replacement_interval:
                        u_metric = self.replacement_interval
                    # missedServiceResidualTimes[service] = d_metric
                    # missedServicesUtil[service] = u_metric
                    missed_services_utilisation.append([service, u_metric])
                    # service_residuals.append([service, d_metric])
                elif cs.numberOfVMInstances[service] > 0:
                    # At least one instance
                    if cs.running_requests[service] > 0:
                        d_metric = 1.0 * runningServiceResidualTimes[service] / cs.running_requests[service]
                    else:
                        d_metric = float('inf')
                    runningServiceResidualTimes[service] = d_metric
                    u_metric_missed = (cs.missed_requests[service]) * cs.services[service].service_time * 1.0
                    if u_metric_missed > self.replacement_interval:
                        u_metric_missed = self.replacement_interval
                    # missedServicesUtil[service] = u_metric_missed
                    u_metric_served = (1.0 * cs.running_requests[service] * cs.services[service].service_time) / \
                                      cs.numberOfVMInstances[service]
                    # runningServicesUtil[service] = u_metric_served
                    missed_services_utilisation.append([service, u_metric_missed])
                    # running_services_latency.append([service, d_metric])
                    running_services_utilisation_normalised.append(
                        [service, u_metric_served / cs.numberOfVMInstances[service]])
                    # service_residuals.append([service, d_metric])
                    delay[service] = d_metric
                else:
                    print("This should not happen")
            running_services_utilisation_normalised = sorted(running_services_utilisation_normalised,
                                                             key=lambda x: x[1])  # smaller to larger
            missed_services_utilisation = sorted(missed_services_utilisation, key=lambda x: x[1],
                                                 reverse=True)  # larger to smaller
            # service_residuals = sorted(service_residuals, key=lambda x: x[1]) #smaller to larger
            exit_loop = False
            for service_missed, missed_util in missed_services_utilisation:
                if exit_loop:
                    break
                for indx in range(len(running_services_utilisation_normalised)):
                    service_running = running_services_utilisation_normalised[indx][0]
                    running_util = running_services_utilisation_normalised[indx][1]
                    if running_util > missed_util:
                        exit_loop = True
                        break
                    if service_running == service_missed:
                        continue
                    if missed_util >= running_util and delay[service_missed] < delay[service_running] and delay[
                        service_missed] > 0:
                        self.controller.reassign_vm(time, cs, service_running, service_missed, self.debug)
                        # cs.reassign_vm(self.controller, time, service_running, service_missed, self.debug)
                        if self.debug:
                            print("Missed util: " + str(missed_util) + " running util: " + str(
                                running_util) + " Adequate time missed: " + str(
                                delay[service_missed]) + " Adequate time running: " + str(delay[service_running]))
                        del running_services_utilisation_normalised[indx]
                        n_replacements += 1
                        break
            if self.debug:
                print(str(n_replacements) + " replacements at node:" + str(cs.node) + " at time: " + str(time))
                for node in self.compSpots.keys():
                    cs = self.compSpots[node]
                    if cs.is_cloud:
                        continue
                    if cs.node != 14 and cs.node != 6:
                        continue
                    for service in range(0, self.num_services):
                        if cs.numberOfVMInstances[service] > 0:
                            print("Node: " + str(node) + " has " + str(
                                cs.numberOfVMInstances[service]) + " instance of " + str(service))

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

    # TODO: ADAPT FOR POPULARITY PLACEMENT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def handle(self, curTime, msg, node, log, feedback, flow_id, rtt_delay):
        msg['receiveTime'] = time.time()
        if self.view.hasStorageCapability(node):
            if node is self.view.all_labels_most_requests(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, True)
            elif node is self.view.all_labels_main_source(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_storage_labels_to_node(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, False)
            elif node in self.view.labels_sources(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_storage_labels_to_node(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, False)
            else:
                edr = self.view.all_labels_main_source(msg["labels"])
                self.controller.add_request_labels_to_node(node, msg)
                self.controller.start_session(curTime, node, msg, log, feedback, flow_id, msg['shelf_life'])
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                          msg['freshness_per'], rtt_delay, REQUEST)



        elif not self.view.hasStorageCapability(node) and msg['service_type'] is "nonproc":
            curTime = time.time()
            self.view.repoStorage[node].deleteMessage(msg.getId)
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

    @inheritdoc(Strategy)
    def process_event(self, curTime, receiver, content, log, labels, node, flow_id, deadline, rtt_delay, status, task=None):
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
            feedback = True
        else:
            feedback = False

        if self.view.hasStorageCapability(node):

            self.updateCloudBW(node)
            self.deplCloud(node)
            self.updateDeplBW(node)
            self.deplStorage(node)

        elif not self.view.hasStorageCapability(node) and self.view.has_computationalSpot(node):
            self.updateUpBW(node)
            self.deplUp(node)

        """
                response : True, if this is a response from the cloudlet/cloud
                deadline : deadline for the request 
                flow_id : Id of the flow that the request/response is part of
                node : the current node at which the request/response arrived

                TODO: Maybe could even implement the old "application" storage
                    space and message services management in here, as well!!!!

                """
        # self.debug = False
        # if node == 12:
        #    self.debug = True
        service = None
        if content["service_type"] is "proc":
            service = content if content['content'] is not '' else self.view.all_labels_main_source(labels)['content']
        self.handle(content, node, log, flow_id, rtt_delay)
        source = self.view.content_source(content)

        if curTime - self.last_replacement > self.replacement_interval:
            # self.print_stats()
            print("Replacement time: " + repr(curTime))
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, curTime)
            self.replace_services1(curTime)
            self.last_replacement = curTime
            self.initialise_metrics()

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        if service is not None:
            #  Request reached the cloud
            if source == node and status == REQUEST:
                self.controller.add_request_labels_to_node(receiver, service)
                ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver, rtt_delay,
                                                  self.controller, self.debug)

                if ret == False:
                    print("This should not happen in Hybrid.")
                    raise ValueError("Task should not be rejected at the cloud.")
                return

                #  Request at the receiver
            if receiver == node and status == REQUEST:
                self.controller.add_request_labels_to_node(receiver, service)
                self.controller.start_session(curTime, receiver, service, log, feedback, flow_id, deadline)
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                rtt_delay += delay * 2
                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, REQUEST)
                return

            if self.debug:
                print("\nEvent\n time: " + repr(curTime) + " receiver  " + repr(receiver) + " service " + repr(
                    service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(
                    deadline) + " status " + repr(status))

            if status == RESPONSE:
                # response is on its way back to the receiver
                if node == receiver:
                    self.controller.end_session(True, curTime, flow_id)  # TODO add flow_time
                    return
                else:
                    path = self.view.shortest_path(node, receiver)
                    next_node = path[1]
                    delay = self.view.link_delay(node, next_node)
                    path_del = self.view.path_delay(node, receiver)
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, RESPONSE)
                    if path_del + curTime > deadline:
                        compSpot.missed_requests[service['content']] += 1

            elif status == TASK_COMPLETE:
                self.controller.complete_task(task, curTime)
                if service['freshness_per'] > curTime - service['receiveTime']:
                    service['Fresh'] = True
                    service['Shelf'] = True
                elif service['shelf_life'] > curTime - service['receiveTime']:
                    service['Fresh'] = False
                    service['Shelf'] = True
                else:
                    service['Fresh'] = False
                    service['Shelf'] = False
                if node != source:
                    newTask = compSpot.scheduler.schedule(curTime)
                    # schedule the next queued task at this node
                    if newTask is not None:
                        self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.service,
                                                  node, newTask.flow_id, newTask.expiry, newTask.rtt_delay,
                                                  TASK_COMPLETE, newTask)

                # forward the completed task
                if task.taskType == Task.TASK_TYPE_VM_START:
                    return
                path = self.view.shortest_path(node, receiver)
                path_delay = self.view.path_delay(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.view.hasStorageCapability(node):
                    service['service_type'] = None
                    service['service_type'] = "processed"
                    if all(elem in labels for elem in self.view.model.node_labels(node)["request_labels"]):
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_request_labels_to_storage(node, service)
                    else:
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_storage_labels_to_node(node, service)

                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, RESPONSE)
                if (node != source and curTime + path_delay > deadline):
                    print("Error in HYBRID strategy: Request missed its deadline\nResponse at receiver at time: " + str(
                        curTime + path_delay) + " deadline: " + str(deadline))
                    task.print_task()
                    # raise ValueError("This should not happen: a task missed its deadline after being executed at an edge node.")

            elif status == REQUEST:
                self.controller.add_request_labels_to_node(receiver, service)
                # Processing a request
                source, cache_ret = self.view.closest_source(node, service)

                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                cache_delay = 0
                if node == source:
                    if not cache_ret and self.view.has_cache(node):
                        if self.controller.put_content(source, content):
                            cache_delay = 0.01
                        else:
                            self.controller.put_content_local_cache(source, content)
                            cache_delay = 0.01
                else:
                    if not cache_ret and self.view.has_cache(source):
                        if self.controller.put_content(source, content):
                            pass
                        else:
                            self.controller.put_content_local_cache(source, content)
                deadline_metric = (
                            deadline - curTime - rtt_delay - cache_delay - cache_delay - compSpot.services[service['content']].service_time)  # /deadline
                if self.debug:
                    print("Deadline metric: " + repr(deadline_metric))
                if self.view.has_service(node, service) and service["service_type"] is "proc":
                    if self.debug:
                        print("Calling admit_task")
                    ret, reason = compSpot.admit_task(service['content'], curTime, flow_id, deadline, receiver, rtt_delay,
                                                      self.controller, self.debug)
                    if self.debug:
                        print("Done Calling admit_task")
                    if ret is False:
                        # Pass the Request upstream
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, service,labels,  next_node,
                                                  flow_id, deadline, rtt_delay, REQUEST)
                        if deadline_metric > 0:
                            self.cand_deadline_metric[node][service['content']] += deadline_metric
                        if self.debug:
                            print("Pass upstream to node: " + repr(next_node))
                        # compSpot.missed_requests[service['content']] += 1 # Added here
                    else:
                        if deadline_metric > 0:
                            self.deadline_metric[node][service['content']] += deadline_metric
                else:  # Not running the service
                    # compSpot.missed_requests[service['content']] += 1
                    if self.debug:
                        print("Not running the service: Pass upstream to node: " + repr(next_node))
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, REQUEST)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service['content']] += deadline_metric
            else:
                print("Error: unrecognised status value : " + repr(status))

    # TODO: UPDATE BELOW WITH COLLECTORS INSTEAD OF PREVIOUS OUTPUT FILES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

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

                elif not self.storMode and self.view.repoStorage[node].getOldestStaleMessage() is not None:
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

                if (self.view.repoStorage[node].getOldestStaleMessage is not None and
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
                #				elif (not self.storMod e and self.view.repoStorage[node].getOldestStaleMessage is not None)
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
                self.view.repoStorage[node].deleteMessage(self.view.repoStorage[node].getOldestProcessMessage.getId)
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

                elif (self.view.repoStorage[node].getOldestStaleMessage is not None):
                    self.oldestSatisfiedDepletion(node)


                elif (self.view.repoStorage[node].getOldestInvalidProcessMessage is not None):
                    self.oldestInvalidProcDepletion(node)

                elif (self.view.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.repoStorage[node].getOldestMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp.getId)
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.repoStorage[node].addToDeplMessages(ctemp)
                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    else:
                        self.view.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.repoStorage[node].getNewestProcessMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp.getId)
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.repoStorage[node].addToDeplProcMessages(ctemp)
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
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
            if (self.view.repoStorage[node].getOldestFreshMessage.getProperty("procTime") is None):
                temp = self.view.repoStorage[node].getOldestFreshMessage
                report = False
                self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)
                """
                * Make sure here that the added message to the cloud depletion 
                * tracking is also tracked by whether it 's Fresh or Stale.
                """
                report = True
                self.view.repoStorage[node].addToStoredMessages(temp)
                self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)



            elif (self.view.repoStorage[node].getOldestShelfMessage is not None):
                if (self.view.repoStorage[node].getOldestShelfMessage.getProperty("procTime") is None):
                    temp = self.view.repoStorage[node].getOldestShelfMessage
                    report = False
                    self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)
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
                    self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.repoStorage[node].getOldestStaleMessage
        self.view.repoStorage[node].deleteMessage(ctemp.getId)
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.repoStorage[node].addToCloudDeplMessages(ctemp)

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
        temp = self.view.repoStorage[node].getOldestInvalidProcessMessage
        self.view.repoStorage[node].deleteMessage(temp.getId)
        if (temp['comp'] is not None):
            if (temp['comp']):
                ctemp = self.compressMessage(node, temp)
                self.view.repoStorage[node].deleteMessage(ctemp.getId)
                storTime = curTime - ctemp['receiveTime']
                ctemp['storTime'] = storTime
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                self.view.repoStorage[node].addToDeplProcMessages(ctemp)

            elif (temp['comp'] is None):
                storTime = curTime - temp['receiveTime']
                temp['storTime'] = storTime
                temp['satisfied'] = False
                if (storTime <= temp['shelfLife'] + 1):
                    temp['overtime'] = False

                elif (storTime > temp['shelfLife'] + 1):
                    temp['overtime'] = True

                self.view.repoStorage[node].addToDeplProcMessages(temp)

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.repoStorage[node].getOldestDeplUnProcMessage
        self.view.repoStorage[node].deleteMessage(temp.getId)
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

    def getProcEndTimes(self):
        return self.procEndTimes

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




@register_strategy('HYBRIDS_RE_REPO_APP')
class HServReStorApp(Strategy):
    """
    Run in passive mode - don't process messages, but store
    """

    def __init__(self, s, view, controller, replacement_interval=10, debug=False, n_replacements=1, **kwargs):
        super(HServReStorApp, self).__init__(view, controller)

        self.replacement_interval = replacement_interval
        self.n_replacements = n_replacements
        self.last_replacement = 0
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        # metric to rank each VM of Comp. Spot
        self.deadline_metric = {x: {} for x in range(0, self.num_nodes)}
        self.cand_deadline_metric = {x: {} for x in range(0, self.num_nodes)}
        self.replacements_so_far = 0
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0



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

    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            cs.running_requests = [0 for x in range(0, self.num_services)]
            cs.missed_requests = [0 for x in range(0, self.num_services)]
            cs.scheduler.idleTime = 0.0
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0

    # HYBRID
    def replace_services1(self, time):
        for node, cs in self.compSpots.items():
            n_replacements = 0
            if cs.is_cloud:
                continue
            runningServiceResidualTimes = self.deadline_metric[node]
            missedServiceResidualTimes = self.cand_deadline_metric[node]
            # service_residuals = []
            running_services_utilisation_normalised = []
            missed_services_utilisation = []
            delay = {}
            # runningServicesUtil = {}
            # missedServicesUtil = {}

            if len(cs.scheduler.upcomingTaskQueue) > 0:
                print("Printing upcoming task queue at node: " + str(cs.node))
                for task in cs.scheduler.upcomingTaskQueue:
                    task.print_task()

            util = []
            util_normalised = {}

            if self.debug:
                print("Replacement at node " + repr(node))
            for service in range(0, self.num_services):
                if cs.numberOfVMInstances[service] != len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                    print("Error: number of vm instances do not match for service: " + str(service) + " node: " + str(
                        cs.node))
                    print("numberOfInstances = " + str(cs.numberOfVMInstances[service]))
                    print("Total VMs: " + str(
                        len(cs.scheduler.idleVMs[service]) + len(cs.scheduler.busyVMs[service]) + len(
                            cs.scheduler.startingVMs[service])))
                    print("\t Idle: " + str(len(cs.scheduler.idleVMs[service])) + " Busy: " + str(
                        len(cs.scheduler.busyVMs[service])) + " Starting: " + str(
                        len(cs.scheduler.startingVMs[service])))
                d_metric = 0.0
                u_metric = 0.0
                util.append([service, (cs.missed_requests[service] + cs.running_requests[service]) * cs.services[
                    service].service_time])
                if cs.numberOfVMInstances[service] == 0:
                    # No instances
                    if cs.missed_requests[service] > 0:
                        d_metric = 1.0 * missedServiceResidualTimes[service] / cs.missed_requests[service]
                    else:
                        d_metric = float('inf')
                    delay[service] = d_metric
                    u_metric = cs.missed_requests[service] * cs.services[service].service_time
                    if u_metric > self.replacement_interval:
                        u_metric = self.replacement_interval
                    # missedServiceResidualTimes[service] = d_metric
                    # missedServicesUtil[service] = u_metric
                    missed_services_utilisation.append([service, u_metric])
                    # service_residuals.append([service, d_metric])
                elif cs.numberOfVMInstances[service] > 0:
                    # At least one instance
                    if cs.running_requests[service] > 0:
                        d_metric = 1.0 * runningServiceResidualTimes[service] / cs.running_requests[service]
                    else:
                        d_metric = float('inf')
                    runningServiceResidualTimes[service] = d_metric
                    u_metric_missed = (cs.missed_requests[service]) * cs.services[service].service_time * 1.0
                    if u_metric_missed > self.replacement_interval:
                        u_metric_missed = self.replacement_interval
                    # missedServicesUtil[service] = u_metric_missed
                    u_metric_served = (1.0 * cs.running_requests[service] * cs.services[service].service_time) / \
                                      cs.numberOfVMInstances[service]
                    # runningServicesUtil[service] = u_metric_served
                    missed_services_utilisation.append([service, u_metric_missed])
                    # running_services_latency.append([service, d_metric])
                    running_services_utilisation_normalised.append(
                        [service, u_metric_served / cs.numberOfVMInstances[service]])
                    # service_residuals.append([service, d_metric])
                    delay[service] = d_metric
                else:
                    print("This should not happen")
            running_services_utilisation_normalised = sorted(running_services_utilisation_normalised,
                                                             key=lambda x: x[1])  # smaller to larger
            missed_services_utilisation = sorted(missed_services_utilisation, key=lambda x: x[1],
                                                 reverse=True)  # larger to smaller
            # service_residuals = sorted(service_residuals, key=lambda x: x[1]) #smaller to larger
            exit_loop = False
            for service_missed, missed_util in missed_services_utilisation:
                if exit_loop:
                    break
                for indx in range(len(running_services_utilisation_normalised)):
                    service_running = running_services_utilisation_normalised[indx][0]
                    running_util = running_services_utilisation_normalised[indx][1]
                    if running_util > missed_util:
                        exit_loop = True
                        break
                    if service_running == service_missed:
                        continue
                    if missed_util >= running_util and delay[service_missed] < delay[service_running] and delay[
                        service_missed] > 0:
                        self.controller.reassign_vm(time, cs, service_running, service_missed, self.debug)
                        # cs.reassign_vm(self.controller, time, service_running, service_missed, self.debug)
                        if self.debug:
                            print("Missed util: " + str(missed_util) + " running util: " + str(
                                running_util) + " Adequate time missed: " + str(
                                delay[service_missed]) + " Adequate time running: " + str(delay[service_running]))
                        del running_services_utilisation_normalised[indx]
                        n_replacements += 1
                        break
            if self.debug:
                print(str(n_replacements) + " replacements at node:" + str(cs.node) + " at time: " + str(time))
                for node in self.compSpots.keys():
                    cs = self.compSpots[node]
                    if cs.is_cloud:
                        continue
                    if cs.node != 14 and cs.node != 6:
                        continue
                    for service in range(0, self.num_services):
                        if cs.numberOfVMInstances[service] > 0:
                            print("Node: " + str(node) + " has " + str(
                                cs.numberOfVMInstances[service]) + " instance of " + str(service))

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

    # TODO: ADAPT FOR SERVICE-PROXIMATE PLACEMENT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def handle(self, curTime, msg, node, path, log, feedback, flow_id, rtt_delay):
        """

        curTime:

        msg:

        node:

        path:

        log:

        flow_id:

        rtt_delay:

        :return:
        """
        msg['receiveTime'] = time.time()
        if self.view.hasStorageCapability(node):
            # TODO: Check usages of IS OPERATOR throughout code!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            on_path = True
            node_c, in_path = self.view.service_labels_closest_repo(msg['labels'], node, path, on_path)
            on_path = False
            node_s, off_path = self.view.service_labels_closest_repo(msg['labels'], node, path, on_path)
            if node == node_c and in_path is True:
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_storage_labels_to_node(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, False)
            elif node is self.view.all_labels_most_requests(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, True)
            elif node == node_s and not off_path:
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_storage_labels_to_node(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, False)
            elif in_path:
                edr = node_c
                self.controller.add_request_labels_to_node(node, msg)
                self.controller.start_session(curTime, node, msg, log, feedback, feedback, flow_id, msg['freshness_per'])
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                          msg['freshness_per'], rtt_delay, REQUEST)
            elif node_s and not off_path:
                edr = node_s
                self.controller.add_request_labels_to_node(node, msg)
                self.controller.start_session(curTime, node, msg, log, feedback, flow_id, msg['freshness_per'])
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                          msg['freshness_per'], rtt_delay, REQUEST)

            else:
                edr = self.view.all_labels_most_requests(msg["labels"])
                self.controller.add_request_labels_to_node(node, msg)
                self.controller.start_session(curTime, node, msg, log, feedback, flow_id, msg['shelf_life'])
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                          msg['shelf_life'], rtt_delay, REQUEST)

        elif not self.view.hasStorageCapability(node) and msg['service_type'] is "nonproc":
            curTime = time.time()
            self.view.repoStorage[node].deleteMessage(msg.getId)
            storTime = curTime - msg['receiveTime']
            msg['storTime'] = storTime
            if msg['storTime'] < msg['shelfLife']:
                msg['satisfied'] = False
                msg['overtime'] = False
            self.view.repoStorage[node].addToDeplMessages(msg)

        else:
            msg['satisfied'] = False
            msg['overtime'] = False
            self.view.repoStorage[node].addToDeplMessages(msg)

        return msg

    @inheritdoc(Strategy)
    def process_event(self, curTime, receiver, content, log, labels,
                      node, flow_id, deadline, rtt_delay, status, task=None):
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
            feedback = True
        else:
            feedback = False

        if self.view.hasStorageCapability(node):

            self.updateCloudBW(node)
            self.deplCloud(node)
            self.updateDeplBW(node)
            self.deplStorage(node)

        elif not self.view.hasStorageCapability(node) and self.view.has_computationalSpot(node):
            self.updateUpBW(node)
            self.deplUp(node)

        """
                response : True, if this is a response from the cloudlet/cloud
                deadline : deadline for the request 
                flow_id : Id of the flow that the request/response is part of
                node : the current node at which the request/response arrived

                TODO: Maybe could even implement the old "application" storage
                    space and message services management in here, as well!!!!

                """
        # self.debug = False
        # if node == 12:
        #    self.debug = True
        path = self.view.shortest_path(receiver, node)
        service = None
        if content["service_type"] is "proc":
            service = content if content['content'] is not '' else self.view.all_labels_main_source(labels)['content']
        self.handle(curTime, content, node, path, log, feedback, flow_id, rtt_delay)
        source = self.view.content_source(content)

        if curTime - self.last_replacement > self.replacement_interval:
            # self.print_stats()
            print("Replacement time: " + repr(curTime))
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, curTime)
            self.replace_services1(curTime)
            self.last_replacement = curTime
            self.initialise_metrics()

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        if service is not None:
            #  Request reached the cloud
            if source == node and status == REQUEST:
                self.controller.add_request_labels_to_node(receiver, service)
                service['receiveTime'] = curTime
                ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver, rtt_delay,
                                                  self.controller, self.debug)

                if not ret:
                    print("This should not happen in Hybrid.")
                    raise ValueError("Task should not be rejected at the cloud.")
                return

                #  Request at the receiver
            if receiver == node and status == REQUEST:
                service['receiveTime'] = curTime
                self.controller.add_request_labels_to_node(receiver, service)
                self.controller.start_session(curTime, receiver, service, log, feedback, flow_id, deadline)
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                rtt_delay += delay * 2
                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, REQUEST)
                return

            if self.debug:
                print("\nEvent\n time: " + repr(curTime) + " receiver  " + repr(receiver) + " service " + repr(
                    service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(
                    deadline) + " status " + repr(status))

            if status == RESPONSE:
                # response is on its way back to the receiver
                if node == receiver:
                    self.controller.end_session(True, curTime, flow_id)  # TODO add flow_time
                    return
                else:
                    path = self.view.shortest_path(node, receiver)
                    next_node = path[1]
                    delay = self.view.link_delay(node, next_node)
                    path_del = self.view.path_delay(node, receiver)
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, RESPONSE)
                    if path_del + curTime > deadline:
                        compSpot.missed_requests[service['content']] += 1

            elif status == TASK_COMPLETE:
                self.controller.complete_task(task, curTime)
                if service['freshness_per'] > curTime - service['receiveTime']:
                    service['Fresh'] = True
                    service['Shelf'] = True
                elif service['shelf_life'] > curTime - service['receiveTime']:
                    service['Fresh'] = False
                    service['Shelf'] = True
                else:
                    service['Fresh'] = False
                    service['Shelf'] = False
                if node != source:
                    newTask = compSpot.scheduler.schedule(curTime)
                    # schedule the next queued task at this node
                    if newTask is not None:
                        self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.service,
                                                  newTask.labels, node, newTask.flow_id,
                                                  newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask)

                # forward the completed task
                if task.taskType == Task.TASK_TYPE_VM_START:
                    return
                path = self.view.shortest_path(node, receiver)
                path_delay = self.view.path_delay(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.view.hasStorageCapability(node):
                    service['service_type'] = None
                    service['service_type'] = "processed"
                    if all(elem in labels for elem in self.view.model.node_labels(node)["request_labels"]):
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_request_labels_to_storage(node, service)
                    else:
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_storage_labels_to_node(node, service)

                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, RESPONSE)
                if node != source and curTime + path_delay > deadline:
                    print("Error in HYBRID strategy: Request missed its deadline\nResponse at receiver at time: " + str(
                        curTime + path_delay) + " deadline: " + str(deadline))
                    task.print_task()
                    # raise ValueError("This should not happen: a task missed its deadline after being executed at an edge node.")

            elif status == REQUEST:
                service['receiveTime'] = curTime
                self.controller.add_request_labels_to_node(receiver, service)
                # Processing a request
                source, cache_ret = self.view.closest_source(service, node)
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                cache_delay = 0
                if node == source:
                    if not cache_ret and self.view.has_cache(node):
                        if self.controller.put_content(source, content):
                            cache_delay = 0.01
                        else:
                            self.controller.put_content_local_cache(source, content)
                            cache_delay = 0.01
                else:
                    if not cache_ret and self.view.has_cache(source):
                        if self.controller.put_content(source, content):
                            pass
                        else:
                            self.controller.put_content_local_cache(source, content)
                deadline_metric = (
                            deadline - curTime - rtt_delay - compSpot.services[service['content']].service_time)  # /deadline
                if self.debug:
                    print("Deadline metric: " + repr(deadline_metric))
                if self.view.has_service(node, service) and service["service_type"] is "proc":
                    if self.debug:
                        print("Calling admit_task")
                    ret, reason = compSpot.admit_task(service['content'], curTime, flow_id, deadline, receiver, rtt_delay,
                                                      self.controller, self.debug)
                    if self.debug:
                        print("Done Calling admit_task")
                    if ret is False:
                        # Pass the Request upstream
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, service,labels,  next_node,
                                                  flow_id, deadline, rtt_delay, REQUEST)
                        if deadline_metric > 0:
                            self.cand_deadline_metric[node][service['content']] += deadline_metric
                        if self.debug:
                            print("Pass upstream to node: " + repr(next_node))
                        # compSpot.missed_requests[service] += 1 # Added here
                    else:
                        if deadline_metric > 0:
                            self.deadline_metric[node][service['content']] += deadline_metric
                else:  # Not running the service
                    # compSpot.missed_requests[service] += 1
                    if self.debug:
                        print("Not running the service: Pass upstream to node: " + repr(next_node))
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, REQUEST)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service['content']] += deadline_metric
            else:
                print("Error: unrecognised status value : " + repr(status))

    # TODO: UPDATE BELOW WITH COLLECTORS INSTEAD OF PREVIOUS OUTPUT FILES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

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

                elif not self.storMode and self.view.repoStorage[node].getOldestStaleMessage() is not None:
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

                if (self.view.repoStorage[node].getOldestStaleMessage is not None and
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
                #				elif (not self.storMod e and self.view.repoStorage[node].getOldestStaleMessage is not None)
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
                self.view.repoStorage[node].deleteMessage(self.view.repoStorage[node].getOldestProcessMessage.getId)
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

                elif (self.view.repoStorage[node].getOldestStaleMessage is not None):
                    self.oldestSatisfiedDepletion(node)


                elif (self.view.repoStorage[node].getOldestInvalidProcessMessage is not None):
                    self.oldestInvalidProcDepletion(node)

                elif (self.view.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.repoStorage[node].getOldestMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp.getId)
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.repoStorage[node].addToDeplMessages(ctemp)
                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    else:
                        self.view.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.repoStorage[node].getNewestProcessMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp.getId)
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.repoStorage[node].addToDeplProcMessages(ctemp)
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
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
            if (self.view.repoStorage[node].getOldestFreshMessage.getProperty("procTime") is None):
                temp = self.view.repoStorage[node].getOldestFreshMessage
                report = False
                self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)
                """
                * Make sure here that the added message to the cloud depletion 
                * tracking is also tracked by whether it 's Fresh or Stale.
                """
                report = True
                self.view.repoStorage[node].addToStoredMessages(temp)
                self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)



            elif (self.view.repoStorage[node].getOldestShelfMessage is not None):
                if (self.view.repoStorage[node].getOldestShelfMessage.getProperty("procTime") is None):
                    temp = self.view.repoStorage[node].getOldestShelfMessage
                    report = False
                    self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)
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
                    self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.repoStorage[node].getOldestStaleMessage
        self.view.repoStorage[node].deleteMessage(ctemp.getId)
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.repoStorage[node].addToCloudDeplMessages(ctemp)

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
        temp = self.view.repoStorage[node].getOldestInvalidProcessMessage
        self.view.repoStorage[node].deleteMessage(temp.getId)
        if (temp['comp'] is not None):
            if (temp['comp']):
                ctemp = self.compressMessage(node, temp)
                self.view.repoStorage[node].deleteMessage(ctemp.getId)
                storTime = curTime - ctemp['receiveTime']
                ctemp['storTime'] = storTime
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                self.view.repoStorage[node].addToDeplProcMessages(ctemp)

            elif (temp['comp'] is None):
                storTime = curTime - temp['receiveTime']
                temp['storTime'] = storTime
                temp['satisfied'] = False
                if (storTime <= temp['shelfLife'] + 1):
                    temp['overtime'] = False

                elif (storTime > temp['shelfLife'] + 1):
                    temp['overtime'] = True

                self.view.repoStorage[node].addToDeplProcMessages(temp)

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.repoStorage[node].getOldestDeplUnProcMessage
        self.view.repoStorage[node].deleteMessage(temp.getId)
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

    def getProcEndTimes(self):
        return self.procEndTimes

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


@register_strategy('HYBRIDS_SPEC_REPO_APP')
class HServSpecStorApp(Strategy):
    """
    Run in passive mode - don't process messages, but store
    """

    def __init__(self, s, view, controller, replacement_interval=10, debug=False, n_replacements=1, **kwargs):
        super(HServSpecStorApp, self).__init__(view, controller)

        self.replacement_interval = replacement_interval
        self.n_replacements = n_replacements
        self.last_replacement = 0
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        # metric to rank each VM of Comp. Spot
        self.deadline_metric = {x: {} for x in range(0, self.num_nodes)}
        self.cand_deadline_metric = {x: {} for x in range(0, self.num_nodes)}
        self.replacements_so_far = 0
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0



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

    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            cs.running_requests = [0 for x in range(0, self.num_services)]
            cs.missed_requests = [0 for x in range(0, self.num_services)]
            cs.scheduler.idleTime = 0.0
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0

    # HYBRID
    def replace_services1(self, time):
        for node, cs in self.compSpots.items():
            n_replacements = 0
            if cs.is_cloud:
                continue
            runningServiceResidualTimes = self.deadline_metric[node]
            missedServiceResidualTimes = self.cand_deadline_metric[node]
            # service_residuals = []
            running_services_utilisation_normalised = []
            missed_services_utilisation = []
            delay = {}
            # runningServicesUtil = {}
            # missedServicesUtil = {}

            if len(cs.scheduler.upcomingTaskQueue) > 0:
                print("Printing upcoming task queue at node: " + str(cs.node))
                for task in cs.scheduler.upcomingTaskQueue:
                    task.print_task()

            util = []
            util_normalised = {}

            if self.debug:
                print("Replacement at node " + repr(node))
            for service in range(0, self.num_services):
                if cs.numberOfVMInstances[service] != len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                    print("Error: number of vm instances do not match for service: " + str(service) + " node: " + str(
                        cs.node))
                    print("numberOfInstances = " + str(cs.numberOfVMInstances[service]))
                    print("Total VMs: " + str(
                        len(cs.scheduler.idleVMs[service]) + len(cs.scheduler.busyVMs[service]) + len(
                            cs.scheduler.startingVMs[service])))
                    print("\t Idle: " + str(len(cs.scheduler.idleVMs[service])) + " Busy: " + str(
                        len(cs.scheduler.busyVMs[service])) + " Starting: " + str(
                        len(cs.scheduler.startingVMs[service])))
                d_metric = 0.0
                u_metric = 0.0
                util.append([service, (cs.missed_requests[service] + cs.running_requests[service]) * cs.services[
                    service].service_time])
                if cs.numberOfVMInstances[service] == 0:
                    # No instances
                    if cs.missed_requests[service] > 0:
                        d_metric = 1.0 * missedServiceResidualTimes[service] / cs.missed_requests[service]
                    else:
                        d_metric = float('inf')
                    delay[service] = d_metric
                    u_metric = cs.missed_requests[service] * cs.services[service].service_time
                    if u_metric > self.replacement_interval:
                        u_metric = self.replacement_interval
                    # missedServiceResidualTimes[service] = d_metric
                    # missedServicesUtil[service] = u_metric
                    missed_services_utilisation.append([service, u_metric])
                    # service_residuals.append([service, d_metric])
                elif cs.numberOfVMInstances[service] > 0:
                    # At least one instance
                    if cs.running_requests[service] > 0:
                        d_metric = 1.0 * runningServiceResidualTimes[service] / cs.running_requests[service]
                    else:
                        d_metric = float('inf')
                    runningServiceResidualTimes[service] = d_metric
                    u_metric_missed = (cs.missed_requests[service]) * cs.services[service].service_time * 1.0
                    if u_metric_missed > self.replacement_interval:
                        u_metric_missed = self.replacement_interval
                    # missedServicesUtil[service] = u_metric_missed
                    u_metric_served = (1.0 * cs.running_requests[service] * cs.services[service].service_time) / \
                                      cs.numberOfVMInstances[service]
                    # runningServicesUtil[service] = u_metric_served
                    missed_services_utilisation.append([service, u_metric_missed])
                    # running_services_latency.append([service, d_metric])
                    running_services_utilisation_normalised.append(
                        [service, u_metric_served / cs.numberOfVMInstances[service]])
                    # service_residuals.append([service, d_metric])
                    delay[service] = d_metric
                else:
                    print("This should not happen")
            running_services_utilisation_normalised = sorted(running_services_utilisation_normalised,
                                                             key=lambda x: x[1])  # smaller to larger
            missed_services_utilisation = sorted(missed_services_utilisation, key=lambda x: x[1],
                                                 reverse=True)  # larger to smaller
            # service_residuals = sorted(service_residuals, key=lambda x: x[1]) #smaller to larger
            exit_loop = False
            for service_missed, missed_util in missed_services_utilisation:
                if exit_loop:
                    break
                for indx in range(len(running_services_utilisation_normalised)):
                    service_running = running_services_utilisation_normalised[indx][0]
                    running_util = running_services_utilisation_normalised[indx][1]
                    if running_util > missed_util:
                        exit_loop = True
                        break
                    if service_running == service_missed:
                        continue
                    if missed_util >= running_util and delay[service_missed] < delay[service_running] and delay[
                        service_missed] > 0:
                        self.controller.reassign_vm(time, cs, service_running, service_missed, self.debug)
                        # cs.reassign_vm(self.controller, time, service_running, service_missed, self.debug)
                        if self.debug:
                            print("Missed util: " + str(missed_util) + " running util: " + str(
                                running_util) + " Adequate time missed: " + str(
                                delay[service_missed]) + " Adequate time running: " + str(delay[service_running]))
                        del running_services_utilisation_normalised[indx]
                        n_replacements += 1
                        break
            if self.debug:
                print(str(n_replacements) + " replacements at node:" + str(cs.node) + " at time: " + str(time))
                for node in self.compSpots.keys():
                    cs = self.compSpots[node]
                    if cs.is_cloud:
                        continue
                    if cs.node != 14 and cs.node != 6:
                        continue
                    for service in range(0, self.num_services):
                        if cs.numberOfVMInstances[service] > 0:
                            print("Node: " + str(node) + " has " + str(
                                cs.numberOfVMInstances[service]) + " instance of " + str(service))

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

    # TODO: ADAPT FOR MOST-USED/SPECIFIC-SERVICE-PROXIMATE PLACEMENT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    def handle(self, curTime, msg, node, path, log, feedback, flow_id, rtt_delay):
        """

        curTime:

        msg:

        node:

        path:

        log:

        flow_id:

        rtt_delay:

        :return:
        """
        msg['receiveTime'] = time.time()
        if self.view.hasStorageCapability(node):
            # TODO: Check usages of IS OPERATOR throughout code!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            on_path = True
            node_c, in_path = self.view.most_services_labels_closest_repo(msg['labels'], node, path, on_path)
            on_path = False
            node_s, off_path = self.view.most_services_labels_closest_repo(msg['labels'], node, path, on_path)
            if node == node_c and in_path:
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_storage_labels_to_node(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, False)
            elif node is self.view.all_labels_most_requests(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_request_labels_to_storage(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, True)
            elif node == node_s and not off_path:
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_storage_labels_to_node(node, msg)
                self.controller.add_request_labels_to_storage(node, msg, False)
            elif in_path:
                edr = node_c
                self.controller.add_request_labels_to_node(node, msg)
                self.controller.start_session(curTime, node, msg, log, feedback, flow_id, msg['freshness_per'])
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                          msg['freshness_per'], rtt_delay, REQUEST)
            elif node_s and not off_path:
                edr = node_s
                self.controller.add_request_labels_to_node(node, msg)
                self.controller.start_session(curTime, node, msg, log, feedback, flow_id, msg['freshness_per'])
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                          msg['freshness_per'], rtt_delay, REQUEST)

            else:
                edr = self.view.all_labels_most_requests(msg["labels"])
                self.controller.add_request_labels_to_node(node, msg)
                self.controller.start_session(curTime, node, msg, log, feedback, flow_id, msg['shelf_life'])
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                          msg['shelf_life'], rtt_delay, REQUEST)

        elif not self.view.hasStorageCapability(node) and msg['service_type'] is "nonproc":
            curTime = time.time()
            self.view.repoStorage[node].deleteMessage(msg.getId)
            storTime = curTime - msg['receiveTime']
            msg['storTime'] = storTime
            if msg['storTime'] < msg['shelfLife']:
                msg['satisfied'] = False
                msg['overtime'] = False
            self.view.repoStorage[node].addToDeplMessages(msg)

        else:
            msg['satisfied'] = False
            msg['overtime'] = False
            self.view.repoStorage[node].addToDeplMessages(msg)

        return msg

    @inheritdoc(Strategy)
    def process_event(self, curTime, receiver, content, log, labels,
                      node, flow_id, deadline, rtt_delay, status, task=None):
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
            feedback = True
        else:
            feedback = False

        if self.view.hasStorageCapability(node):

            self.updateCloudBW(node)
            self.deplCloud(node)
            self.updateDeplBW(node)
            self.deplStorage(node)

        elif not self.view.hasStorageCapability(node) and self.view.has_computationalSpot(node):
            self.updateUpBW(node)
            self.deplUp(node)

        """
                response : True, if this is a response from the cloudlet/cloud
                deadline : deadline for the request 
                flow_id : Id of the flow that the request/response is part of
                node : the current node at which the request/response arrived

                TODO: Maybe could even implement the old "application" storage
                    space and message services management in here, as well!!!!

                """
        # self.debug = False
        # if node == 12:
        #    self.debug = True
        path = self.view.shortest_path(receiver, node)
        service = None
        if content["service_type"] is "proc":
            service = content if content['content'] is not '' else self.view.all_labels_main_source(labels)['content']
        self.handle(curTime, content, node, path, log, feedback, flow_id, rtt_delay)
        source = self.view.content_source(content)

        if curTime - self.last_replacement > self.replacement_interval:
            # self.print_stats()
            print("Replacement time: " + repr(curTime))
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, curTime)
            self.replace_services1(curTime)
            self.last_replacement = curTime
            self.initialise_metrics()

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        if service is not None:
            #  Request reached the cloud
            if source == node and status == REQUEST:
                self.controller.add_request_labels_to_node(receiver, service)
                service['receiveTime'] = curTime
                ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver, rtt_delay,
                                                  self.controller, self.debug)

                if not ret:
                    print("This should not happen in Hybrid.")
                    raise ValueError("Task should not be rejected at the cloud.")
                return

                #  Request at the receiver
            if receiver == node and status == REQUEST:
                service['receiveTime'] = curTime
                self.controller.add_request_labels_to_node(receiver, service)
                self.controller.start_session(curTime, receiver, service, log, feedback, flow_id, deadline)
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                rtt_delay += delay * 2
                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, REQUEST)
                return

            if self.debug:
                print("\nEvent\n time: " + repr(curTime) + " receiver  " + repr(receiver) + " service " + repr(
                    service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(
                    deadline) + " status " + repr(status))

            if status == RESPONSE:
                # response is on its way back to the receiver
                if node == receiver:
                    self.controller.end_session(True, curTime, flow_id)  # TODO add flow_time
                    return
                else:
                    path = self.view.shortest_path(node, receiver)
                    next_node = path[1]
                    delay = self.view.link_delay(node, next_node)
                    path_del = self.view.path_delay(node, receiver)
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, RESPONSE)
                    if path_del + curTime > deadline:
                        compSpot.missed_requests[service['content']] += 1

            elif status == TASK_COMPLETE:
                self.controller.complete_task(task, curTime)
                if service['freshness_per'] > curTime - service['receiveTime']:
                    service['Fresh'] = True
                    service['Shelf'] = True
                elif service['shelf_life'] > curTime - service['receiveTime']:
                    service['Fresh'] = False
                    service['Shelf'] = True
                else:
                    service['Fresh'] = False
                    service['Shelf'] = False
                if node != source:
                    newTask = compSpot.scheduler.schedule(curTime)
                    # schedule the next queued task at this node
                    if newTask is not None:
                        self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.service,
                                                  newTask.labels, node, newTask.flow_id,
                                                  newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask)

                # forward the completed task
                if task.taskType == Task.TASK_TYPE_VM_START:
                    return
                path = self.view.shortest_path(node, receiver)
                path_delay = self.view.path_delay(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.view.hasStorageCapability(node):
                    service['service_type'] = None
                    service['service_type'] = "processed"
                    if all(elem in labels for elem in self.view.model.node_labels(node)["request_labels"]):
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_request_labels_to_storage(node, service)
                    else:
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_storage_labels_to_node(node, service)

                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, RESPONSE)
                if node != source and curTime + path_delay > deadline:
                    print("Error in HYBRID strategy: Request missed its deadline\nResponse at receiver at time: " + str(
                        curTime + path_delay) + " deadline: " + str(deadline))
                    task.print_task()
                    # raise ValueError("This should not happen: a task missed its deadline after being executed at an edge node.")

            elif status == REQUEST:
                service['receiveTime'] = curTime
                self.controller.add_request_labels_to_node(receiver, service)
                # Processing a request
                source, cache_ret = self.view.closest_source(node, service)
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                cache_delay = 0
                if node == source:
                    if not cache_ret and self.view.has_cache(node):
                        if self.controller.put_content(source, content):
                            cache_delay = 0.01
                        else:
                            self.controller.put_content_local_cache(source, content)
                            cache_delay = 0.01
                else:
                    if not cache_ret and self.view.has_cache(source):
                        if self.controller.put_content(source, content):
                            pass
                        else:
                            self.controller.put_content_local_cache(source, content)
                deadline_metric = (
                            deadline - curTime - rtt_delay - compSpot.services[service['content']].service_time)  # /deadline
                if self.debug:
                    print("Deadline metric: " + repr(deadline_metric))
                if self.view.has_service(node, service) and service["service_type"] is "proc":
                    if self.debug:
                        print("Calling admit_task")
                    ret, reason = compSpot.admit_task(service['content'], curTime, flow_id, deadline, receiver, rtt_delay,
                                                      self.controller, self.debug)
                    if self.debug:
                        print("Done Calling admit_task")
                    if ret is False:
                        # Pass the Request upstream
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, service,labels,  next_node,
                                                  flow_id, deadline, rtt_delay, REQUEST)
                        if deadline_metric > 0:
                            self.cand_deadline_metric[node][service['content']] += deadline_metric
                        if self.debug:
                            print("Pass upstream to node: " + repr(next_node))
                        # compSpot.missed_requests[service] += 1 # Added here
                    else:
                        if deadline_metric > 0:
                            self.deadline_metric[node][service['content']] += deadline_metric
                else:  # Not running the service
                    # compSpot.missed_requests[service] += 1
                    if self.debug:
                        print("Not running the service: Pass upstream to node: " + repr(next_node))
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, REQUEST)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service['content']] += deadline_metric
            else:
                print("Error: unrecognised status value : " + repr(status))

    # TODO: UPDATE BELOW WITH COLLECTORS INSTEAD OF PREVIOUS OUTPUT FILES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

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

                elif not self.storMode and self.view.repoStorage[node].getOldestStaleMessage() is not None:
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

                if (self.view.repoStorage[node].getOldestStaleMessage is not None and
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
                #				elif (not self.storMod e and self.view.repoStorage[node].getOldestStaleMessage is not None)
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
                self.view.repoStorage[node].deleteMessage(self.view.repoStorage[node].getOldestProcessMessage.getId)
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

                elif (self.view.repoStorage[node].getOldestStaleMessage is not None):
                    self.oldestSatisfiedDepletion(node)


                elif (self.view.repoStorage[node].getOldestInvalidProcessMessage is not None):
                    self.oldestInvalidProcDepletion(node)

                elif (self.view.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.repoStorage[node].getOldestMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp.getId)
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.repoStorage[node].addToDeplMessages(ctemp)
                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    else:
                        self.view.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.repoStorage[node].getNewestProcessMessage()
                    self.view.repoStorage[node].deleteMessage(ctemp.getId)
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.repoStorage[node].addToDeplProcMessages(ctemp)
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
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
            if (self.view.repoStorage[node].getOldestFreshMessage.getProperty("procTime") is None):
                temp = self.view.repoStorage[node].getOldestFreshMessage
                report = False
                self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)
                """
                * Make sure here that the added message to the cloud depletion 
                * tracking is also tracked by whether it 's Fresh or Stale.
                """
                report = True
                self.view.repoStorage[node].addToStoredMessages(temp)
                self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)



            elif (self.view.repoStorage[node].getOldestShelfMessage is not None):
                if (self.view.repoStorage[node].getOldestShelfMessage.getProperty("procTime") is None):
                    temp = self.view.repoStorage[node].getOldestShelfMessage
                    report = False
                    self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)
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
                    self.view.repoStorage[node].deleteProcessedMessage(temp.getId, report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.repoStorage[node].getOldestStaleMessage
        self.view.repoStorage[node].deleteMessage(ctemp.getId)
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.repoStorage[node].addToCloudDeplMessages(ctemp)

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
        temp = self.view.repoStorage[node].getOldestInvalidProcessMessage
        self.view.repoStorage[node].deleteMessage(temp.getId)
        if (temp['comp'] is not None):
            if (temp['comp']):
                ctemp = self.compressMessage(node, temp)
                self.view.repoStorage[node].deleteMessage(ctemp.getId)
                storTime = curTime - ctemp['receiveTime']
                ctemp['storTime'] = storTime
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                self.view.repoStorage[node].addToDeplProcMessages(ctemp)

            elif (temp['comp'] is None):
                storTime = curTime - temp['receiveTime']
                temp['storTime'] = storTime
                temp['satisfied'] = False
                if (storTime <= temp['shelfLife'] + 1):
                    temp['overtime'] = False

                elif (storTime > temp['shelfLife'] + 1):
                    temp['overtime'] = True

                self.view.repoStorage[node].addToDeplProcMessages(temp)

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.repoStorage[node].getOldestDeplUnProcMessage
        self.view.repoStorage[node].deleteMessage(temp.getId)
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

    def getProcEndTimes(self):
        return self.procEndTimes

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