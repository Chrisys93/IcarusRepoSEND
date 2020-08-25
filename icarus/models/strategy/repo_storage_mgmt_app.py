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

import math
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
STORE = 3
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

    def __init__(self, view, controller, replacement_interval=10, debug=False, n_replacements=1,
                 depl_rate=10000000, cloud_lim=20000000, max_stor=9900000000, min_stor=10000000,**kwargs):
        super(GenRepoStorApp, self).__init__(view, controller)

        self.lastDepl = 0
        
        self.last_period = 0
        
        self.last_period = 0

        self.cloudEmptyLoop = True

        self.deplEmptyLoop = True

        self.upEmptyLoop = True

        self.lastCloudUpload = 0

        self.deplBW = 0

        self.cloudBW = 0

        self.procMinI = 0
        # processedSize = self.procSize * self.proc_ratio
        self.depl_rate = depl_rate
        self.cloud_lim = cloud_lim
        self.max_stor = max_stor
        self.min_stor = min_stor
        self.view = view


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

    def handle(self, curTime, receiver, msg, node, flow_id, deadline, rtt_delay):
        if (self.view.hasStorageCapability(node) and node.hasProcessingCapability):
            self.view.model.repoStorage[node].addToStoredMessages(msg)

        elif not self.view.hasStorageCapability(node) and node.hasProcessingCapability and (
        msg['type']).equalsIgnoreCase("nonproc"):
            curTime = time.time()
            self.view.model.repoStorage[node].deleteAnyMessage(msg['content'])
            storTime = curTime - msg['receiveTime']
            msg['storTime'] = storTime
            if (msg['storTime'] < msg['shelfLife']):
                msg['satisfied'] = False
                msg['overtime'] = False
            self.view.model.repoStorage[node].addToDeplMessages(msg)

        else:
            msg['overtime'] = False
            source = self.view.content_source_cloud(msg, msg['labels'])
            if not source:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                        source = n
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay * 2
            self.controller.add_event(curTime + delay, receiver, msg, msg['labels'], next_node, flow_id, deadline, rtt_delay,
                                      STORE)
            # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)

        return msg


    @inheritdoc(Strategy)
    def process_event(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay):

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

        if time.time() - self.last_period >= 1:
            self.last_period = time.time()
            period = True
        else:
            period = False

        if self.view.hasStorageCapability(node):

            self.updateCloudBW(node, period)
            self.deplCloud(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)
            self.updateDeplBW(node, period)
            self.deplStorage(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)

        elif not self.view.hasStorageCapability(node) and self.view.has_computationalSpot(node):
            self.updateUpBW(node, period)
            self.deplUp(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)

    def updateCloudBW(self, node, period):
        self.cloudBW = self.view.model.repoStorage[node].getDepletedCloudProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedCloudMessagesBW(period)

    def updateUpBW(self, node, period):
        self.cloudBW = self.view.model.repoStorage[node].getDepletedCloudProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedMessagesBW(period)

    def updateDeplBW(self, node, period):
        self.deplBW = self.view.model.repoStorage[node].getDepletedProcMessagesBW(period) + \
                      self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                      self.view.model.repoStorage[node].getDepletedMessagesBW(period)

    def deplCloud(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcessedMessagesSize() +
                self.view.model.repoStorage[node].getStaleMessagesSize() >
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor)):
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

                if not self.view.model.repoStorage[node].isProcessedEmpty():
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.model.repoStorage[node].getOldestDeplUnProcMessage()() is not None:
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")


                elif self.view.model.repoStorage[node].getOldestStaleMessage()() is not None:
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                else:
                    self.cloudEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.cloudBW)

                # Revise:
                self.updateCloudBW(node, period)

                # System.out.prln("Depletion is at: " + deplBW)
                self.lastDepl = curTime
                """System.out.prln("self.cloudBW is at " +
                      self.cloudBW +
                      " self.cloud_lim is at " +
                      self.cloud_lim +
                      " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equal " +
                      (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor) +
                      " Total space is " + self.view.model.repoStorage[node].getTotalStorageSpace()) """
            # System.out.prln("Depleted  messages: " + sdepleted)
        elif (self.view.model.repoStorage[node].getProcessedMessagesSize() +
              self.view.model.repoStorage[node].getStaleMessagesSize()) > \
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.max_stor):
            self.cloudEmptyLoop = True
            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                if (self.view.model.repoStorage[node].getOldestStaleMessage() is not None and
                        self.cloudBW < self.cloud_lim):
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """
                    * Oldest unprocessed messages should be given priority for depletion
                    * at a certain po.
                    """

                elif (self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None):
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """
                * Oldest processed message is depleted (as a FIFO type of storage,
                * and a  message for processing is processed
                    """
                elif (not self.view.model.repoStorage[node].isProcessedEmpty):
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateCloudBW(node, period)
                    # System.out.prln("Depletion is at: " + deplBW)
                    self.lastDepl = curTime
                    """System.out.prln("self.cloudBW is at " +
                      self.cloudBW +
                      " self.cloud_lim is at " +
                      self.cloud_lim +
                      " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equal " +
                      (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor) +
                      " Total space is " + self.view.model.repoStorage[node].getTotalStorageSpace()) """
                # System.out.prln("Depleted  messages: " + sdepleted)

    def deplUp(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcessedMessagesSize() >
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor)):

            self.cloudEmptyLoop = True

            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """

                * Oldest processed	message is depleted(as a FIFO type of storage,
                * and a	message for processing is processed

                """
                # TODO: NEED TO add COMPRESSED PROCESSED messages to storage AFTER normal servicing
                if (not self.view.model.repoStorage[node].isProcessedEmpty):
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None:
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                elif self.view.model.repoStorage[node].getOldestStaleMessage() is not None:
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                #
                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateUpBW(node, period)

            # System.out.prln("Depletion is at : "+ deplBW)
            self.lastDepl = curTime
            """System.out.prln("self.cloudBW is at " +
                     self.cloudBW +
                     " self.cloud_lim is at " +
                     self.cloud_lim +
                     " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equa l "+
                     (self.view.model.repoStorage[node].getTotalStorageSpac e *self.min_st or)+
                     " Total space i s  "+self.view.model.repoStorage[node].getTotalStorageSpace()( ) )"""
            # System.out.prln("Depleted  messages : "+ sdepleted)

            self.upEmptyLoop = True
        for i in range(0, 50) and self.cloudBW > self.cloud_lim and \
                 not self.view.model.repoStorage[node].isProcessingEmpty() and self.upEmptyLoop:
            if (not self.view.model.repoStorage[node].isProcessedEmpty):
                self.processedDepletion(node)

            elif (not self.view.model.repoStorage[node].isProcessingEmpty):
                self.view.model.repoStorage[node].deleteAnyMessage(
                    self.view.model.repoStorage[node].getOldestProcessMessage['content'])
            else:
                self.upEmptyLoop = False

    # System.out.prln("Depletion is at: "+ self.cloudBW)

    def deplStorage(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcMessagesSize() +
                self.view.model.repoStorage[node].getMessagesSize() >
                self.view.model.repoStorage[node].getTotalStorageSpace() * self.max_stor):
            self.deplEmptyLoop = True
            for i in range(0, 50) and self.deplBW < self.depl_rate and self.deplEmptyLoop:
                if (self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None):
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                elif (self.view.model.repoStorage[node].getOldestStaleMessage() is not None):
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")


                elif (self.view.model.repoStorage[node].getOldestInvalidProcessMessage() is not None):
                    msg = self.oldestInvalidProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                elif (self.view.model.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.model.repoStorage[node].getOldestMessage()
                    self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.model.repoStorage[node].addToDeplMessages(ctemp)
                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.model.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, ctemp, labels, next_node, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    else:
                        self.view.model.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.model.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.model.repoStorage[node].getNewestProcessMessage()
                    self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.model.repoStorage[node].addToDeplProcMessages(ctemp)
                    source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, ctemp, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    if (storTime <= ctemp['shelfLife'] + 1):
                        ctemp['overtime'] = False

                    elif (storTime > ctemp['shelfLife'] + 1):
                        ctemp['overtime'] = True

                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.model.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                        source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                    node] is not None:
                                    source = n
                        path = self.view.shortest_path(node, source)
                        next_node = path[1]
                        delay = self.view.path_delay(node, next_node)
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, ctemp, labels, next_node, flow_id,
                                                  deadline, rtt_delay, STORE)
                        if self.debug:
                            print("Message is scheduled to be stored in the CLOUD")
                    else:
                        self.view.model.repoStorage[node].addToDeplMessages(ctemp)
                        source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                    node] is not None:
                                    source = n
                        path = self.view.shortest_path(node, source)
                        next_node = path[1]
                        delay = self.view.path_delay(node, next_node)
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, ctemp, labels, next_node, flow_id,
                                                  deadline, rtt_delay, STORE)
                        if self.debug:
                            print("Message is scheduled to be stored in the CLOUD")
            else:
                self.deplEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.deplBW)

                self.updateDeplBW(node, period)
                self.updateCloudBW(node, period)
            # Revise:
            self.lastDepl = curTime

    def processedDepletion(self, node):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getOldestFreshMessage() is not None):
            if (self.view.model.repoStorage[node].getOldestFreshMessage().getProperty("procTime") is None):
                temp = self.view.model.repoStorage[node].getOldestFreshMessage()
                report = False
                self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
                """
                * Make sure here that the added message to the cloud depletion 
                * tracking is also tracked by whether it 's Fresh or Stale.
                """
                report = True
                self.view.model.repoStorage[node].addToStoredMessages(temp)
                self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)



            elif (self.view.model.repoStorage[node].getOldestShelfMessage() is not None):
                if (self.view.model.repoStorage[node].getOldestShelfMessage().getProperty("procTime") is None):
                    temp = self.view.model.repoStorage[node].getOldestShelfMessage()
                    report = False
                    self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
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
                    self.view.model.repoStorage[node].addToStoredMessages(temp)
                    self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.model.repoStorage[node].getOldestStaleMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.model.repoStorage[node].addToCloudDeplMessages(ctemp)

        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.model.repoStorage[node].addToCloudDeplMessages(ctemp)

        self.lastCloudUpload = curTime

    def oldestInvalidProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.model.repoStorage[node].getOldestInvalidProcessMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(temp['content'])
        if (temp['comp'] is not None):
            if (temp['comp']):
                ctemp = self.compressMessage(node, temp)
                self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                storTime = curTime - ctemp['receiveTime']
                ctemp['storTime'] = storTime
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                self.view.model.repoStorage[node].addToDeplProcMessages(ctemp)

            elif (temp['comp'] is None):
                storTime = curTime - temp['receiveTime']
                temp['storTime'] = storTime
                temp['satisfied'] = False
                if (storTime <= temp['shelfLife'] + 1):
                    temp['overtime'] = False

                elif (storTime > temp['shelfLife'] + 1):
                    temp['overtime'] = True

                self.view.model.repoStorage[node].addToDeplProcMessages(temp)
        return temp

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.model.repoStorage[node].getOldestDeplUnProcMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(temp['content'])
        self.view.model.repoStorage[node].addToDepletedUnProcMessages(temp)
        return temp

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

    def __init__(self, view, controller, replacement_interval=10, debug=False, n_replacements=1,
                 depl_rate=10000000, cloud_lim=20000000, max_stor=9900000000, min_stor=10000000,**kwargs):
        super(HServRepoStorApp, self).__init__(view, controller)

        self.view.model.strategy = 'HYBRIDS_REPO_APP'
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
        self.serviceNodeUtil = [None]*len(self.receivers)
        self.last_period = 0
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0

        for recv in self.receivers:
            recv = int(recv[4:])
            self.serviceNodeUtil[recv] = [None]*self.num_nodes
            for n in self.compSpots.keys():
                cs = self.compSpots[n]
                if cs.is_cloud:
                    continue
                self.serviceNodeUtil[recv][n] = [0.0]*self.num_services

        # vars

        self.lastDepl = 0

        self.last_period = 0

        self.cloudEmptyLoop = True

        self.deplEmptyLoop = True

        self.upEmptyLoop = True

        self.lastCloudUpload = 0

        self.deplBW = 0

        self.cloudBW = 0

        self.procMinI = 0
        # processedSize = self.procSize * self.proc_ratio
        self.depl_rate = depl_rate
        self.cloud_lim = cloud_lim
        self.max_stor = max_stor
        self.min_stor = min_stor
        self.self_calls = {}
        for node in view.storage_nodes(True):
            self.self_calls[node] = 0
        self.view = view

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
    def replace_services1(self, curTime):
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
                    if cs.numberOfVMInstances[service] > len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                        aVM = VM(self, service)
                        cs.scheduler.idleVMs[service].append(aVM)
                    elif cs.numberOfVMInstances[service] < len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                        cs.numberOfVMInstances[service] = len(cs.scheduler.idleVMs[service]) + len(cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service])

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
                        self.controller.reassign_vm(curTime, cs, service_running, service_missed, self.debug)
                        # cs.reassign_vm(self.controller, curTime, service_running, service_missed, self.debug)
                        if self.debug:
                            print("Missed util: " + str(missed_util) + " running util: " + str(
                                running_util) + " Adequate time missed: " + str(
                                delay[service_missed]) + " Adequate time running: " + str(delay[service_running]))
                        del running_services_utilisation_normalised[indx]
                        n_replacements += 1
                        break
            if self.debug:
                print(str(n_replacements) + " replacements at node:" + str(cs.node) + " at time: " + str(curTime))
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

    # @profile
    def handle(self, curTime, receiver, msg, node, log, feedback, flow_id, rtt_delay, deadline):
        # TODO: NEED TO MAKE A COLLECTOR FUNCTION TO UPDATE A HOP COUNTER FOR DATA REPLICATION!!!!!!!!!!!!!!!!!!!!!!!!!!
        msg['receiveTime'] = time.time()
        if self.view.hasStorageCapability(node) and 'satisfied' not in msg or ('Shelf' not in msg or msg['Shelf']):
            self.controller.add_replication_hops(msg)
            if node is self.view.all_labels_main_source(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_replication_hops(msg)
                self.controller.add_storage_labels_to_node(node, msg)
                # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
                if self.controller.has_request_labels(node, msg['labels']):
                    self.controller.add_request_labels_to_storage(node, msg['labels'], False)
            elif node in self.view.labels_sources(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_replication_hops(msg)
                self.controller.add_storage_labels_to_node(node, msg)
                # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
                if self.controller.has_request_labels(node, msg['labels']):
                    self.controller.add_request_labels_to_storage(node, msg['labels'], False)
            else:
                edr = self.view.all_labels_main_source(msg["labels"])
                if edr:
                    self.controller.add_request_labels_to_node(node, msg)
                    # if edr and edr.hasMessage(msg['content'], msg['labels']):
                    #     msg = edr.hasMessage(msg['content'], msg['labels'])
                    # else:
                    #     msg['shelf_life'] = deadline - curTime
                    path = self.view.shortest_path(node, edr.node)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    self.controller.add_request_labels_to_node(node, msg)

                    self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                              curTime + msg['shelf_life'], rtt_delay, STORE)
                    # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)
                    self.controller.replicate(node, next_node)

        else:
            msg['overtime'] = False
            source = self.view.content_source_cloud(msg, msg['labels'])
            if not source:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                        source = n
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay * 2
            self.controller.add_event(curTime + delay, receiver, msg, msg['labels'], next_node, flow_id, deadline, rtt_delay,
                                      STORE)
            # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)

        return msg

    @inheritdoc(Strategy)
    # @profile
    def process_event(self, curTime, receiver, content, log, labels, node, flow_id, deadline, rtt_delay, status,
                      task=None):
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

        if time.time() - self.last_period >= 1:
            self.last_period = time.time()
            period = True
        else:
            period = False

        if self.view.hasStorageCapability(node):

            self.updateCloudBW(node, period)
            self.deplCloud(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)
            self.updateDeplBW(node, period)
            self.deplStorage(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)

        elif not self.view.hasStorageCapability(node) and self.view.has_computationalSpot(node):
            self.updateUpBW(node, period)
            self.deplUp(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)

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
        if type(content) is dict and 'shelf_life' in content and 'max_replications' in content:
            if content["shelf_life"] and content['replications'] <= content['max_replications']:
                source, in_cache = self.view.closest_source(node, content)
                path = self.view.shortest_path(node, source)
                self.handle(curTime, receiver, content, node, log, feedback, flow_id, rtt_delay, deadline)
        elif type(content) is dict and 'shelf_life' in content and content["shelf_life"] :
            source, in_cache = self.view.closest_source(node, content)
            path = self.view.shortest_path(node, source)
            self.handle(curTime, receiver, content, node, log, feedback, flow_id, rtt_delay, deadline)
        service = content

        if curTime - self.last_replacement > self.replacement_interval:
            #self.print_stats()
            print("Replacement time: " + repr(curTime))
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, curTime)
            self.replace_services1(curTime)
            self.last_replacement = curTime
            self.initialise_metrics()

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)

        source, in_cache = self.view.closest_source(node, service)
        cloud_source = self.view.content_source_cloud(service, labels)
        if not cloud_source:
            cloud_source, in_cache = self.view.closest_source(node, service)
            if cloud_source == node:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                        node] is not None:
                        cloud_source = n

        path = self.view.shortest_path(node, cloud_source)

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
            if cloud_source == node and status == REQUEST:

                cache_delay = 0
                if not in_cache and self.view.has_cache(node):
                    if self.controller.put_content(source, content['content']):
                        cache_delay = 0.005
                    else:
                        self.controller.put_content_local_cache(source)
                        cache_delay = 0.005
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                elif in_cache:
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                # for n in range(0, 3):
                #     self.controller.add_replication_hops(content)
                # self.controller.replication_overhead_update(content)
                # self.controller.remove_replication_hops(service)
                ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver,
                                                  rtt_delay + cache_delay, self.controller, self.debug)

                if ret == False:
                    if type(node) == str and "src" in node:
                        print("This should not happen in Hybrid.")
                        raise ValueError("Task should not be rejected at the cloud.")
                    else:
                        if type(service) is dict and self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                            self.controller.add_request_labels_to_node(node, service)
                        # request is to be executed in the cloud and returned to receiver
                        services = self.view.services()
                        serviceTime = services[service['content']].service_time
                        self.controller.add_event(curTime + rtt_delay + serviceTime, receiver, service, labels,
                                                  receiver, flow_id, deadline, rtt_delay, RESPONSE)
                        if self.debug:
                            print("Request is scheduled to run at the CLOUD")
                    for n in path[1:]:
                        cs = self.view.compSpot(n)
                        if cs.is_cloud:
                            continue
                        self.serviceNodeUtil[int(receiver[4:])][n][service['content']] += self.view.services()[
                            service['content']].service_time
                    return
                return

            if source == node and status == REQUEST:
                cache_delay = 0
                if not in_cache and self.view.has_cache(node):
                    if self.controller.put_content(source, content['content']):
                        cache_delay = 0.005
                    else:
                        self.controller.put_content_local_cache(source)
                        cache_delay = 0.005
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                elif in_cache:
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                if compSpot is not None and self.view.has_service(node, service) and service["service_type"] is "proc":
                    ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver,
                                                      rtt_delay + cache_delay, self.controller, self.debug)

                    if ret == False:


                        if type(service) is dict and self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                            self.controller.add_request_labels_to_node(node, service)
                        source = self.view.content_source_cloud(service, labels)
                        if not source:
                            source, in_cache = self.view.closest_source(node, service)
                            if source == node:
                                for n in self.view.model.comp_size:
                                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                        node] is not None:
                                        source = n
                        path = self.view.shortest_path(node, source)
                        upstream_node = self.find_closest_feasible_node(receiver, flow_id, path, curTime, service, deadline, rtt_delay)
                        delay = self.view.path_delay(node, source)
                        # self.controller.add_event(curTime + delay, receiver, service, labels, upstream_node, flow_id,
                        #                           deadline, rtt_delay, STORE)
                        rtt_delay += delay * 2
                        if upstream_node != source:
                            self.controller.add_event(curTime + delay, receiver, content, labels, upstream_node,
                                                      flow_id, deadline, rtt_delay, REQUEST)
                            if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                            if self.debug:
                                print("Message is scheduled to run at: " + str(upstream_node))
                        else:  # request is to be executed in the cloud and returned to receiver
                            services = self.view.services()
                            serviceTime = services[service['content']].service_time
                            self.controller.add_event(curTime + rtt_delay + serviceTime, receiver, service, labels,
                                                      receiver, flow_id, deadline, rtt_delay, RESPONSE)
                            if self.debug:
                                print("Request is scheduled to run at the CLOUD")
                    return
                return

            #  Request at the receiver
            if receiver == node and status == REQUEST:

                self.controller.start_session(curTime, receiver, service, labels, log, flow_id, deadline)
                path = self.view.shortest_path(node, cloud_source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                rtt_delay += delay * 2
                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, REQUEST)
                if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                        service['content'], service['labels']):
                    self.controller.add_request_labels_to_node(node, service)
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
                        if type(content) is dict:
                            compSpot.missed_requests[content['content']] += 1
                        else:
                            compSpot.missed_requests[content] += 1

            elif status == TASK_COMPLETE:
                self.controller.complete_task(task, curTime)
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
                    source, in_cache = self.view.closest_source(node, service)
                    # TODO: BIIIIIG PROBLEM HERE! Need to revise what happens between the following if-elses, because
                    #  it might see that the content is stored, the stored content might not pe processed, or it might,
                    #  and the right flags and/or processing of messages might still not be done correctly because of
                    #  going through one if and/or else, but not going through to the next, so messages might just be
                    #  skipped, in a VERY BAD manner, rendering some of the storage useless, pretty much...
                    if type(service) is not dict and self.controller.has_message(node, labels, content):
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content)
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node, flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return

                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content)
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        service = self.view.storage_nodes()[node].hasMessage(content, labels)
                    elif type(service) is dict and self.controller.has_message(node, labels, content):
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content['content']):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        service['labels'] = self.controller.has_message(node, labels, content)['labels']
                        if service['freshness_per'] > curTime - service['receiveTime']:
                            service['Fresh'] = True
                            service['Shelf'] = True
                        elif service['shelf_life'] > curTime - service['receiveTime']:
                            service['Fresh'] = False
                            service['Shelf'] = True
                        else:
                            service['Fresh'] = False
                            service['Shelf'] = False
                        service['receiveTime'] = curTime
                        service['service_type'] = "processed"
                    else:
                        service = dict()
                        service['content'] = content
                        service['labels'] = labels
                        service['msg_size'] = 1000000
                        service['Fresh'] = False
                        service['Shelf'] = False
                        service['receiveTime'] = curTime
                        service['service_type'] = "processed"
                    if self.controller.has_message(node, service['labels'], service['content']) and service['msg_size'] == 1000000:
                        self.view.storage_nodes()[node].deleteAnyMessage(service['content'])
                        self.controller.replication_overhead_update(service)
                        self.controller.remove_replication_hops(service)
                        service['msg_size'] = service['msg_size'] / 2
                        all_in = 0
                        if labels:
                            for l in labels:
                                if node in self.view.model.request_labels and l in self.view.model.request_labels[
                                    node]:
                                    all_in += 1
                            if all_in == len(labels):
                                self.controller.add_request_labels_to_storage(node, service, True)
                            else:
                                self.controller.add_storage_labels_to_node(node, service)
                        self.controller.add_message_to_storage(node, service)
                    elif self.controller.has_message(node, service['labels'], service['content']) and service['msg_size'] == 500000:
                        self.view.storage_nodes()[node].deleteAnyMessage(service['content'])
                        self.controller.replication_overhead_update(service)
                        self.controller.remove_replication_hops(service)
                        all_in = 0
                        if labels:
                            for l in labels:
                                if node in self.view.model.request_labels and l in self.view.model.request_labels[
                                    node]:
                                    all_in += 1
                            if all_in == len(labels):
                                self.controller.add_request_labels_to_storage(node, service, True)
                            else:
                                self.controller.add_storage_labels_to_node(node, service)
                        self.controller.add_message_to_storage(node, service)

                    else:
                        if service['msg_size'] == 1000000:
                            self.controller.replication_overhead_update(service)
                            self.controller.remove_replication_hops(service)
                            service['msg_size'] = service['msg_size'] / 2
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_storage_labels_to_node(node, service)

                else:
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                              deadline, rtt_delay, STORE)

                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, RESPONSE)
                if (node != source and curTime + path_delay > deadline):
                    print("Error in HYBRID strategy: Request missed its deadline\nResponse at receiver at time: " + str(
                        curTime + path_delay) + " deadline: " + str(deadline))
                    task.print_task()
                    # raise ValueError("This should not happen: a task missed its deadline after being executed at an edge node.")

            elif status == REQUEST:

                # Processing a request
                source, cache_ret = self.view.closest_source(node, service)

                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                if not cache_ret and self.view.has_cache(source):
                    if self.controller.put_content(source, content['content']):
                        pass
                    else:
                        self.controller.put_content_local_cache(source)
                deadline_metric = (deadline - curTime - rtt_delay - compSpot.services[
                    service['content']].service_time)  # /deadline
                if self.debug:
                    print("Deadline metric: " + repr(deadline_metric))
                if self.view.has_service(node, service) and service["service_type"] is "proc":
                    if self.controller.has_message(node, labels, service):
                        print "ERROR: Tasks should not be admitted in nodes which do not have the data they need to " \
                              "process the request and these nodes should be added as data 'source', but hey-ho"
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content['content']):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        if self.debug:
                            print("Calling admit_task")
                        ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline,
                                                          receiver, rtt_delay, self.controller, self.debug)
                        if self.debug:
                            print("Done Calling admit_task")
                        if ret is False:
                            # Pass the Request upstream
                            rtt_delay += delay * 2
                            # delay += 0.01 # delay associated with content fetch from storage
                            # WE DON'T INCLUDE THIS BECAUSE THE CONTENT MAY BE PRE-FETCHED INTO THE CACHE FROM STORAGE
                            self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                      flow_id, deadline, rtt_delay, REQUEST)
                            if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                            if deadline_metric > 0:
                                self.cand_deadline_metric[node][service['content']] += deadline_metric
                            if self.debug:
                                print("Pass upstream to node: " + repr(next_node))
                            compSpot.missed_requests[service['content']] += 1  # Added here
                        else:
                            if deadline_metric > 0:
                                self.deadline_metric[node][service['content']] += deadline_metric
                    else:
                        source, cache_ret = self.view.closest_source(node, service)

                        path = self.view.shortest_path(node, source)
                        next_node = path[1]
                        delay = self.view.path_delay(node, next_node)
                        if not cache_ret and self.view.has_cache(source):
                            if self.controller.put_content(source, content['content']):
                                pass
                            else:
                                self.controller.put_content_local_cache(source)
                        compSpot.missed_requests[service['content']] += 1
                        if self.debug:
                            print("Not running the service: Pass upstream to node: " + repr(next_node))
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, REQUEST)
                        if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                service['content'], service['labels']):
                            self.controller.add_request_labels_to_node(node, service)
                        if deadline_metric > 0:
                            self.cand_deadline_metric[node][service['content']] += deadline_metric

                else:  # Not running the service
                    compSpot.missed_requests[service['content']] += 1
                    if self.debug:
                        print("Not running the service: Pass upstream to node: " + repr(next_node))
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, REQUEST)
                    if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                            service['content'], service['labels']):
                        self.controller.add_request_labels_to_node(node, service)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service['content']] += deadline_metric
            elif status == STORE:
                pass
            else:
                print("Error: unrecognised status value : " + repr(status))

    def find_closest_feasible_node(self, receiver, flow_id, path, curTime, service, deadline, rtt_delay):
        """
        finds fathest comp. spot to schedule a request using current
        congestion information at each upstream comp. spot.
        The goal is to carry out computations at the farthest node to create space for
        tasks that require closer comp. spots.
        """

        source = self.view.content_source(service, [])[len(self.view.content_source(service, [])) - 1]
        if len(self.view.content_source(service, [])) > 1:
            if source == path[0]:
                self.self_calls[path[0]] += 1
        if self.self_calls[path[0]] >= 3:
            source = self.view.content_source_cloud(service, service['labels'])
            if not source:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[path[0]] is not None:
                        source = n
            self.self_calls[path[0]] = 0

        # start from the upper-most node in the path and check feasibility
        upstream_node = source
        aTask = None
        for n in reversed(path[1:-1]):
            cs = self.compSpots[n]
            if cs.is_cloud:
                continue
            if cs.numberOfVMInstances[service['content']] == 0:
                continue
            if len(cs.scheduler.busyVMs[service['content']]) + len(cs.scheduler.idleVMs[service['content']]) <= 0:
                continue
            delay = self.view.path_delay(receiver, n)
            rtt_to_cs = rtt_delay + 2*delay
            serviceTime = cs.services[service['content']].service_time
            if deadline - curTime - rtt_to_cs < serviceTime:
                continue
            aTask = Task(curTime, Task.TASK_TYPE_SERVICE, deadline, rtt_to_cs, n, service['content'], service['labels'], serviceTime, flow_id, receiver, curTime+delay)
            cs.scheduler.upcomingTaskQueue.append(aTask)
            cs.scheduler.upcomingTaskQueue = sorted(cs.scheduler.upcomingTaskQueue, key=lambda x: x.arrivalTime)
            cs.compute_completion_times(curTime, False, self.debug)
            for task in cs.scheduler._taskQueue + cs.scheduler.upcomingTaskQueue:
                if self.debug:
                    print("After compute_completion_times:")
                    task.print_task()
                if task.taskType == Task.TASK_TYPE_VM_START:
                    continue
                if ( (task.expiry - delay) < task.completionTime ) or ( task.completionTime == float('inf') ):
                    cs.scheduler.upcomingTaskQueue.remove(aTask)
                    if self.debug:
                        print ("Task with flow_id " + str(aTask.flow_id) + " is violating its expiration time at node: " + str(n))
                    break
            else:
                source = n
                #if self.debug:
                #if aTask.flow_id == 1964:
                #    print ("Task is scheduled at node 5:")
                #    aTask.print_task()
                if self.debug:
                    print ("Flow_id: " + str(aTask.flow_id) + " is scheduled to run at " + str(source))
                return source
        return source

    # TODO: UPDATE BELOW WITH COLLECTORS INSTEAD OF PREVIOUS OUTPUT FILES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def updateCloudBW(self, node, period):
        self.cloudBW = self.view.model.repoStorage[node].getDepletedCloudProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedCloudMessagesBW(period)

    def updateUpBW(self, node, period):
        self.cloudBW = self.view.model.repoStorage[node].getDepletedCloudProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedMessagesBW(period)

    def updateDeplBW(self, node, period):
        self.deplBW = self.view.model.repoStorage[node].getDepletedProcMessagesBW(period) + \
                      self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                      self.view.model.repoStorage[node].getDepletedMessagesBW(period)
    def deplCloud(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcessedMessagesSize() +
                self.view.model.repoStorage[node].getStaleMessagesSize() >
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor)):
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

                if not self.view.model.repoStorage[node].isProcessedEmpty():
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.model.repoStorage[node].getOldestDeplUnProcMessage()() is not None:
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")


                elif self.view.model.repoStorage[node].getOldestStaleMessage()() is not None:
                    msg = self.oldestSatisfiedDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                else:
                    self.cloudEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.cloudBW)

                # Revise:
                self.updateCloudBW(node, period)

                # System.out.prln("Depletion is at: " + deplBW)
                self.lastDepl = curTime
                """System.out.prln("self.cloudBW is at " +
                      self.cloudBW +
                      " self.cloud_lim is at " +
                      self.cloud_lim +
                      " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equal " +
                      (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor) +
                      " Total space is " + self.view.model.repoStorage[node].getTotalStorageSpace()) """
            # System.out.prln("Depleted  messages: " + sdepleted)
        elif (self.view.model.repoStorage[node].getProcessedMessagesSize() +
              self.view.model.repoStorage[node].getStaleMessagesSize()) > \
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.max_stor):
            self.cloudEmptyLoop = True
            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                if (self.view.model.repoStorage[node].getOldestStaleMessage() is not None and
                        self.cloudBW < self.cloud_lim):
                    msg = self.oldestSatisfiedDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """
                    * Oldest unprocessed messages should be given priority for depletion
                    * at a certain po.
                    """

                elif (self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None):
                    msg = self.oldestUnProcDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """
                * Oldest processed message is depleted (as a FIFO type of storage,
                * and a  message for processing is processed
                    """
                elif (not self.view.model.repoStorage[node].isProcessedEmpty):
                    msg = self.processedDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateCloudBW(node, period)
                    # System.out.prln("Depletion is at: " + deplBW)
                    self.lastDepl = curTime
                    """System.out.prln("self.cloudBW is at " +
                      self.cloudBW +
                      " self.cloud_lim is at " +
                      self.cloud_lim +
                      " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equal " +
                      (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor) +
                      " Total space is " + self.view.model.repoStorage[node].getTotalStorageSpace()) """
                # System.out.prln("Depleted  messages: " + sdepleted)

    def deplUp(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcessedMessagesSize() >
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor)):

            self.cloudEmptyLoop = True

            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """

                * Oldest processed	message is depleted(as a FIFO type of storage,
                * and a	message for processing is processed

                """
                # TODO: NEED TO add COMPRESSED PROCESSED messages to storage AFTER normal servicing
                if (not self.view.model.repoStorage[node].isProcessedEmpty):
                    msg = self.processedDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None:
                    msg = self.oldestUnProcDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                elif self.view.model.repoStorage[node].getOldestStaleMessage() is not None:
                    msg = self.oldestSatisfiedDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                #
                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateUpBW(node, period)

            # System.out.prln("Depletion is at : "+ deplBW)
            self.lastDepl = curTime
            """System.out.prln("self.cloudBW is at " +
                     self.cloudBW +
                     " self.cloud_lim is at " +
                     self.cloud_lim +
                     " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equa l "+
                     (self.view.model.repoStorage[node].getTotalStorageSpac e *self.min_st or)+
                     " Total space i s  "+self.view.model.repoStorage[node].getTotalStorageSpace()( ) )"""
            # System.out.prln("Depleted  messages : "+ sdepleted)

            self.upEmptyLoop = True
        for i in range(0, 50) and self.cloudBW > self.cloud_lim and \
                 not self.view.model.repoStorage[node].isProcessingEmpty() and self.upEmptyLoop:
            if (not self.view.model.repoStorage[node].isProcessedEmpty):
                self.processedDepletion(node)

            elif (not self.view.model.repoStorage[node].isProcessingEmpty):
                self.view.model.repoStorage[node].deleteAnyMessage(
                    self.view.model.repoStorage[node].getOldestProcessMessage['content'])
            else:
                self.upEmptyLoop = False

    # System.out.prln("Depletion is at: "+ self.cloudBW)

    def deplStorage(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcMessagesSize() +
                self.view.model.repoStorage[node].getMessagesSize() >
                self.view.model.repoStorage[node].getTotalStorageSpace() * self.max_stor):
            self.deplEmptyLoop = True
            for i in range(0, 50) and self.deplBW < self.depl_rate and self.deplEmptyLoop:
                if (self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None):
                    msg = self.oldestUnProcDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                elif (self.view.model.repoStorage[node].getOldestStaleMessage() is not None):
                    msg = self.oldestSatisfiedDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")


                elif (self.view.model.repoStorage[node].getOldestInvalidProcessMessage() is not None):
                    msg = self.oldestInvalidProcDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                elif (self.view.model.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.model.repoStorage[node].getOldestMessage()
                    self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.model.repoStorage[node].addToDeplMessages(ctemp)
                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.model.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                    delay = self.view.path_delay(node, source)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, ctemp, labels, source, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    else:
                        self.view.model.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.model.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.model.repoStorage[node].getNewestProcessMessage()
                    self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.model.repoStorage[node].addToDeplProcMessages(ctemp)
                    source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                    delay = self.view.path_delay(node, source)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, ctemp, labels, source, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    if (storTime <= ctemp['shelfLife'] + 1):
                        ctemp['overtime'] = False

                    elif (storTime > ctemp['shelfLife'] + 1):
                        ctemp['overtime'] = True

                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.model.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                        source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                        delay = self.view.path_delay(node, source)
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, content, labels, source, flow_id, deadline, rtt_delay,
                                                  STORE)
                        if self.debug:
                            print("Message is scheduled to be stored in the CLOUD")
                    else:
                        self.view.model.repoStorage[node].addToDeplMessages(ctemp)
                        source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                        delay = self.view.path_delay(node, source)
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, content, labels, source, flow_id, deadline, rtt_delay,
                                                  STORE)
                        if self.debug:
                            print("Message is scheduled to be stored in the CLOUD")
            else:
                self.deplEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.deplBW)

                self.updateDeplBW(node, period)
                self.updateCloudBW(node, period)
            # Revise:
            self.lastDepl = curTime

    def processedDepletion(self, node):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getOldestFreshMessage() is not None):
            if (self.view.model.repoStorage[node].getOldestFreshMessage().getProperty("procTime") is None):
                temp = self.view.model.repoStorage[node].getOldestFreshMessage()
                report = False
                self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
                """
                * Make sure here that the added message to the cloud depletion 
                * tracking is also tracked by whether it 's Fresh or Stale.
                """
                report = True
                self.view.model.repoStorage[node].addToStoredMessages(temp)
                self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
                return temp



            elif (self.view.model.repoStorage[node].getOldestShelfMessage() is not None):
                if (self.view.model.repoStorage[node].getOldestShelfMessage().getProperty("procTime") is None):
                    temp = self.view.model.repoStorage[node].getOldestShelfMessage()
                    report = False
                    self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
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
                    self.view.model.repoStorage[node].addToStoredMessages(temp)
                    self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.model.repoStorage[node].getOldestStaleMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.model.repoStorage[node].addToCloudDeplMessages(ctemp)

        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.model.repoStorage[node].addToCloudDeplMessages(ctemp)

        self.lastCloudUpload = curTime
        return ctemp

    def oldestInvalidProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.model.repoStorage[node].getOldestInvalidProcessMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(temp['content'])
        if (temp['comp'] is not None):
            if (temp['comp']):
                ctemp = self.compressMessage(node, temp)
                self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                storTime = curTime - ctemp['receiveTime']
                ctemp['storTime'] = storTime
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                self.view.model.repoStorage[node].addToDeplProcMessages(ctemp)

            elif (temp['comp'] is None):
                storTime = curTime - temp['receiveTime']
                temp['storTime'] = storTime
                temp['satisfied'] = False
                if (storTime <= temp['shelfLife'] + 1):
                    temp['overtime'] = False

                elif (storTime > temp['shelfLife'] + 1):
                    temp['overtime'] = True

                self.view.model.repoStorage[node].addToDeplProcMessages(temp)
        return temp

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.model.repoStorage[node].getOldestDeplUnProcMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(temp['content'])
        self.view.model.repoStorage[node].addToDepletedUnProcMessages(temp)
        return temp

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

    def __init__(self, view, controller, replacement_interval=10, debug=False, n_replacements=1,
                 depl_rate=10000000, cloud_lim=20000000, max_stor=9900000000, min_stor=10000000,**kwargs):
        super(HServProStorApp, self).__init__(view, controller)

        self.view.model.strategy = 'HYBRIDS_PRO_REPO_APP'
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
        self.serviceNodeUtil = [None]*len(self.receivers)
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0

        for recv in self.receivers:
            recv = int(recv[4:])
            self.serviceNodeUtil[recv] = [None]*self.num_nodes
            for n in self.compSpots.keys():
                cs = self.compSpots[n]
                if cs.is_cloud:
                    continue
                self.serviceNodeUtil[recv][n] = [0.0]*self.num_services

        # vars

        self.lastDepl = 0

        self.last_period = 0

        self.cloudEmptyLoop = True

        self.deplEmptyLoop = True

        self.upEmptyLoop = True

        self.lastCloudUpload = 0

        self.deplBW = 0

        self.cloudBW = 0

        self.procMinI = 0
        # processedSize = self.procSize * self.proc_ratio
        self.depl_rate = depl_rate
        self.cloud_lim = cloud_lim
        self.max_stor = max_stor
        self.min_stor = min_stor
        self.self_calls = {}
        for node in view.storage_nodes(True):
            self.self_calls[node] = 0
        self.view = view


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
    def replace_services1(self, curTime):
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
                    if cs.numberOfVMInstances[service] > len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                        aVM = VM(self, service)
                        cs.scheduler.idleVMs[service].append(aVM)
                    elif cs.numberOfVMInstances[service] < len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                        cs.numberOfVMInstances[service] = len(cs.scheduler.idleVMs[service]) + len(cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service])

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
                        self.controller.reassign_vm(curTime, cs, service_running, service_missed, self.debug)
                        # cs.reassign_vm(self.controller, curTime, service_running, service_missed, self.debug)
                        if self.debug:
                            print("Missed util: " + str(missed_util) + " running util: " + str(
                                running_util) + " Adequate time missed: " + str(
                                delay[service_missed]) + " Adequate time running: " + str(delay[service_running]))
                        del running_services_utilisation_normalised[indx]
                        n_replacements += 1
                        break
            if self.debug:
                print(str(n_replacements) + " replacements at node:" + str(cs.node) + " at time: " + str(curTime))
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
    #@profile
    def handle(self, curTime, receiver, msg, node, log, feedback, flow_id, rtt_delay, deadline):
        msg['receiveTime'] = time.time()
        if self.view.hasStorageCapability(node) and 'satisfied' not in msg or ('Shelf' not in msg or msg['Shelf']):
            self.controller.add_replication_hops(msg)
            if node in self.view.storage_nodes() and self.view.all_labels_most_requests(msg["labels"]) and self.view.storage_nodes()[node] is self.view.all_labels_most_requests(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_replication_hops(msg)
                self.controller.add_request_labels_to_storage(node, msg['labels'], True)
                # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
            elif node is self.view.all_labels_main_source(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_replication_hops(msg)
                self.controller.add_request_labels_to_storage(node, msg['labels'], True)
                # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
            else:
                edr = self.view.all_labels_most_requests(msg["labels"])
                if edr and node != edr.node:
                    self.controller.add_request_labels_to_node(node, msg)
                    # if edr and edr.hasMessage(msg['content'], msg['labels']):
                    #     msg = edr.hasMessage(msg['content'], msg['labels'])
                    # else:
                    #     msg['shelf_life'] = deadline - curTime
                    path = self.view.shortest_path(node, edr.node)
                    if len(path) <= 1 or not path:
                        self.controller.add_message_to_storage(edr, msg)
                        self.controller.add_replication_hops(msg)
                        self.controller.add_request_labels_to_storage(edr, msg['labels'], True)
                    else:
                        next_node = path[1]
                        delay = self.view.path_delay(node, next_node)
                        self.controller.add_request_labels_to_node(node, msg)

                        self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                              curTime + msg['shelf_life'], rtt_delay, STORE)
                        # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)
                    self.controller.replicate(node, next_node)

        else:
            msg['overtime'] = False
            source = self.view.content_source_cloud(msg, msg['labels'])
            if not source:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                        source = n
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay * 2
            self.controller.add_event(curTime + delay, receiver, msg, msg['labels'], next_node, flow_id, deadline, rtt_delay,
                                      STORE)
            # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)

        return msg

    @inheritdoc(Strategy)
    def process_event(self, curTime, receiver, content, log, labels, node, flow_id, deadline, rtt_delay, status,
                      task=None):
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

        if time.time() - self.last_period >= 1:
            self.last_period = time.time()
            period = True
        else:
            period = False

        if self.view.hasStorageCapability(node):

            self.updateCloudBW(node, period)
            self.deplCloud(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)
            self.updateDeplBW(node, period)
            self.deplStorage(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)

        elif not self.view.hasStorageCapability(node) and self.view.has_computationalSpot(node):
            self.updateUpBW(node, period)
            self.deplUp(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)

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
        if type(content) is dict and 'shelf_life' in content and 'max_replications' in content:
            if content["shelf_life"] and content['replications'] <= content['max_replications']:
                source, in_cache = self.view.closest_source(node, content)
                path = self.view.shortest_path(node, source)
                self.handle(curTime, receiver, content, node, log, feedback, flow_id, rtt_delay, deadline)
        elif type(content) is dict and 'shelf_life' in content and content["shelf_life"] :
            source, in_cache = self.view.closest_source(node, content)
            path = self.view.shortest_path(node, source)
            self.handle(curTime, receiver, content, node, log, feedback, flow_id, rtt_delay, deadline)
        service = content

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

        source, in_cache = self.view.closest_source(node, service)
        cloud_source = self.view.content_source_cloud(service, labels)
        if not cloud_source:
            cloud_source, in_cache = self.view.closest_source(node, service)
            if cloud_source == node:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                        node] is not None:
                        cloud_source = n

        path = self.view.shortest_path(node, cloud_source)

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
            if cloud_source == node and status == REQUEST:

                cache_delay = 0
                if not in_cache and self.view.has_cache(node):
                    if self.controller.put_content(source, content['content']):
                        cache_delay = 0.005
                    else:
                        self.controller.put_content_local_cache(source)
                        cache_delay = 0.005
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                elif in_cache:
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                # for n in range(0, 3):
                #     self.controller.add_replication_hops(content)
                ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver,
                                                  rtt_delay + cache_delay, self.controller, self.debug)

                if ret == False:
                    if type(node) == str and "src" in node:
                        print("This should not happen in Hybrid.")
                        raise ValueError("Task should not be rejected at the cloud.")
                    else:
                        # request is to be executed in the cloud and returned to receiver
                        if type(service) is dict and self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                            self.controller.add_request_labels_to_node(node, service)
                        services = self.view.services()
                        serviceTime = services[service['content']].service_time
                        self.controller.add_event(curTime + rtt_delay + serviceTime, receiver, service, labels,
                                                  receiver, flow_id, deadline, rtt_delay, RESPONSE)
                        if self.debug:
                            print("Request is scheduled to run at the CLOUD")
                    for n in path[1:]:
                        cs = self.view.compSpot(n)
                        if cs.is_cloud:
                            continue
                        self.serviceNodeUtil[int(receiver[4:])][n][service['content']] += self.view.services()[
                            service['content']].service_time
                    return
                return

            if source == node and status == REQUEST:
                cache_delay = 0
                if not in_cache and self.view.has_cache(node):
                    if self.controller.put_content(source, content['content']):
                        cache_delay = 0.005
                    else:
                        self.controller.put_content_local_cache(source)
                        cache_delay = 0.005
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                elif in_cache:
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return

                if compSpot is not None and self.view.has_service(node, service) and service["service_type"] is "proc":
                    ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver,
                                                      rtt_delay + cache_delay, self.controller, self.debug)

                    if ret == False:

                        if type(service) is dict and self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                        source = self.view.content_source_cloud(service, labels)
                        if not source:
                            source, in_cache = self.view.closest_source(node, service)
                            if source == node:
                                for n in self.view.model.comp_size:
                                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                        node] is not None:
                                        source = n
                        path = self.view.shortest_path(node, source)
                        upstream_node = self.find_closest_feasible_node(receiver, flow_id, path, curTime, service, deadline, rtt_delay)
                        delay = self.view.path_delay(node, source)
                        # self.controller.add_event(curTime + delay, receiver, service, labels, upstream_node, flow_id,
                        #                           deadline, rtt_delay, STORE)
                        rtt_delay += delay * 2
                        if upstream_node != source:
                            self.controller.add_event(curTime + delay, receiver, content, labels, upstream_node,
                                                      flow_id, deadline, rtt_delay, REQUEST)
                            if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                            if self.debug:
                                print("Message is scheduled to run at: " + str(upstream_node))
                        else:  # request is to be executed in the cloud and returned to receiver
                            services = self.view.services()
                            serviceTime = services[service['content']].service_time
                            self.controller.add_event(curTime + rtt_delay + serviceTime, receiver, service, labels,
                                                      receiver, flow_id, deadline, rtt_delay, RESPONSE)
                            if self.debug:
                                print("Request is scheduled to run at the CLOUD")
                    return
                return

            #  Request at the receiver
            if receiver == node and status == REQUEST:

                self.controller.start_session(curTime, receiver, service, labels, log, flow_id, deadline)
                path = self.view.shortest_path(node, cloud_source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                rtt_delay += delay * 2
                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, REQUEST)
                if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                        service['content'], service['labels']):
                    self.controller.add_request_labels_to_node(node, service)
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
                        if type(content) is dict:
                            compSpot.missed_requests[content['content']] += 1
                        else:
                            compSpot.missed_requests[content] += 1

            elif status == TASK_COMPLETE:
                self.controller.complete_task(task, curTime)
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
                    source, in_cache = self.view.closest_source(node, service)
                    if type(service) is not dict and self.controller.has_message(node, labels, content):
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content)
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content)
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        service = self.view.storage_nodes()[node].hasMessage(content, labels)
                    elif type(service) is dict and self.controller.has_message(node, labels, content):
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content['content']):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        service['labels'] = self.controller.has_message(node, labels, content)['labels']
                        if service['freshness_per'] > curTime - service['receiveTime']:
                            service['Fresh'] = True
                            service['Shelf'] = True
                        elif service['shelf_life'] > curTime - service['receiveTime']:
                            service['Fresh'] = False
                            service['Shelf'] = True
                        else:
                            service['Fresh'] = False
                            service['Shelf'] = False
                        service['receiveTime'] = curTime
                        service['service_type'] = "processed"
                    else:
                        service = dict()
                        service['content'] = content
                        service['labels'] = labels
                        service['msg_size'] = 1000000
                        service['Fresh'] = False
                        service['Shelf'] = False
                        service['receiveTime'] = curTime
                        service['service_type'] = "processed"
                    if self.controller.has_message(node, service['labels'], service['content']) and service['msg_size'] == 1000000:
                        self.view.storage_nodes()[node].deleteAnyMessage(service['content'])
                        self.controller.replication_overhead_update(service)
                        self.controller.remove_replication_hops(service)
                        service['msg_size'] = service['msg_size'] / 2
                        all_in = 0
                        if labels:
                            for l in labels:
                                if node in self.view.model.request_labels and l in self.view.model.request_labels[
                                    node]:
                                    all_in += 1
                            if all_in == len(labels):
                                self.controller.add_request_labels_to_storage(node, service, True)
                            else:
                                self.controller.add_storage_labels_to_node(node, service)
                        self.controller.add_message_to_storage(node, service)
                    elif self.controller.has_message(node, service['labels'], service['content']) and service['msg_size'] == 500000:
                        self.view.storage_nodes()[node].deleteAnyMessage(service['content'])
                        self.controller.replication_overhead_update(service)
                        self.controller.remove_replication_hops(service)
                        all_in = 0
                        if labels:
                            for l in labels:
                                if node in self.view.model.request_labels and l in self.view.model.request_labels[
                                    node]:
                                    all_in += 1
                            if all_in == len(labels):
                                self.controller.add_request_labels_to_storage(node, service, True)
                            else:
                                self.controller.add_storage_labels_to_node(node, service)
                        self.controller.add_message_to_storage(node, service)
                    else:
                        if service['msg_size'] == 1000000:
                            self.controller.replication_overhead_update(service)
                            self.controller.remove_replication_hops(service)
                            service['msg_size'] = service['msg_size'] / 2
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_storage_labels_to_node(node, service)

                else:
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                              deadline, rtt_delay, STORE)

                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, RESPONSE)
                if (node != source and curTime + path_delay > deadline):
                    print("Error in HYBRID strategy: Request missed its deadline\nResponse at receiver at time: " + str(
                        curTime + path_delay) + " deadline: " + str(deadline))
                    task.print_task()
                    # raise ValueError("This should not happen: a task missed its deadline after being executed at an edge node.")

            elif status == REQUEST:

                # Processing a request
                source, cache_ret = self.view.closest_source(node, service)

                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                if not cache_ret and self.view.has_cache(source):
                    if self.controller.put_content(source, content['content']):
                        pass
                    else:
                        self.controller.put_content_local_cache(source)
                deadline_metric = (deadline - curTime - rtt_delay - compSpot.services[
                    service['content']].service_time)  # /deadline
                if self.debug:
                    print("Deadline metric: " + repr(deadline_metric))
                if self.view.has_service(node, service) and service["service_type"] is "proc":
                    if self.controller.has_message(node, labels, service):
                        print "ERROR: Tasks should not be admitted in nodes which do not have the data they need to " \
                              "process the request and these nodes should be added as data 'source', but hey-ho"
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content['content']):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        if self.debug:
                            print("Calling admit_task")
                        ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline,
                                                          receiver, rtt_delay, self.controller, self.debug)
                        if self.debug:
                            print("Done Calling admit_task")
                        if ret is False:
                            # Pass the Request upstream
                            rtt_delay += delay * 2
                            # delay += 0.01 # delay associated with content fetch from storage
                            # WE DON'T INCLUDE THIS BECAUSE THE CONTENT MAY BE PRE-FETCHED INTO THE CACHE FROM STORAGE
                            self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                      flow_id, deadline, rtt_delay, REQUEST)
                            if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                            if deadline_metric > 0:
                                self.cand_deadline_metric[node][service['content']] += deadline_metric
                            if self.debug:
                                print("Pass upstream to node: " + repr(next_node))
                            compSpot.missed_requests[service['content']] += 1  # Added here
                        else:
                            if deadline_metric > 0:
                                self.deadline_metric[node][service['content']] += deadline_metric
                    else:
                        source, cache_ret = self.view.closest_source(node, service)

                        path = self.view.shortest_path(node, source)
                        next_node = path[1]
                        delay = self.view.path_delay(node, next_node)
                        if not cache_ret and self.view.has_cache(source):
                            if self.controller.put_content(source, content['content']):
                                pass
                            else:
                                self.controller.put_content_local_cache(source)
                        compSpot.missed_requests[service['content']] += 1
                        if self.debug:
                            print("Not running the service: Pass upstream to node: " + repr(next_node))
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, REQUEST)
                        if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                service['content'], service['labels']):
                            self.controller.add_request_labels_to_node(node, service)
                        if deadline_metric > 0:
                            self.cand_deadline_metric[node][service['content']] += deadline_metric

                else:  # Not running the service
                    compSpot.missed_requests[service['content']] += 1
                    if self.debug:
                        print("Not running the service: Pass upstream to node: " + repr(next_node))
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, REQUEST)
                    if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                            service['content'], service['labels']):
                        self.controller.add_request_labels_to_node(node, service)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service['content']] += deadline_metric
            elif status == STORE:
                pass
            else:
                print("Error: unrecognised status value : " + repr(status))

    def find_closest_feasible_node(self, receiver, flow_id, path, curTime, service, deadline, rtt_delay):
        """
        finds fathest comp. spot to schedule a request using current
        congestion information at each upstream comp. spot.
        The goal is to carry out computations at the farthest node to create space for
        tasks that require closer comp. spots.
        """

        source = self.view.content_source(service, [])[len(self.view.content_source(service, [])) - 1]
        if len(self.view.content_source(service, [])) > 1:
            if source == path[0]:
                self.self_calls[path[0]] += 1
        if self.self_calls[path[0]] >= 3:
            source = self.view.content_source_cloud(service, service['labels'])
            if not source:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[path[0]] is not None:
                        source = n
            self.self_calls[path[0]] = 0
        # start from the upper-most node in the path and check feasibility
        upstream_node = source


        for n in reversed(path[1:-1]):
            cs = self.compSpots[n]
            if cs.is_cloud:
                continue
            if cs.numberOfVMInstances[service['content']] == 0:
                continue
            if len(cs.scheduler.busyVMs[service['content']]) + len(cs.scheduler.idleVMs[service['content']]) <= 0:
                continue
            delay = self.view.path_delay(receiver, n)
            rtt_to_cs = rtt_delay + 2 * delay
            serviceTime = cs.services[service['content']].service_time
            if deadline - curTime - rtt_to_cs < serviceTime:
                continue
            aTask = Task(curTime, Task.TASK_TYPE_SERVICE, deadline, rtt_to_cs, n, service['content'], service['labels'],
                         serviceTime, flow_id, receiver, curTime + delay)
            cs.scheduler.upcomingTaskQueue.append(aTask)
            cs.scheduler.upcomingTaskQueue = sorted(cs.scheduler.upcomingTaskQueue, key=lambda x: x.arrivalTime)
            cs.compute_completion_times(curTime, False, self.debug)
            for task in cs.scheduler._taskQueue + cs.scheduler.upcomingTaskQueue:
                if self.debug:
                    print("After compute_completion_times:")
                    task.print_task()
                if task.taskType == Task.TASK_TYPE_VM_START:
                    continue
                if ((task.expiry - delay) < task.completionTime) or (task.completionTime == float('inf')):
                    cs.scheduler.upcomingTaskQueue.remove(aTask)
                    if self.debug:
                        print ("Task with flow_id " + str(
                            aTask.flow_id) + " is violating its expiration time at node: " + str(n))
                    break
            else:
                source = n
                # if self.debug:
                # if aTask.flow_id == 1964:
                #    print ("Task is scheduled at node 5:")
                #    aTask.print_task()
                if self.debug:
                    print ("Flow_id: " + str(aTask.flow_id) + " is scheduled to run at " + str(source))
                return source
        return source

    # TODO: UPDATE BELOW WITH COLLECTORS INSTEAD OF PREVIOUS OUTPUT FILES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def updateCloudBW(self, node, period):
        self.cloudBW = self.view.model.repoStorage[node].getDepletedCloudProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedCloudMessagesBW(period)

    def updateUpBW(self, node, period):
        self.cloudBW = self.view.model.repoStorage[node].getDepletedCloudProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedMessagesBW(period)

    def updateDeplBW(self, node, period):
        self.deplBW = self.view.model.repoStorage[node].getDepletedProcMessagesBW(period) + \
                      self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                      self.view.model.repoStorage[node].getDepletedMessagesBW(period)

    def deplCloud(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcessedMessagesSize() +
                self.view.model.repoStorage[node].getStaleMessagesSize() >
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor)):
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

                if not self.view.model.repoStorage[node].isProcessedEmpty():
                    msg = self.processedDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.model.repoStorage[node].getOldestDeplUnProcMessage()() is not None:
                    msg = self.oldestUnProcDepletion(node)

                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")


                elif self.view.model.repoStorage[node].getOldestStaleMessage()() is not None:
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                else:
                    self.cloudEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.cloudBW)

                # Revise:
                self.updateCloudBW(node, period)

                # System.out.prln("Depletion is at: " + deplBW)
                self.lastDepl = curTime
                """System.out.prln("self.cloudBW is at " +
                      self.cloudBW +
                      " self.cloud_lim is at " +
                      self.cloud_lim +
                      " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equal " +
                      (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor) +
                      " Total space is " + self.view.model.repoStorage[node].getTotalStorageSpace()) """
            # System.out.prln("Depleted  messages: " + sdepleted)
        elif (self.view.model.repoStorage[node].getProcessedMessagesSize() +
                self.view.model.repoStorage[node].getStaleMessagesSize()) > \
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.max_stor):
            self.cloudEmptyLoop = True
            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                if (self.view.model.repoStorage[node].getOldestStaleMessage() is not None and
                        self.cloudBW < self.cloud_lim):
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """
                    * Oldest unprocessed messages should be given priority for depletion
                    * at a certain po.
                    """

                elif (self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None):
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """
                * Oldest processed message is depleted (as a FIFO type of storage,
                * and a  message for processing is processed
                    """
                elif (not self.view.model.repoStorage[node].isProcessedEmpty):
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateCloudBW(node, period)
                    # System.out.prln("Depletion is at: " + deplBW)
                    self.lastDepl = curTime
                    """System.out.prln("self.cloudBW is at " +
                      self.cloudBW +
                      " self.cloud_lim is at " +
                      self.cloud_lim +
                      " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equal " +
                      (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor) +
                      " Total space is " + self.view.model.repoStorage[node].getTotalStorageSpace()) """
                # System.out.prln("Depleted  messages: " + sdepleted)

    def deplUp(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcessedMessagesSize() >
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor)):

            self.cloudEmptyLoop = True

            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """

                * Oldest processed	message is depleted(as a FIFO type of storage,
                * and a	message for processing is processed

                """
                # TODO: NEED TO add COMPRESSED PROCESSED messages to storage AFTER normal servicing
                if (not self.view.model.repoStorage[node].isProcessedEmpty):
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None:
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                elif self.view.model.repoStorage[node].getOldestStaleMessage() is not None:
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                #
                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateUpBW(node, period)

            # System.out.prln("Depletion is at : "+ deplBW)
            self.lastDepl = curTime
            """System.out.prln("self.cloudBW is at " +
                     self.cloudBW +
                     " self.cloud_lim is at " +
                     self.cloud_lim +
                     " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equa l "+
                     (self.view.model.repoStorage[node].getTotalStorageSpac e *self.min_st or)+
                     " Total space i s  "+self.view.model.repoStorage[node].getTotalStorageSpace()( ) )"""
            # System.out.prln("Depleted  messages : "+ sdepleted)

            self.upEmptyLoop = True
        for i in range(0, 50) and self.cloudBW > self.cloud_lim and \
                 not self.view.model.repoStorage[node].isProcessingEmpty() and self.upEmptyLoop:
            if (not self.view.model.repoStorage[node].isProcessedEmpty):
                self.processedDepletion(node)

            elif (not self.view.model.repoStorage[node].isProcessingEmpty):
                self.view.model.repoStorage[node].deleteAnyMessage(self.view.model.repoStorage[node].getOldestProcessMessage['content'])
            else:
                self.upEmptyLoop = False

    # System.out.prln("Depletion is at: "+ self.cloudBW)

    def deplStorage(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcMessagesSize() +
                self.view.model.repoStorage[node].getMessagesSize() >
                self.view.model.repoStorage[node].getTotalStorageSpace() * self.max_stor):
            self.deplEmptyLoop = True
            for i in range(0, 50) and self.deplBW < self.depl_rate and self.deplEmptyLoop:
                if (self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None):
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                elif (self.view.model.repoStorage[node].getOldestStaleMessage() is not None):
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")


                elif (self.view.model.repoStorage[node].getOldestInvalidProcessMessage() is not None):
                    msg = self.oldestInvalidProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                elif (self.view.model.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.model.repoStorage[node].getOldestMessage()
                    self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.model.repoStorage[node].addToDeplMessages(ctemp)
                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.model.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                    if not source:
                        source, in_cache = self.view.closest_source(node, content)
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                    delay = self.view.path_delay(node, source)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, ctemp, labels, source, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    else:
                        self.view.model.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.model.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.model.repoStorage[node].getNewestProcessMessage()
                    self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.model.repoStorage[node].addToDeplProcMessages(ctemp)
                    source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                    if not source:
                        source, in_cache = self.view.closest_source(node, content)
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                    delay = self.view.path_delay(node, source)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, ctemp, labels, source, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    if (storTime <= ctemp['shelfLife'] + 1):
                        ctemp['overtime'] = False

                    elif (storTime > ctemp['shelfLife'] + 1):
                        ctemp['overtime'] = True

                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.model.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                        source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                        delay = self.view.path_delay(node, source)
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, content, labels, source, flow_id, deadline, rtt_delay,
                                                  STORE)
                        if self.debug:
                            print("Message is scheduled to be stored in the CLOUD")
                    else:
                        self.view.model.repoStorage[node].addToDeplMessages(ctemp)
                        source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                        delay = self.view.path_delay(node, source)
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, content, labels, source, flow_id, deadline, rtt_delay,
                                                  STORE)
                        if self.debug:
                            print("Message is scheduled to be stored in the CLOUD")
            else:
                self.deplEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.deplBW)

                self.updateDeplBW(node, period)
                self.updateCloudBW(node, period)
            # Revise:
            self.lastDepl = curTime

    def processedDepletion(self, node):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getOldestFreshMessage() is not None):
            if (self.view.model.repoStorage[node].getOldestFreshMessage().getProperty("procTime") is None):
                temp = self.view.model.repoStorage[node].getOldestFreshMessage()
                report = False
                self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
                """
                * Make sure here that the added message to the cloud depletion 
                * tracking is also tracked by whether it 's Fresh or Stale.
                """
                report = True
                self.view.model.repoStorage[node].addToStoredMessages(temp)
                self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
                return temp



            elif (self.view.model.repoStorage[node].getOldestShelfMessage() is not None):
                if (self.view.model.repoStorage[node].getOldestShelfMessage().getProperty("procTime") is None):
                    temp = self.view.model.repoStorage[node].getOldestShelfMessage()
                    report = False
                    self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
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
                    self.view.model.repoStorage[node].addToStoredMessages(temp)
                    self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.model.repoStorage[node].getOldestStaleMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.model.repoStorage[node].addToCloudDeplMessages(ctemp)

        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.model.repoStorage[node].addToCloudDeplMessages(ctemp)

        self.lastCloudUpload = curTime
        return ctemp

    def oldestInvalidProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.model.repoStorage[node].getOldestInvalidProcessMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(temp['content'])
        if (temp['comp'] is not None):
            if (temp['comp']):
                ctemp = self.compressMessage(node, temp)
                self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                storTime = curTime - ctemp['receiveTime']
                ctemp['storTime'] = storTime
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                self.view.model.repoStorage[node].addToDeplProcMessages(ctemp)

            elif (temp['comp'] is None):
                storTime = curTime - temp['receiveTime']
                temp['storTime'] = storTime
                temp['satisfied'] = False
                if (storTime <= temp['shelfLife'] + 1):
                    temp['overtime'] = False

                elif (storTime > temp['shelfLife'] + 1):
                    temp['overtime'] = True

                self.view.model.repoStorage[node].addToDeplProcMessages(temp)

            return temp

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.model.repoStorage[node].getOldestDeplUnProcMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(temp['content'])
        self.view.model.repoStorage[node].addToDepletedUnProcMessages(temp)
        return temp

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

    def __init__(self, view, controller, replacement_interval=10, debug=False, n_replacements=1,
                 depl_rate=10000000, cloud_lim=20000000, max_stor=9900000000, min_stor=10000000,**kwargs):
        super(HServReStorApp, self).__init__(view, controller)

        self.view.model.strategy = 'HYBRIDS_RE_REPO_APP'
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
        self.serviceNodeUtil = [None]*len(self.receivers)
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0

        for recv in self.receivers:
            recv = int(recv[4:])
            self.serviceNodeUtil[recv] = [None]*self.num_nodes
            for n in self.compSpots.keys():
                cs = self.compSpots[n]
                if cs.is_cloud:
                    continue
                self.serviceNodeUtil[recv][n] = [0.0]*self.num_services

        # vars

        self.lastDepl = 0

        self.last_period = 0

        self.cloudEmptyLoop = True

        self.deplEmptyLoop = True

        self.upEmptyLoop = True

        self.lastCloudUpload = 0

        self.deplBW = 0

        self.cloudBW = 0

        self.procMinI = 0
        # processedSize = self.procSize * self.proc_ratio
        self.depl_rate = depl_rate
        self.cloud_lim = cloud_lim
        self.max_stor = max_stor
        self.min_stor = min_stor
        self.self_calls = {}
        for node in view.storage_nodes(True):
            self.self_calls[node] = 0
        self.view = view


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
    def replace_services1(self, curTime):
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
                    if cs.numberOfVMInstances[service] > len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                        aVM = VM(self, service)
                        cs.scheduler.idleVMs[service].append(aVM)
                    elif cs.numberOfVMInstances[service] < len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                        cs.numberOfVMInstances[service] = len(cs.scheduler.idleVMs[service]) + len(cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service])

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
                        self.controller.reassign_vm(curTime, cs, service_running, service_missed, self.debug)
                        # cs.reassign_vm(self.controller, curTime, service_running, service_missed, self.debug)
                        if self.debug:
                            print("Missed util: " + str(missed_util) + " running util: " + str(
                                running_util) + " Adequate time missed: " + str(
                                delay[service_missed]) + " Adequate time running: " + str(delay[service_running]))
                        del running_services_utilisation_normalised[indx]
                        n_replacements += 1
                        break
            if self.debug:
                print(str(n_replacements) + " replacements at node:" + str(cs.node) + " at time: " + str(curTime))
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


    def replicate(self, ProcApplication):
        return ProcApplication(self)

    # TODO: ADAPT FOR SERVICE-PROXIMATE PLACEMENT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def handle(self, curTime, receiver, msg, node, path, log, feedback, flow_id, rtt_delay, deadline):
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
        if self.view.hasStorageCapability(node) and 'satisfied' not in msg or ('Shelf' not in msg or msg['Shelf']):
            self.controller.add_replication_hops(msg)
            # TODO: Check usages of IS OPERATOR throughout code!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            on_path = True
            in_path, node_c = self.view.service_labels_closest_repo(msg['labels'], node, path, on_path)
            on_path = False
            off_path, node_s = self.view.service_labels_closest_repo(msg['labels'], node, path, on_path)
            if node == node_c and in_path is True:
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_replication_hops(msg)
                self.controller.add_storage_labels_to_node(node, msg)
                # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
                if self.controller.has_request_labels(node, msg['labels']):
                    self.controller.add_request_labels_to_storage(node, msg['labels'], False)
            elif self.view.all_labels_most_requests(msg["labels"]) and node is self.view.all_labels_most_requests(msg["labels"]).node:
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_replication_hops(msg)
                self.controller.add_request_labels_to_storage(node, msg['labels'], True)
                # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
            elif node == node_s and not off_path:
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_replication_hops(msg)
                self.controller.add_storage_labels_to_node(node, msg)
                # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
                if self.controller.has_request_labels(node, msg['labels']):
                    self.controller.add_request_labels_to_storage(node, msg['labels'], False)
            elif in_path:
                edr = node_c
                self.controller.add_request_labels_to_node(node, msg)
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                              curTime + msg['shelf_life'], rtt_delay, STORE)
                # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)
                self.controller.replicate(node, edr)
            elif node_s and not off_path:
                edr = node_s
                self.controller.add_request_labels_to_node(node, msg)
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                              curTime + msg['shelf_life'], rtt_delay, STORE)
                # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)
                self.controller.replicate(node, edr)

            else:
                edr = self.view.all_labels_most_requests(msg["labels"])
                if edr and node == edr:
                    self.controller.add_message_to_storage(node, msg)
                    self.controller.add_replication_hops(msg)
                    self.controller.add_request_labels_to_storage(node, msg['labels'], True)
                    # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
                elif edr:
                    self.controller.add_request_labels_to_node(node, msg)
                    # if edr and edr.hasMessage(msg['content'], msg['labels']):
                    #     msg = edr.hasMessage(msg['content'], msg['labels'])
                    # else:
                    #     msg['shelf_life'] = deadline - curTime
                    path = self.view.shortest_path(node, edr.node)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)

                    self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                              curTime + msg['shelf_life'], rtt_delay, STORE)
                    # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)
                    self.controller.replicate(node, next_node)

        else:
            msg['overtime'] = False
            source = self.view.content_source_cloud(msg, msg['labels'])
            if not source:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                        source = n
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay * 2
            self.controller.add_event(curTime + delay, receiver, msg, msg['labels'], next_node, flow_id, deadline, rtt_delay,
                                      STORE)
            # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)

        return msg

    @inheritdoc(Strategy)
    def process_event(self, curTime, receiver, content, log, labels, node, flow_id, deadline, rtt_delay, status,
                      task=None):
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

        if time.time() - self.last_period >= 1:
            self.last_period = time.time()
            period = True
        else:
            period = False

        if self.view.hasStorageCapability(node):

            self.updateCloudBW(node, period)
            self.deplCloud(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)
            self.updateDeplBW(node, period)
            self.deplStorage(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)

        elif not self.view.hasStorageCapability(node) and self.view.has_computationalSpot(node):
            self.updateUpBW(node, period)
            self.deplUp(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)

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
        if type(content) is dict and 'shelf_life' in content and 'max_replications' in content:
            if content["shelf_life"] and content['replications'] <= content['max_replications']:
                source, in_cache = self.view.closest_source(node, content)
                path = self.view.shortest_path(node, source)
                self.handle(curTime, receiver, content, node, path, log, feedback, flow_id, rtt_delay, deadline)
        elif type(content) is dict and 'shelf_life' in content and content["shelf_life"] :
            source, in_cache = self.view.closest_source(node, content)
            path = self.view.shortest_path(node, source)
            self.handle(curTime, receiver, content, node, path, log, feedback, flow_id, rtt_delay, deadline)

        service = content

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

        source, in_cache = self.view.closest_source(node, service)
        cloud_source = self.view.content_source_cloud(service, labels)
        if not cloud_source:
            cloud_source, in_cache = self.view.closest_source(node, service)
            if cloud_source == node:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                        node] is not None:
                        cloud_source = n

        path = self.view.shortest_path(node, cloud_source)

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
            if cloud_source == node and status == REQUEST:

                cache_delay = 0
                if not in_cache and self.view.has_cache(node):
                    if self.controller.put_content(source, content['content']):
                        cache_delay = 0.005
                    else:
                        self.controller.put_content_local_cache(source)
                        cache_delay = 0.005
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                elif in_cache:
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                # for n in range(0, 3):
                #     self.controller.add_replication_hops(content)
                ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver,
                                                  rtt_delay + cache_delay, self.controller, self.debug)

                if ret == False:
                    if type(node) == str and "src" in node:
                        print("This should not happen in Hybrid.")
                        raise ValueError("Task should not be rejected at the cloud.")
                    else:
                        # request is to be executed in the cloud and returned to receiver
                        if type(service) is dict and self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                        services = self.view.services()
                        serviceTime = services[service['content']].service_time
                        self.controller.add_event(curTime + rtt_delay + serviceTime, receiver, service, labels,
                                                  receiver, flow_id, deadline, rtt_delay, RESPONSE)
                        if self.debug:
                            print("Request is scheduled to run at the CLOUD")
                        for n in path[1:]:
                            cs = self.view.compSpot(n)
                            if cs.is_cloud:
                                continue
                            self.serviceNodeUtil[int(receiver[4:])][n][service['content']] += self.view.services()[
                                service['content']].service_time
                    return
                return

            if source == node and status == REQUEST:
                cache_delay = 0
                if not in_cache and self.view.has_cache(node):
                    if self.controller.put_content(source, content['content']):
                        cache_delay = 0.005
                    else:
                        self.controller.put_content_local_cache(source)
                        cache_delay = 0.005
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                elif in_cache:
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                if compSpot is not None and self.view.has_service(node, service) and service["service_type"] is "proc":
                    ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver,
                                                      rtt_delay + cache_delay, self.controller, self.debug)

                    if ret == False:

                        if type(service) is dict and self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                        source = self.view.content_source_cloud(service, labels)
                        if not source:
                            source, in_cache = self.view.closest_source(node, service)
                            if source == node:
                                for n in self.view.model.comp_size:
                                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                        node] is not None:
                                        source = n
                        path = self.view.shortest_path(node, source)
                        upstream_node = self.find_closest_feasible_node(receiver, flow_id, path, curTime, service, deadline, rtt_delay)
                        delay = self.view.path_delay(node, source)
                        # self.controller.add_event(curTime + delay, receiver, service, labels, upstream_node, flow_id,
                        #                           deadline, rtt_delay, STORE)
                        rtt_delay += delay * 2
                        if upstream_node != source:
                            self.controller.add_event(curTime + delay, receiver, content, labels, upstream_node,
                                                      flow_id, deadline, rtt_delay, REQUEST)
                            if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                            if self.debug:
                                print("Message is scheduled to run at: " + str(upstream_node))
                        else:  # request is to be executed in the cloud and returned to receiver
                            services = self.view.services()
                            serviceTime = services[service['content']].service_time
                            self.controller.add_event(curTime + rtt_delay + serviceTime, receiver, service, labels,
                                                      receiver, flow_id, deadline, rtt_delay, RESPONSE)
                            if self.debug:
                                print("Request is scheduled to run at the CLOUD")
                    return
                return

            #  Request at the receiver
            if receiver == node and status == REQUEST:

                self.controller.start_session(curTime, receiver, service, labels, log, flow_id, deadline)
                path = self.view.shortest_path(node, cloud_source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                rtt_delay += delay * 2
                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, REQUEST)
                if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                        service['content'], service['labels']):
                    self.controller.add_request_labels_to_node(node, service)
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
                        if type(content) is dict:
                            compSpot.missed_requests[content['content']] += 1
                        else:
                            compSpot.missed_requests[content] += 1

            elif status == TASK_COMPLETE:
                self.controller.complete_task(task, curTime)
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
                    source, in_cache = self.view.closest_source(node, service)
                    if type(service) is not dict and self.controller.has_message(node, labels, content):
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content)
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content)
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        service = self.view.storage_nodes()[node].hasMessage(content, labels)
                    elif type(service) is dict and self.controller.has_message(node, labels, content):
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content['content']):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        service['labels'] = self.controller.has_message(node, labels, content)['labels']
                        if service['freshness_per'] > curTime - service['receiveTime']:
                            service['Fresh'] = True
                            service['Shelf'] = True
                        elif service['shelf_life'] > curTime - service['receiveTime']:
                            service['Fresh'] = False
                            service['Shelf'] = True
                        else:
                            service['Fresh'] = False
                            service['Shelf'] = False
                        service['receiveTime'] = curTime
                        service['service_type'] = "processed"
                    else:
                        service = dict()
                        service['content'] = content
                        service['labels'] = labels
                        service['msg_size'] = 1000000
                        service['Fresh'] = False
                        service['Shelf'] = False
                        service['receiveTime'] = curTime
                        service['service_type'] = "processed"
                    if self.controller.has_message(node, service['labels'], service['content']) and service['msg_size'] == 1000000:
                        self.view.storage_nodes()[node].deleteAnyMessage(service['content'])
                        self.controller.replication_overhead_update(service)
                        self.controller.remove_replication_hops(service)
                        service['msg_size'] = service['msg_size'] / 2
                        all_in = 0
                        if labels:
                            for l in labels:
                                if node in self.view.model.request_labels and l in self.view.model.request_labels[
                                    node]:
                                    all_in += 1
                            if all_in == len(labels):
                                self.controller.add_request_labels_to_storage(node, service, True)
                            else:
                                self.controller.add_storage_labels_to_node(node, service)
                        self.controller.add_message_to_storage(node, service)
                    elif self.controller.has_message(node, service['labels'], service['content']) and service['msg_size'] == 500000:
                        self.view.storage_nodes()[node].deleteAnyMessage(service['content'])
                        self.controller.replication_overhead_update(service)
                        self.controller.remove_replication_hops(service)
                        all_in = 0
                        if labels:
                            for l in labels:
                                if node in self.view.model.request_labels and l in self.view.model.request_labels[
                                    node]:
                                    all_in += 1
                            if all_in == len(labels):
                                self.controller.add_request_labels_to_storage(node, service, True)
                            else:
                                self.controller.add_storage_labels_to_node(node, service)
                        self.controller.add_message_to_storage(node, service)
                    else:
                        if service['msg_size'] == 1000000:
                            self.controller.replication_overhead_update(service)
                            self.controller.remove_replication_hops(service)
                            service['msg_size'] = service['msg_size'] / 2
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_storage_labels_to_node(node, service)

                else:
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                              deadline, rtt_delay, STORE)

                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, RESPONSE)
                if (node != source and curTime + path_delay > deadline):
                    print("Error in HYBRID strategy: Request missed its deadline\nResponse at receiver at time: " + str(
                        curTime + path_delay) + " deadline: " + str(deadline))
                    task.print_task()
                    # raise ValueError("This should not happen: a task missed its deadline after being executed at an edge node.")

            elif status == REQUEST:

                # Processing a request
                source, cache_ret = self.view.closest_source(node, service)

                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                if not cache_ret and self.view.has_cache(source):
                    if self.controller.put_content(source, content['content']):
                        pass
                    else:
                        self.controller.put_content_local_cache(source)
                deadline_metric = (deadline - curTime - rtt_delay - compSpot.services[
                    service['content']].service_time)  # /deadline
                if self.debug:
                    print("Deadline metric: " + repr(deadline_metric))
                if self.view.has_service(node, service) and service["service_type"] is "proc":
                    if self.controller.has_message(node, labels, service):
                        print "ERROR: Tasks should not be admitted in nodes which do not have the data they need to " \
                              "process the request and these nodes should be added as data 'source', but hey-ho"
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content['content']):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        if self.debug:
                            print("Calling admit_task")
                        ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline,
                                                          receiver, rtt_delay, self.controller, self.debug)
                        if self.debug:
                            print("Done Calling admit_task")
                        if ret is False:
                            # Pass the Request upstream
                            rtt_delay += delay * 2
                            # delay += 0.01 # delay associated with content fetch from storage
                            # WE DON'T INCLUDE THIS BECAUSE THE CONTENT MAY BE PRE-FETCHED INTO THE CACHE FROM STORAGE
                            self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                      flow_id, deadline, rtt_delay, REQUEST)
                            if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                            if deadline_metric > 0:
                                self.cand_deadline_metric[node][service['content']] += deadline_metric
                            if self.debug:
                                print("Pass upstream to node: " + repr(next_node))
                            compSpot.missed_requests[service['content']] += 1  # Added here
                        else:
                            if deadline_metric > 0:
                                self.deadline_metric[node][service['content']] += deadline_metric
                    else:
                        source, cache_ret = self.view.closest_source(node, service)

                        path = self.view.shortest_path(node, source)
                        next_node = path[1]
                        delay = self.view.path_delay(node, next_node)
                        if not cache_ret and self.view.has_cache(source):
                            if self.controller.put_content(source, content['content']):
                                pass
                            else:
                                self.controller.put_content_local_cache(source)
                        compSpot.missed_requests[service['content']] += 1
                        if self.debug:
                            print("Not running the service: Pass upstream to node: " + repr(next_node))
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, REQUEST)
                        if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                service['content'], service['labels']):
                            self.controller.add_request_labels_to_node(node, service)
                        if deadline_metric > 0:
                            self.cand_deadline_metric[node][service['content']] += deadline_metric

                else:  # Not running the service
                    compSpot.missed_requests[service['content']] += 1
                    if self.debug:
                        print("Not running the service: Pass upstream to node: " + repr(next_node))
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, REQUEST)
                    if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                            service['content'], service['labels']):
                        self.controller.add_request_labels_to_node(node, service)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service['content']] += deadline_metric
            elif status == STORE:
                pass
            else:
                print("Error: unrecognised status value : " + repr(status))

    def find_closest_feasible_node(self, receiver, flow_id, path, curTime, service, deadline, rtt_delay):
        """
        finds fathest comp. spot to schedule a request using current
        congestion information at each upstream comp. spot.
        The goal is to carry out computations at the farthest node to create space for
        tasks that require closer comp. spots.
        """

        source = self.view.content_source(service, [])[len(self.view.content_source(service, [])) - 1]
        if len(self.view.content_source(service, [])) > 1:
            if source == path[0]:
                self.self_calls[path[0]] += 1
        if self.self_calls[path[0]] >= 3:
            source = self.view.content_source_cloud(service, service['labels'])
            if not source:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[path[0]] is not None:
                        source = n
            self.self_calls[path[0]] = 0
        # start from the upper-most node in the path and check feasibility
        upstream_node = source
        aTask = None
        for n in reversed(path[1:-1]):
            cs = self.compSpots[n]
            if cs.is_cloud:
                continue
            if cs.numberOfVMInstances[service['content']] == 0:
                continue
            if len(cs.scheduler.busyVMs[service['content']]) + len(cs.scheduler.idleVMs[service['content']]) <= 0:
                continue
            delay = self.view.path_delay(receiver, n)
            rtt_to_cs = rtt_delay + 2 * delay
            serviceTime = cs.services[service['content']].service_time
            if deadline - curTime - rtt_to_cs < serviceTime:
                continue
            aTask = Task(curTime, Task.TASK_TYPE_SERVICE, deadline, rtt_to_cs, n, service['content'], service['labels'],
                         serviceTime, flow_id, receiver, curTime + delay)
            cs.scheduler.upcomingTaskQueue.append(aTask)
            cs.scheduler.upcomingTaskQueue = sorted(cs.scheduler.upcomingTaskQueue, key=lambda x: x.arrivalTime)
            cs.compute_completion_times(curTime, False, self.debug)
            for task in cs.scheduler._taskQueue + cs.scheduler.upcomingTaskQueue:
                if self.debug:
                    print("After compute_completion_times:")
                    task.print_task()
                if task.taskType == Task.TASK_TYPE_VM_START:
                    continue
                if ((task.expiry - delay) < task.completionTime) or (task.completionTime == float('inf')):
                    cs.scheduler.upcomingTaskQueue.remove(aTask)
                    if self.debug:
                        print ("Task with flow_id " + str(
                            aTask.flow_id) + " is violating its expiration time at node: " + str(n))
                    break
            else:
                source = n
                # if self.debug:
                # if aTask.flow_id == 1964:
                #    print ("Task is scheduled at node 5:")
                #    aTask.print_task()
                if self.debug:
                    print ("Flow_id: " + str(aTask.flow_id) + " is scheduled to run at " + str(source))
                return source
        return source

    # TODO: UPDATE BELOW WITH COLLECTORS INSTEAD OF PREVIOUS OUTPUT FILES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def updateCloudBW(self, node, period):
        self.cloudBW = self.view.model.repoStorage[node].getDepletedCloudProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedCloudMessagesBW(period)

    def updateUpBW(self, node, period):
        self.cloudBW = self.view.model.repoStorage[node].getDepletedCloudProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedMessagesBW(period)

    def updateDeplBW(self, node, period):
        self.deplBW = self.view.model.repoStorage[node].getDepletedProcMessagesBW(period) + \
                      self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                      self.view.model.repoStorage[node].getDepletedMessagesBW(period)

    def deplCloud(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcessedMessagesSize() +
                self.view.model.repoStorage[node].getStaleMessagesSize() >
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor)):
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

                if not self.view.model.repoStorage[node].isProcessedEmpty():
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.model.repoStorage[node].getOldestDeplUnProcMessage()() is not None:
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")


                elif self.view.model.repoStorage[node].getOldestStaleMessage()() is not None:
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                else:
                    self.cloudEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.cloudBW)

                # Revise:
                self.updateCloudBW(node, period)

                # System.out.prln("Depletion is at: " + deplBW)
                self.lastDepl = curTime
                """System.out.prln("self.cloudBW is at " +
                      self.cloudBW +
                      " self.cloud_lim is at " +
                      self.cloud_lim +
                      " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equal " +
                      (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor) +
                      " Total space is " + self.view.model.repoStorage[node].getTotalStorageSpace()) """
            # System.out.prln("Depleted  messages: " + sdepleted)
        elif (self.view.model.repoStorage[node].getProcessedMessagesSize() +
              self.view.model.repoStorage[node].getStaleMessagesSize()) > \
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.max_stor):
            self.cloudEmptyLoop = True
            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                if (self.view.model.repoStorage[node].getOldestStaleMessage() is not None and
                        self.cloudBW < self.cloud_lim):
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """
                    * Oldest unprocessed messages should be given priority for depletion
                    * at a certain po.
                    """

                elif (self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None):
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """
                * Oldest processed message is depleted (as a FIFO type of storage,
                * and a  message for processing is processed
                    """
                elif (not self.view.model.repoStorage[node].isProcessedEmpty):
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateCloudBW(node, period)
                    # System.out.prln("Depletion is at: " + deplBW)
                    self.lastDepl = curTime
                    """System.out.prln("self.cloudBW is at " +
                      self.cloudBW +
                      " self.cloud_lim is at " +
                      self.cloud_lim +
                      " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equal " +
                      (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor) +
                      " Total space is " + self.view.model.repoStorage[node].getTotalStorageSpace()) """
                # System.out.prln("Depleted  messages: " + sdepleted)

    def deplUp(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcessedMessagesSize() >
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor)):

            self.cloudEmptyLoop = True

            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """

                * Oldest processed	message is depleted(as a FIFO type of storage,
                * and a	message for processing is processed

                """
                # TODO: NEED TO add COMPRESSED PROCESSED messages to storage AFTER normal servicing
                if (not self.view.model.repoStorage[node].isProcessedEmpty):
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None:
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                elif self.view.model.repoStorage[node].getOldestStaleMessage() is not None:
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                #
                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateUpBW(node, period)

            # System.out.prln("Depletion is at : "+ deplBW)
            self.lastDepl = curTime
            """System.out.prln("self.cloudBW is at " +
                     self.cloudBW +
                     " self.cloud_lim is at " +
                     self.cloud_lim +
                     " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equa l "+
                     (self.view.model.repoStorage[node].getTotalStorageSpac e *self.min_st or)+
                     " Total space i s  "+self.view.model.repoStorage[node].getTotalStorageSpace()( ) )"""
            # System.out.prln("Depleted  messages : "+ sdepleted)

            self.upEmptyLoop = True
        for i in range(0, 50) and self.cloudBW > self.cloud_lim and \
                 not self.view.model.repoStorage[node].isProcessingEmpty() and self.upEmptyLoop:
            if (not self.view.model.repoStorage[node].isProcessedEmpty):
                self.processedDepletion(node)

            elif (not self.view.model.repoStorage[node].isProcessingEmpty):
                self.view.model.repoStorage[node].deleteAnyMessage(
                    self.view.model.repoStorage[node].getOldestProcessMessage['content'])
            else:
                self.upEmptyLoop = False

    # System.out.prln("Depletion is at: "+ self.cloudBW)

    def deplStorage(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcMessagesSize() +
                self.view.model.repoStorage[node].getMessagesSize() >
                self.view.model.repoStorage[node].getTotalStorageSpace() * self.max_stor):
            self.deplEmptyLoop = True
            for i in range(0, 50) and self.deplBW < self.depl_rate and self.deplEmptyLoop:
                if (self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None):
                    content = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(content, content['labels'])
                    if not source:
                         for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                    delay = self.view.path_delay(node, source)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, content, labels, source, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                elif (self.view.model.repoStorage[node].getOldestStaleMessage() is not None):
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")


                elif (self.view.model.repoStorage[node].getOldestInvalidProcessMessage() is not None):
                    msg = self.oldestInvalidProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                elif (self.view.model.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.model.repoStorage[node].getOldestMessage()
                    self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.model.repoStorage[node].addToDeplMessages(ctemp)
                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.model.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                    if not source:
                        source, in_cache = self.view.closest_source(node, content)
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                    delay = self.view.path_delay(node, source)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, ctemp, labels, source, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    else:
                        self.view.model.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.model.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.model.repoStorage[node].getNewestProcessMessage()
                    self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.model.repoStorage[node].addToDeplProcMessages(ctemp)
                    source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                    if not source:
                        source, in_cache = self.view.closest_source(node, content)
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                    delay = self.view.path_delay(node, source)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, ctemp, labels, source, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    if (storTime <= ctemp['shelfLife'] + 1):
                        ctemp['overtime'] = False

                    elif (storTime > ctemp['shelfLife'] + 1):
                        ctemp['overtime'] = True

                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.model.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                        source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                        delay = self.view.path_delay(node, source)
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, content, labels, source, flow_id, deadline, rtt_delay,
                                                  STORE)
                        if self.debug:
                            print("Message is scheduled to be stored in the CLOUD")
                    else:
                        self.view.model.repoStorage[node].addToDeplMessages(ctemp)
                        source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                        delay = self.view.path_delay(node, source)
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, content, labels, source, flow_id, deadline, rtt_delay,
                                                  STORE)
                        if self.debug:
                            print("Message is scheduled to be stored in the CLOUD")
            else:
                self.deplEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.deplBW)

                self.updateDeplBW(node, period)
                self.updateCloudBW(node, period)
            # Revise:
            self.lastDepl = curTime

    def processedDepletion(self, node):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getOldestFreshMessage() is not None):
            if (self.view.model.repoStorage[node].getOldestFreshMessage().getProperty("procTime") is None):
                temp = self.view.model.repoStorage[node].getOldestFreshMessage()
                report = False
                self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
                """
                * Make sure here that the added message to the cloud depletion 
                * tracking is also tracked by whether it 's Fresh or Stale.
                """
                report = True
                self.view.model.repoStorage[node].addToStoredMessages(temp)
                self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
                return temp



            elif (self.view.model.repoStorage[node].getOldestShelfMessage() is not None):
                if (self.view.model.repoStorage[node].getOldestShelfMessage().getProperty("procTime") is None):
                    temp = self.view.model.repoStorage[node].getOldestShelfMessage()
                    report = False
                    self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
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
                    self.view.model.repoStorage[node].addToStoredMessages(temp)
                    self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.model.repoStorage[node].getOldestStaleMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.model.repoStorage[node].addToCloudDeplMessages(ctemp)

        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.model.repoStorage[node].addToCloudDeplMessages(ctemp)

        self.lastCloudUpload = curTime
        return ctemp

    def oldestInvalidProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.model.repoStorage[node].getOldestInvalidProcessMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(temp['content'])
        if (temp['comp'] is not None):
            if (temp['comp']):
                ctemp = self.compressMessage(node, temp)
                self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                storTime = curTime - ctemp['receiveTime']
                ctemp['storTime'] = storTime
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                self.view.model.repoStorage[node].addToDeplProcMessages(ctemp)

            elif (temp['comp'] is None):
                storTime = curTime - temp['receiveTime']
                temp['storTime'] = storTime
                temp['satisfied'] = False
                if (storTime <= temp['shelfLife'] + 1):
                    temp['overtime'] = False

                elif (storTime > temp['shelfLife'] + 1):
                    temp['overtime'] = True

                self.view.model.repoStorage[node].addToDeplProcMessages(temp)
        return temp

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.model.repoStorage[node].getOldestDeplUnProcMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(temp['content'])
        self.view.model.repoStorage[node].addToDepletedUnProcMessages(temp)
        return temp

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

    def __init__(self, view, controller, replacement_interval=10, debug=False, n_replacements=1,
                 depl_rate=10000000, cloud_lim=20000000, max_stor=9900000000, min_stor=10000000,**kwargs):
        super(HServSpecStorApp, self).__init__(view, controller)

        self.view.model.strategy = 'HYBRIDS_SPEC_REPO_APP'
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
        self.serviceNodeUtil = [None]*len(self.receivers)
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0

        for recv in self.receivers:
            recv = int(recv[4:])
            self.serviceNodeUtil[recv] = [None]*self.num_nodes
            for n in self.compSpots.keys():
                cs = self.compSpots[n]
                if cs.is_cloud:
                    continue
                self.serviceNodeUtil[recv][n] = [0.0]*self.num_services

        # vars

        self.lastDepl = 0

        self.last_period = 0

        self.cloudEmptyLoop = True

        self.deplEmptyLoop = True

        self.upEmptyLoop = True

        self.lastCloudUpload = 0

        self.deplBW = 0

        self.cloudBW = 0

        self.procMinI = 0
        # processedSize = self.procSize * self.proc_ratio
        self.depl_rate = depl_rate
        self.cloud_lim = cloud_lim
        self.max_stor = max_stor
        self.min_stor = min_stor
        self.self_calls = {}
        for node in view.storage_nodes(True):
            self.self_calls[node] = 0
        self.view = view


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
    def replace_services1(self, curTime):
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
                    if cs.numberOfVMInstances[service] > len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                        aVM = VM(self, service)
                        cs.scheduler.idleVMs[service].append(aVM)
                    elif cs.numberOfVMInstances[service] < len(cs.scheduler.idleVMs[service]) + len(
                        cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service]):
                        cs.numberOfVMInstances[service] = len(cs.scheduler.idleVMs[service]) + len(cs.scheduler.busyVMs[service]) + len(cs.scheduler.startingVMs[service])

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
                        self.controller.reassign_vm(curTime, cs, service_running, service_missed, self.debug)
                        # cs.reassign_vm(self.controller, curTime, service_running, service_missed, self.debug)
                        if self.debug:
                            print("Missed util: " + str(missed_util) + " running util: " + str(
                                running_util) + " Adequate time missed: " + str(
                                delay[service_missed]) + " Adequate time running: " + str(delay[service_running]))
                        del running_services_utilisation_normalised[indx]
                        n_replacements += 1
                        break
            if self.debug:
                print(str(n_replacements) + " replacements at node:" + str(cs.node) + " at time: " + str(curTime))
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

    def handle(self, curTime, receiver, msg, node, path, log, feedback, flow_id, rtt_delay, deadline):
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
        if self.view.hasStorageCapability(node) and 'satisfied' not in msg or ('Shelf' not in msg or msg['Shelf']):
            self.controller.add_replication_hops(msg)
            # TODO: Check usages of IS OPERATOR throughout code!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            on_path = True
            in_path, node_c = self.view.most_services_labels_closest_repo(msg['labels'], node, path, on_path)
            on_path = False
            off_path, node_s = self.view.most_services_labels_closest_repo(msg['labels'], node, path, on_path)
            if node == node_c and in_path:
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_replication_hops(msg)
                self.controller.add_storage_labels_to_node(node, msg)
                # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
                if self.controller.has_request_labels(node, msg['labels']):
                    self.controller.add_request_labels_to_storage(node, msg['labels'], False)
            elif self.view.all_labels_most_requests(msg["labels"]) and node is self.view.all_labels_most_requests(msg["labels"]):
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_replication_hops(msg)
                self.controller.add_request_labels_to_storage(node, msg)
                self.controller.add_request_labels_to_storage(node, msg['labels'], True)
                # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
            elif node == node_s and not off_path:
                self.controller.add_message_to_storage(node, msg)
                self.controller.add_replication_hops(msg)
                self.controller.add_storage_labels_to_node(node, msg)
                # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
                if self.controller.has_request_labels(node, msg['labels']):
                    self.controller.add_request_labels_to_storage(node, msg['labels'], False)
            elif in_path:
                edr = node_c
                self.controller.add_request_labels_to_node(node, msg)
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                              curTime + msg['shelf_life'], rtt_delay, STORE)
                # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)
                self.controller.replicate(node, edr)
            elif node_s and not off_path:
                edr = node_s
                self.controller.add_request_labels_to_node(node, msg)
                path = self.view.shortest_path(node, edr)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)

                self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                              curTime + msg['shelf_life'], rtt_delay, STORE)
                # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)
                self.controller.replicate(node, edr)

            else:
                edr = self.view.all_labels_most_requests(msg["labels"])
                if edr and node == edr.node:
                    self.controller.add_message_to_storage(node, msg)
                    self.controller.add_replication_hops(msg)
                    self.controller.add_request_labels_to_storage(node, msg['labels'], True)
                    # print "Message: " + str(msg['content']) + " added to the storage of node: " + str(node)
                elif edr:
                    self.controller.add_request_labels_to_node(node, msg)
                    # if edr and edr.hasMessage(msg['content'], msg['labels']):
                    #     msg = edr.hasMessage(msg['content'], msg['labels'])
                    # else:
                    #     msg['shelf_life'] = deadline - curTime
                    path = self.view.shortest_path(node, edr.node)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)

                    self.controller.add_event(curTime + delay, node, msg, msg['labels'], next_node, flow_id,
                                              curTime + msg['shelf_life'], rtt_delay, STORE)
                    # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)
                    self.controller.replicate(node, next_node)

        else:
            msg['overtime'] = False
            source = self.view.content_source_cloud(msg, msg['labels'])
            if not source:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                        source = n
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay * 2
            self.controller.add_event(curTime + delay, receiver, msg, msg['labels'], next_node, flow_id, deadline, rtt_delay,
                                      STORE)
            # print "Message: " + str(msg['content']) + " sent from node " + str(node) + " to node " + str(next_node)

        return msg

    @inheritdoc(Strategy)
    def process_event(self, curTime, receiver, content, log, labels, node, flow_id, deadline, rtt_delay, status,
                      task=None):
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

        if time.time() - self.last_period >= 1:
            self.last_period = time.time()
            period = True
        else:
            period = False

        if self.view.hasStorageCapability(node):

            self.updateCloudBW(node, period)
            self.deplCloud(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)
            self.updateDeplBW(node, period)
            self.deplStorage(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)

        elif not self.view.hasStorageCapability(node) and self.view.has_computationalSpot(node):
            self.updateUpBW(node, period)
            self.deplUp(node, receiver, content, labels, log, flow_id, deadline, rtt_delay, period)

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
        if type(content) is dict and 'shelf_life' in content and 'max_replications' in content:
            if content["shelf_life"] and content['replications'] <= content['max_replications']:
                source, in_cache = self.view.closest_source(node, content)
                path = self.view.shortest_path(node, source)
                self.handle(curTime, receiver, content, node, path, log, feedback, flow_id, rtt_delay, deadline)
        elif type(content) is dict and 'shelf_life' in content and content["shelf_life"] :
            source, in_cache = self.view.closest_source(node, content)
            path = self.view.shortest_path(node, source)
            self.handle(curTime, receiver, content, node, path, log, feedback, flow_id, rtt_delay, deadline)
        service = content

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

        source, in_cache = self.view.closest_source(node, service)
        cloud_source = self.view.content_source_cloud(service, labels)
        if not cloud_source:
            cloud_source, in_cache = self.view.closest_source(node, service)
            if cloud_source == node:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                        node] is not None:
                        cloud_source = n

        path = self.view.shortest_path(node, cloud_source)

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
            if cloud_source == node and status == REQUEST:

                cache_delay = 0
                if not in_cache and self.view.has_cache(node):
                    if self.controller.put_content(source, content['content']):
                        cache_delay = 0.005
                    else:
                        self.controller.put_content_local_cache(source)
                        cache_delay = 0.005
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                elif in_cache:
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                # for n in range(0, 3):
                #     self.controller.add_replication_hops(content)
                ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver,
                                                  rtt_delay + cache_delay, self.controller, self.debug)

                if ret == False:
                    if type(node) == str and "src" in node:
                        print("This should not happen in Hybrid.")
                        raise ValueError("Task should not be rejected at the cloud.")
                    else:
                        # request is to be executed in the cloud and returned to receiver
                        if type(service) is dict and self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                            self.controller.add_request_labels_to_node(node, service)
                        services = self.view.services()
                        serviceTime = services[service['content']].service_time
                        self.controller.add_event(curTime + rtt_delay + serviceTime, receiver, service, labels,
                                                  receiver, flow_id, deadline, rtt_delay, RESPONSE)
                        if self.debug:
                            print("Request is scheduled to run at the CLOUD")
                    for n in path[1:]:
                        cs = self.view.compSpot(n)
                        if cs.is_cloud:
                            continue
                        self.serviceNodeUtil[int(receiver[4:])][n][service['content']] += self.view.services()[
                            service['content']].service_time
                    return
                return

            if source == node and status == REQUEST:
                cache_delay = 0
                if not in_cache and self.view.has_cache(node):
                    if self.controller.put_content(source, content['content']):
                        cache_delay = 0.005
                    else:
                        self.controller.put_content_local_cache(source)
                        cache_delay = 0.005
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                elif in_cache:
                    pc = self.controller.has_message(node, labels, content['content'])
                    if type(pc) != dict:
                        source, in_cache = self.view.closest_source(node, service)
                        pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                        if type(pc) != dict:
                            for n in self.content_source(content, content['labels']):
                                if n == source:
                                    pc = self.model.contents[n][content['content']]
                    if pc['service_type'] == 'processed':
                        path = self.view.shortest_path(node, receiver)
                        next_node = path[1]
                        delay = self.view.link_delay(node, next_node)
                        path_del = self.view.path_delay(node, receiver)
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, RESPONSE)
                        if path_del + curTime > deadline:
                            if type(content) is dict:
                                compSpot.missed_requests[content['content']] += 1
                            else:
                                compSpot.missed_requests[content] += 1
                        return
                if compSpot is not None and self.view.has_service(node, service) and service["service_type"] is "proc":
                    ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline, receiver,
                                                      rtt_delay + cache_delay, self.controller, self.debug)

                    if ret == False:

                        if type(service) is dict and self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                            self.controller.add_request_labels_to_node(node, service)
                        source = self.view.content_source_cloud(service, labels)
                        if not source:
                            source, in_cache = self.view.closest_source(node, service)
                            if source == node:
                                for n in self.view.model.comp_size:
                                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                        node] is not None:
                                        source = n
                        path = self.view.shortest_path(node, source)
                        upstream_node = self.find_closest_feasible_node(receiver, flow_id, path, curTime, service, deadline, rtt_delay)
                        delay = self.view.path_delay(node, source)
                        # self.controller.add_event(curTime + delay, receiver, service, labels, upstream_node, flow_id,
                        #                           deadline, rtt_delay, STORE)
                        rtt_delay += delay * 2
                        if upstream_node != source:
                            self.controller.add_event(curTime + delay, receiver, content, labels, upstream_node,
                                                      flow_id, deadline, rtt_delay, REQUEST)
                            if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                            if self.debug:
                                print("Message is scheduled to run at: " + str(upstream_node))
                        else:  # request is to be executed in the cloud and returned to receiver
                            services = self.view.services()
                            serviceTime = services[service['content']].service_time
                            self.controller.add_event(curTime + rtt_delay + serviceTime, receiver, service, labels,
                                                      receiver, flow_id, deadline, rtt_delay, RESPONSE)
                            if self.debug:
                                print("Request is scheduled to run at the CLOUD")
                    return
                return

            #  Request at the receiver
            if receiver == node and status == REQUEST:

                self.controller.start_session(curTime, receiver, service, labels, log, flow_id, deadline)
                path = self.view.shortest_path(node, cloud_source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                rtt_delay += delay * 2
                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, REQUEST)
                if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                        service['content'], service['labels']):
                    self.controller.add_request_labels_to_node(node, service)
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
                        if type(content) is dict:
                            compSpot.missed_requests[content['content']] += 1
                        else:
                            compSpot.missed_requests[content] += 1

            elif status == TASK_COMPLETE:
                self.controller.complete_task(task, curTime)
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
                    source, in_cache = self.view.closest_source(node, service)
                    if type(service) is not dict and self.controller.has_message(node, labels, content):
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content)
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content)
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        service = self.view.storage_nodes()[node].hasMessage(content, labels)
                    elif type(service) is dict and self.controller.has_message(node, labels, content):
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content['content']):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        service['labels'] = self.controller.has_message(node, labels, content)['labels']
                        if service['freshness_per'] > curTime - service['receiveTime']:
                            service['Fresh'] = True
                            service['Shelf'] = True
                        elif service['shelf_life'] > curTime - service['receiveTime']:
                            service['Fresh'] = False
                            service['Shelf'] = True
                        else:
                            service['Fresh'] = False
                            service['Shelf'] = False
                        service['receiveTime'] = curTime
                        service['service_type'] = "processed"
                    else:
                        service = dict()
                        service['content'] = content
                        service['labels'] = labels
                        service['msg_size'] = 1000000
                        service['Fresh'] = False
                        service['Shelf'] = False
                        service['receiveTime'] = curTime
                        service['service_type'] = "processed"
                    if self.controller.has_message(node, service['labels'], service['content']) and service['msg_size'] == 1000000:
                        self.view.storage_nodes()[node].deleteAnyMessage(service['content'])
                        self.controller.replication_overhead_update(service)
                        self.controller.remove_replication_hops(service)
                        service['msg_size'] = service['msg_size'] / 2
                        all_in = 0
                        if labels:
                            for l in labels:
                                if node in self.view.model.request_labels and l in self.view.model.request_labels[
                                    node]:
                                    all_in += 1
                            if all_in == len(labels):
                                self.controller.add_request_labels_to_storage(node, service, True)
                            else:
                                self.controller.add_storage_labels_to_node(node, service)
                        self.controller.add_message_to_storage(node, service)
                    elif self.controller.has_message(node, service['labels'], service['content']) and service['msg_size'] == 500000:
                        self.view.storage_nodes()[node].deleteAnyMessage(service['content'])
                        self.controller.replication_overhead_update(service)
                        self.controller.remove_replication_hops(service)
                        all_in = 0
                        if labels:
                            for l in labels:
                                if node in self.view.model.request_labels and l in self.view.model.request_labels[
                                    node]:
                                    all_in += 1
                            if all_in == len(labels):
                                self.controller.add_request_labels_to_storage(node, service, True)
                            else:
                                self.controller.add_storage_labels_to_node(node, service)
                        self.controller.add_message_to_storage(node, service)
                    else:
                        if service['msg_size'] == 1000000:
                            self.controller.replication_overhead_update(service)
                            self.controller.remove_replication_hops(service)
                            service['msg_size'] = service['msg_size'] / 2
                        self.controller.add_message_to_storage(node, service)
                        self.controller.add_storage_labels_to_node(node, service)

                else:
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                              deadline, rtt_delay, STORE)

                self.controller.add_event(curTime + delay, receiver, service, labels, next_node, flow_id,
                                          deadline, rtt_delay, RESPONSE)
                if (node != source and curTime + path_delay > deadline):
                    print("Error in HYBRID strategy: Request missed its deadline\nResponse at receiver at time: " + str(
                        curTime + path_delay) + " deadline: " + str(deadline))
                    task.print_task()
                    # raise ValueError("This should not happen: a task missed its deadline after being executed at an edge node.")

            elif status == REQUEST:

                # Processing a request
                source, cache_ret = self.view.closest_source(node, service)

                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.path_delay(node, next_node)
                if not cache_ret and self.view.has_cache(source):
                    if self.controller.put_content(source, content['content']):
                        pass
                    else:
                        self.controller.put_content_local_cache(source)
                deadline_metric = (deadline - curTime - rtt_delay - compSpot.services[
                    service['content']].service_time)  # /deadline
                if self.debug:
                    print("Deadline metric: " + repr(deadline_metric))
                if self.view.has_service(node, service) and service["service_type"] is "proc":
                    if self.controller.has_message(node, labels, service):
                        print "ERROR: Tasks should not be admitted in nodes which do not have the data they need to " \
                              "process the request and these nodes should be added as data 'source', but hey-ho"
                        cache_delay = 0
                        if not in_cache and self.view.has_cache(node):
                            if self.controller.put_content(source, content['content']):
                                cache_delay = 0.005
                            else:
                                self.controller.put_content_local_cache(source)
                                cache_delay = 0.005
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + cache_delay + delay, receiver, service, labels,
                                                          next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        elif in_cache:
                            pc = self.controller.has_message(node, labels, content['content'])
                            if type(pc) != dict:
                                source, in_cache = self.view.closest_source(node, service)
                                pc = self.view.model.repoStorage[source].hasMessage(content['content'], labels)
                                if type(pc) != dict:
                                    for n in self.content_source(content, content['labels']):
                                        if n == source:
                                            pc = self.model.contents[n][content['content']]
                            if pc['service_type'] == 'processed':
                                path = self.view.shortest_path(node, receiver)
                                next_node = path[1]
                                delay = self.view.link_delay(node, next_node)
                                path_del = self.view.path_delay(node, receiver)
                                self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                          flow_id, deadline, rtt_delay, RESPONSE)
                                if path_del + curTime > deadline:
                                    if type(content) is dict:
                                        compSpot.missed_requests[content['content']] += 1
                                    else:
                                        compSpot.missed_requests[content] += 1
                                return
                        if self.debug:
                            print("Calling admit_task")
                        ret, reason = compSpot.admit_task(service['content'], labels, curTime, flow_id, deadline,
                                                          receiver, rtt_delay, self.controller, self.debug)
                        if self.debug:
                            print("Done Calling admit_task")
                        if ret is False:
                            # Pass the Request upstream
                            rtt_delay += delay * 2
                            # delay += 0.01 # delay associated with content fetch from storage
                            # WE DON'T INCLUDE THIS BECAUSE THE CONTENT MAY BE PRE-FETCHED INTO THE CACHE FROM STORAGE
                            self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                      flow_id, deadline, rtt_delay, REQUEST)
                            if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                    service['content'], service['labels']):
                                self.controller.add_request_labels_to_node(node, service)
                            if deadline_metric > 0:
                                self.cand_deadline_metric[node][service['content']] += deadline_metric
                            if self.debug:
                                print("Pass upstream to node: " + repr(next_node))
                            compSpot.missed_requests[service['content']] += 1  # Added here
                        else:
                            if deadline_metric > 0:
                                self.deadline_metric[node][service['content']] += deadline_metric
                    else:
                        source, cache_ret = self.view.closest_source(node, service)

                        path = self.view.shortest_path(node, source)
                        next_node = path[1]
                        delay = self.view.path_delay(node, next_node)
                        if not cache_ret and self.view.has_cache(source):
                            if self.controller.put_content(source, content['content']):
                                pass
                            else:
                                self.controller.put_content_local_cache(source)
                        compSpot.missed_requests[service['content']] += 1
                        if self.debug:
                            print("Not running the service: Pass upstream to node: " + repr(next_node))
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                                  flow_id, deadline, rtt_delay, REQUEST)
                        if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                                service['content'], service['labels']):
                            self.controller.add_request_labels_to_node(node, service)
                        if deadline_metric > 0:
                            self.cand_deadline_metric[node][service['content']] += deadline_metric

                else:  # Not running the service
                    compSpot.missed_requests[service['content']] += 1
                    if self.debug:
                        print("Not running the service: Pass upstream to node: " + repr(next_node))
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, service, labels, next_node,
                                              flow_id, deadline, rtt_delay, REQUEST)
                    if self.view.hasStorageCapability(node) and not self.view.storage_nodes()[node].hasMessage(
                            service['content'], service['labels']):
                        self.controller.add_request_labels_to_node(node, service)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service['content']] += deadline_metric
            elif status == STORE:
                pass
            else:
                print("Error: unrecognised status value : " + repr(status))

    def find_closest_feasible_node(self, receiver, flow_id, path, curTime, service, deadline, rtt_delay):
        """
        finds fathest comp. spot to schedule a request using current
        congestion information at each upstream comp. spot.
        The goal is to carry out computations at the farthest node to create space for
        tasks that require closer comp. spots.
        """

        source = self.view.content_source(service, [])[len(self.view.content_source(service, [])) - 1]
        if len(self.view.content_source(service, [])) > 1:
            if source == path[0]:
                self.self_calls[path[0]] += 1
        if self.self_calls[path[0]] >= 3:
            source = self.view.content_source_cloud(service, service['labels'])
            if not source:
                for n in self.view.model.comp_size:
                    if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[path[0]] is not None:
                        source = n
            self.self_calls[path[0]] = 0
        # start from the upper-most node in the path and check feasibility
        upstream_node = source
        aTask = None
        for n in reversed(path[1:-1]):
            cs = self.compSpots[n]
            if cs.is_cloud:
                continue
            if cs.numberOfVMInstances[service['content']] == 0:
                continue
            if len(cs.scheduler.busyVMs[service['content']]) + len(cs.scheduler.idleVMs[service['content']]) <= 0:
                continue
            delay = self.view.path_delay(receiver, n)
            rtt_to_cs = rtt_delay + 2 * delay
            serviceTime = cs.services[service['content']].service_time
            if deadline - curTime - rtt_to_cs < serviceTime:
                continue
            aTask = Task(curTime, Task.TASK_TYPE_SERVICE, deadline, rtt_to_cs, n, service['content'], service['labels'],
                         serviceTime, flow_id, receiver, curTime + delay)
            cs.scheduler.upcomingTaskQueue.append(aTask)
            cs.scheduler.upcomingTaskQueue = sorted(cs.scheduler.upcomingTaskQueue, key=lambda x: x.arrivalTime)
            cs.compute_completion_times(curTime, False, self.debug)
            for task in cs.scheduler._taskQueue + cs.scheduler.upcomingTaskQueue:
                if self.debug:
                    print("After compute_completion_times:")
                    task.print_task()
                if task.taskType == Task.TASK_TYPE_VM_START:
                    continue
                if ((task.expiry - delay) < task.completionTime) or (task.completionTime == float('inf')):
                    cs.scheduler.upcomingTaskQueue.remove(aTask)
                    if self.debug:
                        print ("Task with flow_id " + str(
                            aTask.flow_id) + " is violating its expiration time at node: " + str(n))
                    break
            else:
                source = n
                # if self.debug:
                # if aTask.flow_id == 1964:
                #    print ("Task is scheduled at node 5:")
                #    aTask.print_task()
                if self.debug:
                    print ("Flow_id: " + str(aTask.flow_id) + " is scheduled to run at " + str(source))
                return source
        return source

    # TODO: UPDATE BELOW WITH COLLECTORS INSTEAD OF PREVIOUS OUTPUT FILES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def updateCloudBW(self, node, period):
        self.cloudBW = self.view.model.repoStorage[node].getDepletedCloudProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedCloudMessagesBW(period)

    def updateUpBW(self, node, period):
        self.cloudBW = self.view.model.repoStorage[node].getDepletedCloudProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                       self.view.model.repoStorage[node].getDepletedMessagesBW(period)

    def updateDeplBW(self, node, period):
        self.deplBW = self.view.model.repoStorage[node].getDepletedProcMessagesBW(period) + \
                      self.view.model.repoStorage[node].getDepletedUnProcMessagesBW(period) + \
                      self.view.model.repoStorage[node].getDepletedMessagesBW(period)

    def deplCloud(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcessedMessagesSize() +
                self.view.model.repoStorage[node].getStaleMessagesSize() >
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor)):
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

                if not self.view.model.repoStorage[node].isProcessedEmpty():
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.model.repoStorage[node].getOldestDeplUnProcMessage()() is not None:
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")


                elif self.view.model.repoStorage[node].getOldestStaleMessage()() is not None:
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                else:
                    self.cloudEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.cloudBW)

                # Revise:
                self.updateCloudBW(node, period)

                # System.out.prln("Depletion is at: " + deplBW)
                self.lastDepl = curTime
                """System.out.prln("self.cloudBW is at " +
                      self.cloudBW +
                      " self.cloud_lim is at " +
                      self.cloud_lim +
                      " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equal " +
                      (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor) +
                      " Total space is " + self.view.model.repoStorage[node].getTotalStorageSpace()) """
            # System.out.prln("Depleted  messages: " + sdepleted)
        elif (self.view.model.repoStorage[node].getProcessedMessagesSize() +
              self.view.model.repoStorage[node].getStaleMessagesSize()) > \
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.max_stor):
            self.cloudEmptyLoop = True
            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                if (self.view.model.repoStorage[node].getOldestStaleMessage() is not None and
                        self.cloudBW < self.cloud_lim):
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """
                    * Oldest unprocessed messages should be given priority for depletion
                    * at a certain po.
                    """

                elif (self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None):
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """
                * Oldest processed message is depleted (as a FIFO type of storage,
                * and a  message for processing is processed
                    """
                elif (not self.view.model.repoStorage[node].isProcessedEmpty):
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateCloudBW(node, period)
                    # System.out.prln("Depletion is at: " + deplBW)
                    self.lastDepl = curTime
                    """System.out.prln("self.cloudBW is at " +
                      self.cloudBW +
                      " self.cloud_lim is at " +
                      self.cloud_lim +
                      " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equal " +
                      (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor) +
                      " Total space is " + self.view.model.repoStorage[node].getTotalStorageSpace()) """
                # System.out.prln("Depleted  messages: " + sdepleted)

    def deplUp(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcessedMessagesSize() >
                (self.view.model.repoStorage[node].getTotalStorageSpace() * self.min_stor)):

            self.cloudEmptyLoop = True

            for i in range(0, 50) and self.cloudBW < self.cloud_lim and self.cloudEmptyLoop:

                """

                * Oldest processed	message is depleted(as a FIFO type of storage,
                * and a	message for processing is processed

                """
                # TODO: NEED TO add COMPRESSED PROCESSED messages to storage AFTER normal servicing
                if (not self.view.model.repoStorage[node].isProcessedEmpty):
                    msg = self.processedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """
                elif self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None:
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                elif self.view.model.repoStorage[node].getOldestStaleMessage() is not None:
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                #
                else:
                    self.cloudEmptyLoop = False
                    # System.out.prln("Depletion is at: "+ self.cloudBW)

                    # Revise:
                    self.updateUpBW(node, period)

            # System.out.prln("Depletion is at : "+ deplBW)
            self.lastDepl = curTime
            """System.out.prln("self.cloudBW is at " +
                     self.cloudBW +
                     " self.cloud_lim is at " +
                     self.cloud_lim +
                     " self.view.model.repoStorage[node].getTotalStorageSpace()*self.min_stor equa l "+
                     (self.view.model.repoStorage[node].getTotalStorageSpac e *self.min_st or)+
                     " Total space i s  "+self.view.model.repoStorage[node].getTotalStorageSpace()( ) )"""
            # System.out.prln("Depleted  messages : "+ sdepleted)

            self.upEmptyLoop = True
        for i in range(0, 50) and self.cloudBW > self.cloud_lim and \
                 not self.view.model.repoStorage[node].isProcessingEmpty() and self.upEmptyLoop:
            if (not self.view.model.repoStorage[node].isProcessedEmpty):
                self.processedDepletion(node)

            elif (not self.view.model.repoStorage[node].isProcessingEmpty):
                self.view.model.repoStorage[node].deleteAnyMessage(
                    self.view.model.repoStorage[node].getOldestProcessMessage['content'])
            else:
                self.upEmptyLoop = False

    # System.out.prln("Depletion is at: "+ self.cloudBW)

    def deplStorage(self, node, receiver, content, labels, log, flow_id, deadline, rtt_delay=0, period=False):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getProcMessagesSize() +
                self.view.model.repoStorage[node].getMessagesSize() >
                self.view.model.repoStorage[node].getTotalStorageSpace() * self.max_stor):
            self.deplEmptyLoop = True
            for i in range(0, 50) and self.deplBW < self.depl_rate and self.deplEmptyLoop:
                if (self.view.model.repoStorage[node].getOldestDeplUnProcMessage() is not None):
                    msg = self.oldestUnProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    """ Oldest unprocessed message is depleted (as a FIFO type of storage) """

                elif (self.view.model.repoStorage[node].getOldestStaleMessage() is not None):
                    msg = self.oldestSatisfiedDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")


                elif (self.view.model.repoStorage[node].getOldestInvalidProcessMessage() is not None):
                    msg = self.oldestInvalidProcDepletion(node)
                    source = self.view.content_source_cloud(msg, msg['labels'])
                    if not source:
                       source, in_cache = self.view.closest_source(node, content)
                    if not source:
                        for n in self.view.model.comp_size:
                            if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[
                                node] is not None:
                                source = n
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.path_delay(node, next_node)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, msg, labels, next_node, flow_id, deadline,
                                              rtt_delay, STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")

                elif (self.view.model.repoStorage[node].getOldestMessage() is not None):
                    ctemp = self.view.model.repoStorage[node].getOldestMessage()
                    self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                    storTime = curTime - ctemp["receiveTime"]
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    ctemp['overtime'] = False
                    self.view.model.repoStorage[node].addToDeplMessages(ctemp)
                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.model.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                    source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                    if not source:
                        source, in_cache = self.view.closest_source(node, content)
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                    delay = self.view.path_delay(node, source)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, ctemp, labels, source, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    else:
                        self.view.model.repoStorage[node].addToDeplMessages(ctemp)


                elif (self.view.model.repoStorage[node].getNewestProcessMessage() is not None):
                    ctemp = self.view.model.repoStorage[node].getNewestProcessMessage()
                    self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                    storTime = curTime - ctemp['receiveTime']
                    ctemp['storTime'] = storTime
                    ctemp['satisfied'] = False
                    self.view.model.repoStorage[node].addToDeplProcMessages(ctemp)
                    source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                    if not source:
                        source, in_cache = self.view.closest_source(node, content)
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                    delay = self.view.path_delay(node, source)
                    rtt_delay += delay * 2
                    self.controller.add_event(curTime + delay, receiver, ctemp, labels, source, flow_id, deadline, rtt_delay,
                                              STORE)
                    if self.debug:
                        print("Message is scheduled to be stored in the CLOUD")
                    if (storTime <= ctemp['shelfLife'] + 1):
                        ctemp['overtime'] = False

                    elif (storTime > ctemp['shelfLife'] + 1):
                        ctemp['overtime'] = True

                    if ((ctemp['type']).equalsIgnoreCase("unprocessed")):
                        self.view.model.repoStorage[node].addToDepletedUnProcMessages(ctemp)
                        source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                        delay = self.view.path_delay(node, source)
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, content, labels, source, flow_id, deadline, rtt_delay,
                                                  STORE)
                        if self.debug:
                            print("Message is scheduled to be stored in the CLOUD")
                    else:
                        self.view.model.repoStorage[node].addToDeplMessages(ctemp)
                        source = self.view.content_source_cloud(ctemp, ctemp['labels'])
                        if not source:
                            for n in self.view.model.comp_size:
                                if type(self.view.model.comp_size[n]) is not int and self.view.model.comp_size[node] is not None:
                                    source = n
                        delay = self.view.path_delay(node, source)
                        rtt_delay += delay * 2
                        self.controller.add_event(curTime + delay, receiver, content, labels, source, flow_id, deadline, rtt_delay,
                                                  STORE)
                        if self.debug:
                            print("Message is scheduled to be stored in the CLOUD")
            else:
                self.deplEmptyLoop = False
                # System.out.prln("Depletion is at: "+ self.deplBW)

                self.updateDeplBW(node, period)
                self.updateCloudBW(node, period)
            # Revise:
            self.lastDepl = curTime

    def processedDepletion(self, node):
        curTime = time.time()
        if (self.view.model.repoStorage[node].getOldestFreshMessage() is not None):
            if (self.view.model.repoStorage[node].getOldestFreshMessage().getProperty("procTime") is None):
                temp = self.view.model.repoStorage[node].getOldestFreshMessage()
                report = False
                self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
                """
                * Make sure here that the added message to the cloud depletion 
                * tracking is also tracked by whether it 's Fresh or Stale.
                """
                report = True
                self.view.model.repoStorage[node].addToStoredMessages(temp)
                self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
                return temp



            elif (self.view.model.repoStorage[node].getOldestShelfMessage() is not None):
                if (self.view.model.repoStorage[node].getOldestShelfMessage().getProperty("procTime") is None):
                    temp = self.view.model.repoStorage[node].getOldestShelfMessage()
                    report = False
                    self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)
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
                    self.view.model.repoStorage[node].addToStoredMessages(temp)
                    self.view.model.repoStorage[node].deleteProcessedMessage(temp['content'], report)

    def oldestSatisfiedDepletion(self, node):
        curTime = time.time()
        ctemp = self.view.model.repoStorage[node].getOldestStaleMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.model.repoStorage[node].addToCloudDeplMessages(ctemp)

        storTime = curTime - ctemp['receiveTime']
        ctemp['storTime'] = storTime
        ctemp['satisfied'] = True
        if (storTime <= ctemp['shelfLife'] + 1):
            ctemp['overtime'] = False
        elif (storTime > ctemp['shelfLife'] + 1):
            ctemp['overtime'] = True

        self.view.model.repoStorage[node].addToCloudDeplMessages(ctemp)

        self.lastCloudUpload = curTime
        return ctemp

    def oldestInvalidProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.model.repoStorage[node].getOldestInvalidProcessMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(temp['content'])
        if (temp['comp'] is not None):
            if (temp['comp']):
                ctemp = self.compressMessage(node, temp)
                self.view.model.repoStorage[node].deleteAnyMessage(ctemp['content'])
                storTime = curTime - ctemp['receiveTime']
                ctemp['storTime'] = storTime
                if (storTime <= ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = False

                elif (storTime > ctemp['shelfLife'] + 1):
                    ctemp['overtime'] = True

                self.view.model.repoStorage[node].addToDeplProcMessages(ctemp)

            elif (temp['comp'] is None):
                storTime = curTime - temp['receiveTime']
                temp['storTime'] = storTime
                temp['satisfied'] = False
                if (storTime <= temp['shelfLife'] + 1):
                    temp['overtime'] = False

                elif (storTime > temp['shelfLife'] + 1):
                    temp['overtime'] = True

                self.view.model.repoStorage[node].addToDeplProcMessages(temp)
            return temp

    def oldestUnProcDepletion(self, node):
        curTime = time.time()
        temp = self.view.model.repoStorage[node].getOldestDeplUnProcMessage()
        self.view.model.repoStorage[node].deleteAnyMessage(temp['content'])
        self.view.model.repoStorage[node].addToDepletedUnProcMessages(temp)
        return temp

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