# -*- coding: utf-8 -*-
"""Computational Spot implementation
This module contains the implementation of a set of VMs residing at a node. Each VM is abstracted as a FIFO queue. 
"""
from __future__ import division
from collections import deque
import random
import abc
import copy

import numpy as np

from icarus.util import inheritdoc

__all__ = [
        'ComputationalSpot',
        'Task'
           ]

# Status codes
REQUEST = 0
RESPONSE = 1
TASK_COMPLETE = 2
# Admission results:
DEADLINE_MISSED = 0
CONGESTION = 1
SUCCESS = 2
CLOUD = 3
ERROR = 4

class Task(object):
    """
    A request to execute a service at a node
    """
    def __init__(self, time, deadline, rtt_delay, node, service, service_time, flow_id, receiver, finishTime=None):
        
        self.service = service
        self.time = time
        self.expiry = deadline
        self.rtt_delay = rtt_delay
        self.exec_time = service_time
        self.node = node
        self.flow_id = flow_id
        self.receiver = receiver
        self.finishTime = finishTime

    def print_task(self):
        print ("Task.service: " + repr(self.service))
        print ("Task.time: " + repr(self.time))
        print ("Task.deadline: " + repr(self.expiry))
        print ("Task.rtt_delay: " + repr(self.rtt_delay))
        print ("Task.exec_time: " + repr(self.exec_time))
        print ("Task.node: " + repr(self.node))
        print ("Task.flow_id: " + repr(self.flow_id))
        print ("Task.receiver: " + repr(self.receiver))
        print ("Task.finishTime: " + repr(self.finishTime))

class CpuInfo(object):
    """
    Information on running tasks, their finish times, etc. on each core of the CPU
    """
    
    def __init__(self, numOfCores):

        self.numOfCores = numOfCores
        # Hypothetical finish time of the last request (i.e., tail of the queue)
        self.coreFinishTime = [0]*self.numOfCores 
        # Currently running service instance at each cpu (scheduled earlier)
        self.coreService = [None]*self.numOfCores
        # Idle time of the server
        self.idleTime = 0.0

    def get_idleTime(self, time):
        
        # update the idle times
        for indx in range(0, self.numOfCores):
            if self.coreFinishTime[indx] < time:
                self.idleTime += time - self.coreFinishTime[indx]
    
        return self.idleTime

    def get_available_core(self, time):
        
        # update the running services 
        for indx in range(0, len(self.coreService)):
            if self.coreFinishTime[indx] <= time:
                self.idleTime += time - self.coreFinishTime[indx]
                self.coreFinishTime[indx] = time
                self.coreService[indx] = None
            
        indx = self.coreFinishTime.index(min(self.coreFinishTime))
        if self.coreFinishTime[indx] <= time:
            self.coreFinishTime[indx] = time
            return indx
        
        return None
    
    def get_next_available_core(self):
        indx = self.coreFinishTime.index(min(self.coreFinishTime)) 
        
        return indx

    def assign_task_to_core(self, core_indx, fin_time, service):
        
        if self.coreFinishTime[core_indx] > fin_time:
            raise ValueError("Error in assign_task_to_core: there is a running task")
        
        print ("Assigning task to core")
        self.coreService[core_indx] = service
        self.coreFinishTime[core_indx] = fin_time

    def update_core_status(self, time):
        
        for indx in range(0, len(self.coreService)):
            if self.coreFinishTime[indx] <= time:
                self.coreFinishTime[indx] = time
                self.idleTime += time - self.coreFinishTime[indx]
                self.coreService[indx] = None

    def print_core_status(self):
        for indx in range(0, len(self.coreService)):
            print ("Core: " + repr(indx) + " finish time: " + repr(self.coreFinishTime[indx]) + " service: " + repr(self.coreService[indx]))

class ComputationalSpot(object):
    """ 
    A set of computational resources, where the basic unit of computational resource 
    is a VM. Each VM is bound to run a specific service instance and abstracted as a 
    Queue. The service time of the Queue is extracted from the service properties. 
    """

    def __init__(self, numOfCores, n_services, services, node, dist=None):
        """Constructor

        Parameters
        ----------
        numOfCores: total number of VMs available at the computational spot
        n_services : number of services in the memory
        services : list of all the services (service population) with their attributes
        """

        if numOfCores == -1:
            self.numOfCores = len(services)
            self.is_cloud = True
        else:
            self.numOfCores = numOfCores
            self.is_cloud = False

        self.service_population = len(services)

        print ("Number of VMs @node: " + repr(node) + " " + repr(n_services))
        print ("Number of cores @node: " + repr(node) + " " + repr(numOfCores))

        if n_services < numOfCores:
            n_services = numOfCores*2
        
        if n_services > self.service_population:
            n_services = self.service_population

        # CPU info
        self.cpuInfo = CpuInfo(self.numOfCores)
        # num. of services in the memory (capacity)
        self.n_services = n_services
        # Task queue of the comp. spot
        self.taskQueue = []
        # num. of instances of each service in the memory
        self.numberOfInstances = [0]*self.service_population 

        # server missed requests (due to congestion)
        self.missed_requests = [0] * self.service_population
        # service request count (per service)
        self.running_requests = [0 for x in range(0, self.service_population)] #correct!
        # delegated service request counts (per service)
        self.delegated_requests = [0 for x in range(0, self.service_population)]
        
        self.services = services
        self.view = None
        self.node = node

        # TODO setup all the variables: numberOfInstances, cpuInfo, etc. ...

        num_services = 0
        if dist is None and self.is_cloud == False:
            # setup a random set of services to run in the memory
            while num_services < self.n_services:
                service_index = random.choice(range(0, self.service_population))
                if self.numberOfInstances[service_index] == 0:
                    self.numberOfInstances[service_index] = 1
                    num_services += 1

    def schedule(self, time):
        """
        Return the next task to be executed, if there is any.
        """

        core_indx = self.cpuInfo.get_available_core(time)
        
        if (len(self.taskQueue) > 0) and (core_indx is not None):
            coreService = self.cpuInfo.coreService
            for task_indx in range(0, len(self.taskQueue)):
                aTask = self.taskQueue[task_indx]
                serv_count = coreService.count(aTask.service)
                if self.numberOfInstances[aTask.service] > 0: 
                    available_vms = self.numberOfInstances[aTask.service] - serv_count
                    if available_vms > 0:
                        self.taskQueue.pop(task_indx)
                        self.cpuInfo.assign_task_to_core(core_indx, time + aTask.exec_time, aTask.service)
                        aTask.finishTime = time + aTask.exec_time
                        return aTask
                else: # This can happen during service replacement transitions
                    self.taskQueue.pop(task_indx)
                    self.cpuInfo.assign_task_to_core(core_indx, time + aTask.exec_time, service)
                    aTask.finishTime = time + aTask.exec_time
                    return aTask

        return None

    def simulate_execution(self, aTask, time):
        """
        Simulate the execution of tasks in the taskQueue and compute each
        task's finish time. 

        Parameters:
        -----------
        taskQueue: a queue (List) of tasks (Task objects) waiting to be executed
        cpuFinishTime: Current Finish times of CPUs
        """
        # Add the task to the taskQueue
        taskQueueCopy = self.taskQueue[:] #shallow copy


        taskQueueCopy.append(aTask)
        taskQueueSorted = sorted(taskQueueCopy, key=lambda x: x.expiry) #smaller to larger (absolute) deadline
        cpuInfoCopy = copy.deepcopy(self.cpuInfo)

        # Check if the queue has any task that misses its deadline (after adding this)
        #coreFinishTimeCopy = self.cpuInfo.coreFinishTime[:]
        #cpuServiceCopy = self.cpuService[:]
        
        for task_indx in range(0, len(taskQueueSorted)):
            taskQueueSorted[task_indx].print_task()

        cpuInfoCopy.print_core_status()
        sched_failed = False
        aTask = None
        while len(taskQueueSorted) > 0:
            if not sched_failed:
                core_indx = cpuInfoCopy.get_next_available_core()
            time = cpuInfoCopy.coreFinishTime[core_indx]
            cpuInfoCopy.update_core_status(time)
            sched_failed = False

            for task_indx in range(0, len(taskQueueSorted)):
                aTask = taskQueueSorted[task_indx]
                if self.numberOfInstances[aTask.service] > 0: 
                    serv_count = cpuInfoCopy.coreService.count(aTask.service)
                    #print ("Service count: " + repr(serv_count))
                    available_cores = self.numberOfInstances[aTask.service] - serv_count
                    #print ("Available cores: " + repr(available_cores))
                    if available_cores > 0:
                        taskQueueSorted.pop(task_indx)
                        cpuInfoCopy.assign_task_to_core(core_indx, time + aTask.exec_time, aTask.service)
                        aTask.finishTime = time + aTask.exec_time
                        break
                else: # This can happen during service replacement transitions
                    taskQueueSorted.pop(task_indx)
                    cpuInfoCopy.assign_task_to_core(core_indx, time + aTask.exec_time, service)
                    aTask.finishTime = time + aTask.exec_time
                    break
            else: # for loop concluded without a break
                sched_failed = True
                core_indx = cpuInfoCopy.coreService.index(aTask.service)

    def admit_task_EDF(self, service, time, flow_id, deadline, receiver, rtt_delay, controller):
        """
        Parameters
        ----------
        service : index of the service requested
        time    : current time (arrival time of the service job)

        Return
        ------
        comp_time : is when the task is going to be finished (after queuing + execution)
        vm_index : index of the VM that will execute the task
        """

        serviceTime = self.services[service].service_time
        if self.is_cloud:
            #aTask = Task(time, deadline, self.node, service, serviceTime, flow_id, receiver)
            controller.add_event(time+serviceTime, receiver, service, self.node, flow_id, deadline, rtt_delay, TASK_COMPLETE)
            print ("CLOUD: Accepting TASK")
            return [True, CLOUD]
        
        if self.numberOfInstances[service] == 0:
            print ("ERROR no instances in admit_task_EDF")
            return [False, ERROR]
        
        if deadline - rtt_delay - serviceTime < 0:
            print ("Refusing TASK: deadline already missed")
            return [False, DEADLINE_MISSED]
            
        aTask = Task(time, deadline, rtt_delay, self.node, service, serviceTime, flow_id, receiver)
        self.cpuInfo.update_core_status(time) #need to call before simulate
        #print ("About to call simulate_execution()")
        self.simulate_execution(aTask, time)
        #print ("Done calling simulate_execution()")
        for task in self.taskQueue:
            if (task.expiry - task.rtt_delay) < task.finishTime:
                self.missed_requests[service] += 1
                print("Refusing TASK: Congestion")
                return [False, CONGESTION]
        
        # New task can be admitted, add to service Queue
        self.taskQueue.append(aTask)
        self.taskQueue = sorted(self.taskQueue, key=lambda x: x.expiry) 
        self.running_requests[service] += 1
        # Run the next task (if there is any)
        newTask = self.schedule(time) 
        if newTask is not None:
            controller.add_event(newTask.finishTime, newTask.receiver, newTask.service, self.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE) 

        if self.numberOfInstances[service] == 0:
            print "Error: this should not happen in admit_task_EDF()"
        
        print ("Accepting Task")
        return [True, SUCCESS]
    
    def update_counters(self, time):
        
        for service in range(0, self.n_services):
            if self.vm_counts[service] > 0:
                self.getFinishTime(service, time)
            else:
                self.getVirtualTailFinishTime(service, time)
                
    def reassign_vm(self, serviceToReplace, newService, debug):
        """
        Instantiate service at the given vm
        """
        if self.numberOfInstances[serviceToReplace] == 0:
            raise ValueError("Error in reassign_vm: the service to replace has no instances")
            
        if True: #debug:
            print "Replacing service: " + repr(serviceToReplace) + " with: " + repr(newService) + " at node: " + repr(self.node)
        self.numberOfInstances[newService] += 1
        self.numberOfInstances[serviceToReplace] -= 1

    def getIdleTime(self, time):
        """
        Get the total idle time of the node
        """
        return self.cpuInfo.get_idleTime(time)

