# -*- coding: utf-8 -*-
"""Computation Spot implementation
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
        'ComputationSpot',
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
NO_INSTANCES = 4

class Task(object):
    """
    A request to execute a service at a node
    """
    def __init__(self, time, deadline, rtt_delay, node, service, service_time, flow_id, receiver, arrivalTime=None, completionTime=None):
        
        self.service = service
        self.time = time
        self.expiry = deadline
        self.rtt_delay = rtt_delay
        self.exec_time = service_time
        self.node = node
        self.flow_id = flow_id
        self.receiver = receiver
        self.completionTime = completionTime
        if arrivalTime is None:
            self.arrivalTime = time
        else:
            self.arrivalTime = arrivalTime

    def print_task(self):
        print ("Task.service: " + repr(self.service))
        print ("Task.time: " + repr(self.time))
        print ("Task.deadline: " + repr(self.expiry))
        print ("Task.rtt_delay: " + repr(self.rtt_delay))
        print ("Task.exec_time: " + repr(self.exec_time))
        print ("Task.node: " + repr(self.node))
        print ("Task.flow_id: " + repr(self.flow_id))
        print ("Task.receiver: " + repr(self.receiver))
        print ("Task.completionTime: " + repr(self.completionTime))

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
                self.coreFinishTime[indx] = time
        
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
        
        self.coreService[core_indx] = service
        self.coreFinishTime[core_indx] = fin_time

    def update_core_status(self, time):
        
        for indx in range(0, len(self.coreService)):
            if self.coreFinishTime[indx] <= time:
                self.idleTime += time - self.coreFinishTime[indx]
                self.coreFinishTime[indx] = time
                self.coreService[indx] = None

    def print_core_status(self):
        for indx in range(0, len(self.coreService)):
            print ("Core: " + repr(indx) + " finish time: " + repr(self.coreFinishTime[indx]) + " service: " + repr(self.coreService[indx]))

class ComputationSpot(object):
    """ 
    A set of computational resources, where the basic unit of computational resource 
    is a VM. Each VM is bound to run a specific service instance and abstracted as a 
    Queue. The service time of the Queue is extracted from the service properties. 
    """

    def __init__(self, model, numOfCores, n_services, services, node, sched_policy = "EDF", dist=None):
        """Constructor

        Parameters
        ----------
        numOfCores: total number of VMs available at the computational spot
        n_services : number of services in the memory
        services : list of all the services (service population) with their attributes
        """

        if numOfCores == float('inf'):
            self.numOfCores = len(services)
            n_services = len(services)
            self.is_cloud = True
        else:
            self.numOfCores = numOfCores
            self.is_cloud = False

        self.service_population = len(services)
        self.model = model

        print ("Number of VMs @node: " + repr(node) + " " + repr(n_services))
        print ("Number of cores @node: " + repr(node) + " " + repr(numOfCores))

        if n_services < numOfCores:
            n_services = numOfCores*2
        
        if n_services > self.service_population:
            n_services = self.service_population

        self.sched_policy = sched_policy

        # CPU info
        self.cpuInfo = CpuInfo(self.numOfCores)
        # num. of vms (memory capacity) #TODO rename this
        self.n_services = n_services
        # Task queue of the comp. spot
        self.taskQueue = []
        # Task queue for upcoming tasks
        self.upcomingTaskQueue = []
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
        self.rtt_upstream = [0.0 for x in range(0, self.service_population)]

        # TODO setup all the variables: numberOfInstances, cpuInfo, etc. ...

        num_services = 0
        if dist is None and self.is_cloud == False:
            # setup a random set of services to run in the memory
            while num_services < self.n_services:
                service_index = random.choice(range(0, self.service_population))
                if self.numberOfInstances[service_index] == 0:
                    self.numberOfInstances[service_index] = 1
                    num_services += 1
                    evicted = self.model.cache[node].put(service_index) # HACK: should use controller here   

    def schedule(self, time):
        """
        Return the next task to be executed, if there is any.
        """

        # TODO: This method can simply fetch the task with the smallest finish time
        # No need to repeat the same computation already carried out by simulate()

        core_indx = self.cpuInfo.get_available_core(time)
        
        # The following is to accommodate coordinated routing
        if len(self.upcomingTaskQueue) > 0:
            new_addition = False
            if self.sched_policy == 'EDF':
                self.upcomingTaskQueue = sorted(self.upcomingTaskQueue, key=lambda x: x.expiry)
            elif self.sched_policy == 'FIFO':
                self.upcomingTaskQueue = sorted(self.upcomingTaskQueue, key=lambda x: x.arrivalTime)
            else:
                raise ValueError("Invalid scheduling policy")
                print ("Error: This should not happen in admit_task(): " +repr(self.sched_policy))
            for task in self.upcomingTaskQueue[:]: 
                if time < task.arrivalTime:
                    continue
                aTask = self.upcomingTaskQueue.remove(task)
                self.taskQueue.append(task)
                new_addition = True
            if new_addition:
                if self.sched_policy == 'EDF':
                    # sort by deadline
                    self.taskQueue = sorted(self.taskQueue, key=lambda x: x.expiry)
                elif self.sched_policy == 'FIFO':
                    # sort by arrival time
                    self.taskQueue = sorted(self.taskQueue, key=lambda x: x.arrivalTime)
        
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
                        aTask.completionTime = time + aTask.exec_time
                        return aTask
                else: # This can happen during service replacement transitions
                    self.taskQueue.pop(task_indx)
                    self.cpuInfo.assign_task_to_core(core_indx, time + aTask.exec_time, aTask.service)
                    aTask.completionTime = time + aTask.exec_time
                    return aTask
        return None

    def compute_completion_times(self, time, debug=False):
        """
        Simulate the execution of tasks in the taskQueue and compute each
        task's finish time. 

        Parameters:
        -----------
        taskQueue: a queue (List) of tasks (Task objects) waiting to be executed
        cpuFinishTime: Current Finish times of CPUs
        """
        # TODO time argument does not seem to be used in this method, so omit it?
        self.cpuInfo.update_core_status(time)
        # Add the task to the taskQueue
        taskQueueCopy = self.taskQueue[:] #shallow copy
        upcomingTaskQueueCopy = self.upcomingTaskQueue[:] #shallow copy

        cpuInfoCopy = copy.deepcopy(self.cpuInfo)

        # Check if the queue has any task that misses its deadline (after adding this)
        if debug:
            for task_indx in range(0, len(taskQueueCopy)):
                taskQueueCopy[task_indx].print_task()
            cpuInfoCopy.print_core_status()

        sched_failed = False
        aTask = None
        while len(taskQueueCopy) > 0 or len(upcomingTaskQueueCopy) > 0:
            if not sched_failed:
                core_indx = cpuInfoCopy.get_next_available_core()
            time = cpuInfoCopy.coreFinishTime[core_indx]
            cpuInfoCopy.update_core_status(time)
            sched_failed = False
            
            # This part is to accommodate coordinated routing
            if len(taskQueueCopy) == 0 and len(upcomingTaskQueueCopy) > 0 and time < upcomingTaskQueueCopy[0].arrivalTime:
                time = upcomingTaskQueueCopy[0].arrivalTime
            if len(upcomingTaskQueueCopy) > 0:
                new_addition = False
                for task in upcomingTaskQueueCopy[:]:
                    if time < task.arrivalTime:
                        continue
                    upcomingTaskQueueCopy.remove(task)
                    taskQueueCopy.append(task)
                    new_addition = True

                if new_addition:
                    if self.sched_policy == 'EDF':
                        # sort by deadline
                        taskQueueCopy = sorted(taskQueueCopy, key=lambda x: x.expiry)
                    elif self.sched_policy == 'FIFO':
                        # sort by arrival time
                        taskQueueCopy = sorted(taskQueueCopy, key=lambda x: x.arrivalTime)

            for task_indx in range(0, len(taskQueueCopy)):
                aTask = taskQueueCopy[task_indx]
                if self.numberOfInstances[aTask.service] > 0: 
                    serv_count = cpuInfoCopy.coreService.count(aTask.service)
                    available_cores = self.numberOfInstances[aTask.service] - serv_count
                    if available_cores > 0:
                        taskQueueCopy.pop(task_indx)
                        cpuInfoCopy.assign_task_to_core(core_indx, time + aTask.exec_time, aTask.service)
                        aTask.completionTime = time + aTask.exec_time
                        if debug:
                            print("Completion time: " + repr(aTask.completionTime) + " Flow ID: " + repr(aTask.flow_id))
                        break
                else: # This can happen during service replacement transitions
                    taskQueueCopy.pop(task_indx)
                    cpuInfoCopy.assign_task_to_core(core_indx, time + aTask.exec_time, aTask.service)
                    aTask.completionTime = time + aTask.exec_time
                    if debug:
                        print("Completion time: " + repr(aTask.completionTime) + " Flow ID: " + repr(aTask.flow_id))
                    break
            else: # for loop concluded without a break
                sched_failed = True
                core_indx = cpuInfoCopy.coreService.index(aTask.service)

                

    def admit_task_FIFO(self, service, time, flow_id, deadline, receiver, rtt_delay, controller, debug):
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
            controller.add_event(time+serviceTime, receiver, service, self.node, flow_id, deadline, rtt_delay, TASK_COMPLETE)
            controller.execute_service(flow_id, service, self.node, time, self.is_cloud)
            if debug:
                print ("CLOUD: Accepting TASK")
            return [True, CLOUD]
        
        if self.numberOfInstances[service] == 0:
            return [False, NO_INSTANCES]
        
        if deadline - time - rtt_delay - serviceTime < 0:
            if debug:
                print ("Refusing TASK: deadline already missed")
            return [False, DEADLINE_MISSED]
        
        aTask = Task(time, deadline, rtt_delay, self.node, service, serviceTime, flow_id, receiver)
        self.taskQueue.append(aTask)
        #self.cpuInfo.update_core_status(time) #moved to compute_completion_times
        self.compute_completion_times(time, debug)
        for task in self.taskQueue[:]:
            if debug:
                print("After simulate:")
                task.print_task()
            if task.completionTime is None: 
                print("Error: a task with invalid completion time: " + repr(task.completionTime))
                raise ValueError("Task completion time is invalid")
            if (task.expiry - task.rtt_delay) < task.completionTime:
                self.missed_requests[service] += 1
                if debug:
                    print("Refusing TASK: Congestion")
                self.taskQueue.remove(aTask)
                return [False, CONGESTION]

        # New task can be admitted, add to service Queue
        self.running_requests[service] += 1
        # Run the next task (if there is any)
        newTask = self.schedule(time) 
        if newTask is not None:
            controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, self.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE) 
            controller.execute_service(newTask.flow_id, newTask.service, self.node, time, self.is_cloud)
        
        if self.numberOfInstances[service] == 0:
            print "Error: this should not happen in admit_task_FIFO()"
       
        if debug:
            print ("Accepting Task")
        return [True, SUCCESS]

    def admit_task_EDF(self, service, time, flow_id, deadline, receiver, rtt_delay, controller, debug):
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
            controller.add_event(time+serviceTime, receiver, service, self.node, flow_id, deadline, rtt_delay, TASK_COMPLETE)
            controller.execute_service(flow_id, service, self.node, time, self.is_cloud)
            if debug:
                print ("CLOUD: Accepting TASK")
            return [True, CLOUD]
        
        if self.numberOfInstances[service] == 0:
            return [False, NO_INSTANCES]
        
        if deadline - time - rtt_delay - serviceTime < 0:
            if debug:
                print ("Refusing TASK: deadline already missed")
            return [False, DEADLINE_MISSED]
            
        aTask = Task(time, deadline, rtt_delay, self.node, service, serviceTime, flow_id, receiver)
        self.taskQueue.append(aTask)
        self.taskQueue = sorted(self.taskQueue, key=lambda x: x.expiry) #smaller to larger (absolute) deadline
        self.compute_completion_times(time, debug)
        for task in self.taskQueue[:]:
            if debug:
                print("After simulate:")
                task.print_task()
            if task.completionTime is None: 
                print("Error: a task with invalid completion time: " + repr(task.completionTime))
                raise ValueError("Task completion time is invalid")
            if (task.expiry - task.rtt_delay) < task.completionTime:
                self.missed_requests[service] += 1
                if debug:
                    print("Refusing TASK: Congestion")
                self.taskQueue.remove(aTask)
                return [False, CONGESTION]
        
        # New task can be admitted, add to service Queue
        self.running_requests[service] += 1
        # Run the next task (if there is any)
        newTask = self.schedule(time) 
        if newTask is not None:
            controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, self.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE) 
            controller.execute_service(newTask.flow_id, newTask.service, self.node, time, self.is_cloud)

        if self.numberOfInstances[service] == 0:
            print "Error: this should not happen in admit_task_EDF()"
       
        if debug:
            print ("Accepting Task")
        return [True, SUCCESS]

    def admit_task(self, service, time, flow_id, deadline, receiver, rtt_delay, controller, debug):
        ret = None
        if self.sched_policy == "EDF":
            ret = self.admit_task_EDF(service, time, flow_id, deadline, receiver, rtt_delay, controller, debug)
        elif self.sched_policy == "FIFO":
            ret = self.admit_task_FIFO(service, time, flow_id, deadline, receiver, rtt_delay, controller, debug)
        else:
            print ("Error: This should not happen in admit_task(): " +repr(self.sched_policy))
            
        return ret
    
    def reassign_vm(self, serviceToReplace, newService, debug):
        """
        Instantiate service at the given vm
        """
        if self.numberOfInstances[serviceToReplace] == 0:
            raise ValueError("Error in reassign_vm: the service to replace has no instances")
            
        if debug:
            print "Replacing service: " + repr(serviceToReplace) + " with: " + repr(newService) + " at node: " + repr(self.node)
        self.numberOfInstances[newService] += 1
        self.numberOfInstances[serviceToReplace] -= 1

    def getIdleTime(self, time):
        """
        Get the total idle time of the node
        """
        return self.cpuInfo.get_idleTime(time)

