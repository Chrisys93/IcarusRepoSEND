# -*- coding: utf-8 -*-
"""Computation Spot implementation
This module contains the implementation of a set of VMs residing at a node. Each VM is abstracted as a FIFO queue. 
"""
from __future__ import division
from collections import deque
import random
import abc
import copy
import heapq 

import numpy as np

from icarus.util import inheritdoc

__all__ = [
        'ComputationSpot',
        'Task',
        'VM'
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

class VM(object):
    """
    A VM object to simulate a container, Unikernel, VM, etc.
    """
    # Time to instantiate a VM
    instantiationDuration = 0.8

    def __init__(self, compSpot, serviceType=None):
        ### Service type provided by the VM #
        self.service = serviceType # XXX this attribute is not necessary TODO remove
        ### Computation Spot where the VM is running #
        self.cs = compSpot
        ### Task that the VM is currently running #
        self.runningTask = None
        ### Next service that the VM is about to instantiate (if any) #
        self.nextService = None

    def set_service(self, service):
        self.service = service

    def setTask(self, task):
        self.runningTask = task
        
class Task(object):
    """
    A request to execute a service at a node
    """
    TASK_TYPE_SERVICE = 1
    TASK_TYPE_VM_START = 2
    def __init__(self, time, taskType, deadline, rtt_delay, node, service, service_time, flow_id, receiver, arrivalTime=None, completionTime=None):
        ### Type of the task #
        self.taskType = taskType
        ### The service associated with the task #
        self.service = service
        ### The VM that the task is scheduled to run at #
        self.vm = None 
        ### The current time? # XXX remove this if not used
        self.time = time
        ### Expiration time (absolute) of the task #
        self.expiry = deadline
        ### RTT delay that is incurred by the request so far #
        self.rtt_delay = rtt_delay
        ### Execution time of the task #
        self.exec_time = service_time
        ### The node that the task is running at #
        self.node = node
        ### The flow id that the request is part of #
        self.flow_id = flow_id
        ### The receiver that dispatched the task request #
        self.receiver = receiver
        ### Completion tim of the task #
        self.completionTime = completionTime
        ### Arrival time of the task at the scheduler queue #
        if arrivalTime is None:
            self.arrivalTime = time
        else:
            self.arrivalTime = arrivalTime
        ### Service to add in the case of VM_START task #
        self.serviceToInstantiate = None
        ### Next task to execute after this one (for VM reassignments) #
        self.nextTask = []

    def setServiceToInstantiate(self, service):
        if self.taskType != Task.TASK_TYPE_VM_START:
            raise ValueError ("Error: incorrect task type: " + str(self.taskType) +  " Expecting: " + str(Task.TASK_TYPE_VM_START))
        self.serviceToInstantiate = service

    def setNextTasktoRun(self, task):
        if self.taskType != Task.TASK_TYPE_SERVICE:
            raise ValueError ("Error: incorrect task type in setNextTasktoRun(): " + str(self.taskType) +  " Expecting: " + str(Task.TASK_TYPE_SERVICE))
        if task in self.nextTask:
            raise ValueError("Error in setNextTasktoRun(): task already in nextTask list")
        self.nextTask.append(task)

    def setVM(self, aVM):
        self.vm = aVM
    
    def getVM(self):
        return self.vm
    
    def print_task(self):
        print ("---Task.service: " + repr(self.service))
        print ("\tTask.time: " + repr(self.time))
        print ("\tTask.type: " + repr(self.taskType))
        print ("\tTask.deadline: " + repr(self.expiry))
        print ("\tTask.rtt_delay: " + repr(self.rtt_delay))
        print ("\tTask.exec_time: " + repr(self.exec_time))
        print ("\tTask.node: " + repr(self.node))
        print ("\tTask.flow_id: " + repr(self.flow_id))
        print ("\tTask.receiver: " + repr(self.receiver))
        print ("\tTask.completionTime: " + repr(self.completionTime))
        if self.serviceToInstantiate is not None:
            print("\tTask.serviceToInstantiate: " + str(self.serviceToInstantiate))
        if self.arrivalTime is not None:
            print ("\tTask.arrivalTime: " + repr(self.arrivalTime))
        if len(self.nextTask) > 0:
            print ("\tTask.nextTask: ")
            for atask in self.nextTask:
                atask.print_task()
        print ("\n")

    def __lt__(self, other):
        
        return self.completionTime < other.completionTime

class Scheduler(object):
    """
    Information on running tasks, their finish times, etc. on each core of the CPU.
    """
    
    def __init__(self, sched_policy, cs): 
        ### Number of CPU cores that can run VMs in parallel
        self.numOfCores = cs.numOfCores
        ### Hypothetical finish time of the last request (i.e., tail of the queue) #
        self.coreFinishTime = [0.0]*self.numOfCores 
        ### Computation Spot #
        self.cs = cs
        ### Currently running service instance at each cpu #
        self.runningServices = [None]*self.numOfCores
        ### Currently running Task instance at each cpu #
        self.runningTasks = [None]*self.numOfCores
        ### Currently queued service instances  #
        self.queuedServices = [0] * cs.service_population_size
        ### currently queued services by receiver #
        self.queuedServicesPerReceiver = [[0]*cs.service_population_size for x in cs.model.topology.receivers()]
        ### Idle time of the cs #
        self.idleTime = 0.0
        ### Task queue #
        self._taskQueue = []
        ### Task queue for upcoming tasks that will arrive in the future (i.e., for coordinated strategy) #
        self.upcomingTaskQueue = []
        ### Schedule policy #
        self.sched_policy = sched_policy
        ### Request count #
        self.taskCount = 0 
        ### Total task waiting time
        self.waitingTime = 0.0
        ### VMs: busy (carrying out a task), idle, and starting (booting up a service) 
        self.busyVMs = {x:[] for x in range(len(self.cs.services))}
        self.idleVMs = {x:[] for x in range(len(self.cs.services))}
        self.startingVMs = {x:[] for x in range(len(self.cs.services))}

    def get_idleTime(self, time):
        
        # update the idle times
        for indx in range(0, self.numOfCores):
            if self.coreFinishTime[indx] < time:
                self.idleTime += time - self.coreFinishTime[indx]
                self.coreFinishTime[indx] = time
        
        return self.idleTime
    
    def removeFromTaskQueue(self, aTask):
        self._taskQueue.remove(aTask)
        if aTask.taskType == Task.TASK_TYPE_SERVICE:
            self.queuedServices[aTask.service] -= 1
            self.queuedServicesPerReceiver[int(aTask.receiver[4:])][aTask.service] -= 1

    def addVMStartToTaskQ(self, task, curr_time, arrival_time, update_arrival_time = True):
        """ task.nextTask is a VM_START task. Add this task to the taskQ ensuring that 
            existing tasks in the taskQ still meet their deadlines.
        """
        # NOTE: need to make sure the task is added to the end of the queue otherwise existing tasks might miss their deadlines.
        # However, setting expiry of the VM_Start task to greater than the largest expiry in the queue still DOES NOT guarantee all tasks will meet their deadlines:
        # In rare cases, a task that is stalled due to insufficient VMs may end up waiting for the VM_START task which will take up CPU
        
        for next_task in task.nextTask:
            if next_task not in self.upcomingTaskQueue:
                if next_task not in self._taskQueue:
                    print ("Error in addVMStartToTaskQ(): next_task not in upcomingTaskQueue or taskQ. task:")
                    task.print_task()
                    print ("Printing task queue:")
                    for aTask in self._taskQueue:
                        aTask.print_task()
                    print ("Printing upcoming task queue:")
                    for aTask in self.upcomingTaskQueue:
                        aTask.print_task()
                    raise ValueError ("Error in addVMStartToTaskQ(): next_task is neither in upcomingTaskQueue nor taskQ")
                    if next_task.taskType != Task.TASK_TYPE_VM_START:
                        raise ValueError ("Error in complete_service_task(): nextTask is expected to be a VM_START task")
            else: # next_task in self.upcomingTaskQueue

                if arrival_time > curr_time:
                    next_task.arrivalTime = arrival_time
                elif arrival_time == curr_time:
                    self.upcomingTaskQueue.remove(next_task)
                    # Add the next_task to the taskQ
                    if len(self._taskQueue) > 0:
                        if self.sched_policy == 'EDF':
                            next_task.expiry = self._taskQueue[-1].expiry 
                        elif self.sched_policy == 'FIFO':
                            next_task.arrivalTime = time
                        else:
                            raise ValueError("Invalid scheduling policy")
                        self.addToTaskQueue(next_task, curr_time, update_arrival_time)
                    else:
                        # Run this task (if there is available CPU resources)
                        self.addToTaskQueue(next_task, curr_time, update_arrival_time)
                else:
                    raise ValueError("Error in addVMStartToTaskQ(): arrival_time for the task: " + str(arrival_time) + " should not be smaller than curr_time: " + str(curr_time))

    def addToTaskQueue(self, aTask, time, update_arrival_time = True):

        self._taskQueue.append(aTask)

        if update_arrival_time is True:
            aTask.arrivalTime = time

        if self.sched_policy == 'EDF':
            self._taskQueue = sorted(self._taskQueue, key=lambda x: x.expiry)
        elif self.sched_policy == 'FIFO':
            self._taskQueue = sorted(self._taskQueue, key=lambda x: x.arrivalTime)
        else:
            raise ValueError("Invalid scheduling policy")

        if aTask.taskType == Task.TASK_TYPE_SERVICE:
            self.queuedServicesPerReceiver[int(aTask.receiver[4:])][aTask.service] += 1
            self.queuedServices[aTask.service] += 1
        
    def addToUpcomingTaskQueue(self, aTask, time):
        self.upcomingTaskQueue.append(aTask)
        self.upcomingTaskQueue = sorted(self.upcomingTaskQueue, key=lambda x: x.arrivalTime)

    def get_available_core(self, time, update_arrival_times=True):
        
        self.update_state(time, update_arrival_times)   
        indx = self.coreFinishTime.index(min(self.coreFinishTime))
        if self.coreFinishTime[indx] <= time:
            self.coreFinishTime[indx] = time
            return indx
        
        return None
    
    def get_next_available_core(self):
        indx = self.coreFinishTime.index(min(self.coreFinishTime)) 
        
        return indx

    def get_next_task_admit_time(self, time):
        indx = self.get_next_available_core()
        if self.coreFinishTime[indx] < time:
            self.coreFinishTime[indx] = time
        return self.coreFinishTime[indx]

    def get_next_finish_time(self, time): 
        """
        Return the smallest of the finish times of the currently *busy* Cores.
        """
        minFinishTime = time
        for indx in range(0, self.numOfCores):
            if self.coreFinishTime[indx] > time: 
                if minFinishTime == time:
                    minFinishTime = self.coreFinishTime[indx]
                else:
                    if self.coreFinishTime[indx] < minFinishTime:
                        minFinishTime = self.coreFinishTime[indx]

        return minFinishTime                

    def assign_task_to_cpu_core(self, core_indx, aTask, time):
        
        if self.coreFinishTime[core_indx] > time:
            raise ValueError("Error in assign_task_to_cpu_core: there is a running task")
        
        self.runningServices[core_indx] = aTask.service
        self.runningTasks[core_indx] = aTask
        self.coreFinishTime[core_indx] = aTask.completionTime
        taskWaitingTime = time - aTask.arrivalTime 
        if taskWaitingTime < 0:
            print ("Error in assign_task_to_cpu_core(): Task arrival time is after current time")
            aTask.print_task()
            raise ValueError("This should not happen: Queue waiting time is below 0")
        self.waitingTime += taskWaitingTime
        self.taskCount += 1

    def update_state(self, time, update_arrival_times = True):
        """
        Updates the completion times of currently running tasks according to
        the current time and returns the number of running tasks
        time : current time
        """
        
        # The following is to accommodate coordinated routing
        ### Move any tasks in the upcomingTasksQueue to taskQueue if they have already arrived
        if len(self.upcomingTaskQueue) > 0:
            new_addition = False
            for task in self.upcomingTaskQueue[:]: 
                if time < task.arrivalTime:
                    continue
                aTask = self.upcomingTaskQueue.remove(task)
                self.addToTaskQueue(task, time, update_arrival_times)
                new_addition = True
        numRunning = 0   
        for indx in range(0, self.numOfCores):
            if self.coreFinishTime[indx] <= time:
                self.idleTime += time - self.coreFinishTime[indx]
                self.coreFinishTime[indx] = time
                self.runningServices[indx] = None
                self.runningTasks[indx] = None
            else:
                numRunning += 1

        return numRunning
    
    # This needs to be modified as it is unable to mimic the real scheduling of VMs that go through a startup process. 
    # The problem is that there needs to be a delay before a VM is ready to use (i.e., becomes idle) and this is not done
    # in this method.
    def schedule_without_vm_allocation(self, time, upcomingVMStartEvents, numVMs, numStartingVMs, fail_if_no_vm = True, debug=True):
        """
        Return the next task to be executed, if there is any.
        """
        core_indx = self.get_available_core(time, False)
        
        if time == float('inf') or time is None:
            raise ValueError("Error in schedule(): time is invalid - " + str(time))
        
        # The following is to accommodate coordinated routing
        if len(self.upcomingTaskQueue) > 0:
            new_addition = False
            self.upcomingTaskQueue = sorted(self.upcomingTaskQueue, key=lambda x: x.arrivalTime)
            for task in self.upcomingTaskQueue[:]: 
                if time < task.arrivalTime:
                    continue
                self.upcomingTaskQueue.remove(task)
                self.addToTaskQueue(task, time, False)
                new_addition = True

        for aTask in upcomingVMStartEvents[:]:
            if aTask.completionTime <= time:
                service = aTask.serviceToInstantiate
                numVMs[service] += 1
                numStartingVMs[service] -= 1
                heapq.heappop(upcomingVMStartEvents)
            else:
                break
        
        if (len(self._taskQueue) > 0) and (core_indx is not None):
            runningServices = self.runningServices
            runningTasks = self.runningTasks
            for aTask in self._taskQueue[:]:
                serv_count = runningServices.count(aTask.service)
                number_of_vms = numVMs[aTask.service]
                if number_of_vms > 0: 
                    available_vms = number_of_vms - serv_count
                    if available_vms > 0:
                        #self.taskQueue.remove(aTask)
                        self.removeFromTaskQueue(aTask)
                        aTask.completionTime = time + aTask.exec_time
                        if debug:
                            print("Schedule_without_vm_allocation: Task is scheduled to run:")
                            aTask.print_task()

                        if aTask.taskType == Task.TASK_TYPE_VM_START:
                            heapq.heappush(upcomingVMStartEvents, aTask)
                            newService = aTask.serviceToInstantiate 
                            oldService = aTask.service
                            numStartingVMs[newService] += 1
                            numVMs[oldService] -= 1
                            self.assign_task_to_cpu_core(core_indx, aTask, time)

                        elif aTask.taskType == Task.TASK_TYPE_SERVICE:    
                            self.assign_task_to_cpu_core(core_indx, aTask, time)
                        
                        return aTask
                    else:
                        if debug:
                            print("No idle VMs for service: " + str(aTask.service))
                else:
                    if fail_if_no_vm is True:
                    # Under strange circumstances where one service is both being replaced and added
                    # there may be no instances available for a task that is being considered.
                        #print ("This should not happen in schedule_without_vm_allocation(): no instances")
                        #aTask.print_task()
                        #print ("startingVMs: " + str(len(self.cs.scheduler.startingVMs[aTask.service]) ) ) 
                        #print ("idleVMs: " + str(len(self.cs.scheduler.idleVMs[aTask.service]) ) ) 
                        #print ("busyVMs: " + str(len(self.cs.scheduler.busyVMs[aTask.service]) ) ) 
                        #print ("The number of VM instances: " + str(self.cs.numberOfVMInstances[aTask.service]))
                        #print ("taskQ for copy: ")
                        #for task in self._taskQueue:
                        #    task.print_task()
                        #print ("taskQ for real scheduler ")
                        #for task in self.cs.scheduler._taskQueue:
                        #    task.print_task()
                        #raise ValueError("Error in schedule_without_vm_allocation(): unable to find a VM for a service: " + str(aTask.service))
                        aTask.completionTime = float('inf')
                        return aTask
                    else:
                        aTask.completionTime = float('inf')
                        return aTask
        return None

    def try_task_schedule(self, time, task):
        """
        Attempt to schedule the given task if available CPU and no other tasks with higher priority.
        """
        core_indx = self.get_available_core(time)
        if core_indx is None:
            return False

        if task not in self._taskQueue:
            raise ValueError("This should not happen in try_task_schedule(): task to schedule is not in taskQ")

        # The following is to accommodate coordinated routing
        if len(self.upcomingTaskQueue) > 0:
            self.upcomingTaskQueue = sorted(self.upcomingTaskQueue, key=lambda x: x.arrivalTime)
            for aTask in self.upcomingTaskQueue[:]: 
                if time < aTask.arrivalTime:
                    continue
                self.upcomingTaskQueue.remove(aTask)
                self.addToTaskQueue(aTask, time)

        aTask = self._taskQueue[0]
        if aTask == task and len(self.idleVMs[aTask.service]) > 0: 
            self.removeFromTaskQueue(task)
            task.completionTime = time + task.exec_time
            self.cs.run_task(core_indx, task, time)
            return True
        else:
            return False

    def schedule(self, time, debug=False):
        """
        Return the next task to be executed, if there is any.
        """
        if debug:
            print("Schedule is called at time: " + str(time))
        if time == float('inf') or time is None:
            raise ValueError("Error in schedule(): time is invalid - " + str(time))

        core_indx = self.get_available_core(time)
        if core_indx is None:
            return None
        
        # The following is to accommodate coordinated routing
        if len(self.upcomingTaskQueue) > 0:
            self.upcomingTaskQueue = sorted(self.upcomingTaskQueue, key=lambda x: x.arrivalTime)
            for task in self.upcomingTaskQueue[:]: 
                if time < task.arrivalTime:
                    continue
                self.upcomingTaskQueue.remove(task)
                self.addToTaskQueue(task, time)
        
        if (len(self._taskQueue) > 0) and (core_indx is not None):
            for aTask in self._taskQueue[:]:
                if len(self.idleVMs[aTask.service]) > 0: 
                    self.removeFromTaskQueue(aTask)
                    aTask.completionTime = time + aTask.exec_time
                    self.cs.run_task(core_indx, aTask, time)
                    if debug:
                        print("Schedule: Task is scheduled to run:")
                        aTask.print_task()
                    if len(aTask.nextTask) > 0:
                        for nTask in aTask.nextTask:
                            nTask.arrivalTime = aTask.completionTime
                    return aTask
                else:
                    if debug:
                        print("No idle VMs for service: " + str(aTask.service))
                    if (len(self.idleVMs[aTask.service]) + len(self.busyVMs[aTask.service])) <= 0:
                        print ("This should not happen in scheduler: no instances, task:")
                        aTask.print_task()
                        raise ValueError("Error in schedule(): unable to find a VM for a service" + str(aTask.service))
                    self.cs.insufficientVMEvents[aTask.service] += 1
        return None
    
    def print_core_status(self):
        for indx in range(0, self.numOfCores):
            print ("\tCore: " + repr(indx) + " finish time: " + repr(self.coreFinishTime[indx]) + " service: " + repr(self.runningServices[indx]))

class ComputationSpot(object):
    """ 
    A set of computational resources, where the basic unit of computational resource 
    is a VM. Each VM is bound to run a specific service instance and abstracted as a 
    Queue. The service time of the Queue is extracted from the service properties. 
    """
    #services     : list of all the services (service population) with their attributes.
    services = None
    def __init__(self, model, numOfCores, numOfVMs, services, node, sched_policy = "EDF", dist=None):
        """Constructor

        Parameters
        ----------
        numOfCores   : total number of VMs available at the computational spot.
        numOfVMs     : number of VMs in the memory.  
        node         : node_id of the computation spot in the topology.
        sched_policy : scheduling policy when assigning queued tasks to CPU. 
        """

        self.services = services #TODO this is a bit awkward: both instance and class variable
        if numOfCores == float('inf'):
            self.numOfCores = len(services)
            self.numOfVMs = len(services)
            self.is_cloud = True
        else:
            self.numOfCores = numOfCores
            self.numOfVMs = numOfVMs
            self.is_cloud = False
        
        self.service_population_size = len(services)
        self.model = model

        if numOfVMs < numOfCores:
            self.numOfVMs = numOfCores * 2
        
        if numOfVMs > self.service_population_size:
            self.numOfVMs = self.service_population_size

        print ("Number of VMs @node: " + repr(node) + " " + repr(self.numOfVMs))
        print ("Number of cores @node: " + repr(node) + " " + repr(self.numOfCores))

        # number of VMs in the memory (capacity) 
        self.numOfVMs = numOfVMs
        ### number of VM instances of each service in the memory #
        self.numberOfVMInstances = [0]*self.service_population_size
        ### the below variable is for the optimal placement strategy
        self.serviceProbabilities = [0.0]*self.service_population_size

        # Scheduler
        self.scheduler = Scheduler(sched_policy, self)
        # A copy of the scheduler for making admission decisions
        self.schedulerCopy = None 

        # server missed requests (due to congestion)
        self.missed_requests = [0] * self.service_population_size
        # service requests that were missed due to insufficient/non-existent VMs
        self.insufficientVMEvents = [0] * self.service_population_size
        # service request count (per service)
        self.running_requests = [0 for x in range(0, self.service_population_size)]
        # delegated (i.e., upstream) service request counts (per service)
        self.delegated_requests = [0 for x in range(0, self.service_population_size)]
        # The list of all the VMs belonging to the Computation Spot
        self.allVMs = []
        
        self.view = None
        self.node = node
        self.rtt_upstream = [0.0 for x in range(0, self.service_population_size)]

        num_of_VMs = 0
        if dist is None and self.is_cloud == False:
            # setup a random set of services to run in the memory
            while num_of_VMs < self.numOfVMs:
                service_index = random.choice(range(0, self.service_population_size))
                #if self.numberOfVMInstances[service_index] == 0:
                self.numberOfVMInstances[service_index] += 1
                num_of_VMs += 1
                aVM = VM(self, service_index)
                self.allVMs.append(aVM)
                self.scheduler.idleVMs[service_index].append(aVM)
                evicted = self.model.cache[node].put(service_index) # HACK: should use controller here       
        if self.is_cloud == False:
            for service in range(len(ComputationSpot.services)):
                if self.numberOfVMInstances[service] != len(self.scheduler.idleVMs[service]) + len(self.scheduler.busyVMs[service]) + len(self.scheduler.startingVMs[service]):
                    print ("Error in setting up the VMs at node: " + str(self.node))
                    print ("numberOfInstances = " + str(self.numberOfVMInstances[service]))
                    print ("Total VMs: " + str(len(self.scheduler.idleVMs[service]) + len(self.scheduler.busyVMs[service]) + len(self.scheduler.startingVMs[service])) )
                    print ("\t Idle: " + str(len(self.scheduler.idleVMs[service])) + " Busy: " + str(len(self.scheduler.busyVMs[service])) + " Starting: " + str(len(self.scheduler.startingVMs[service])) )
                    raise ValueError("Error in setting up the VMs")


    def complete_VM_start(self, controller, task, time):
        service = task.serviceToInstantiate
        if service is None:
            print ("Error in complete_VM_start(): service to instantiate is not specified")
            raise ValueError("missing task field service to instantiate")
        if len(self.scheduler.startingVMs[service]) <= 0:
            print("Error in complete_VM_start(): No VM instance to start for service: " + str(service))
            raise ValueError("No VM instance to start")
        vm = self.scheduler.startingVMs[service].pop(0)
        self.scheduler.idleVMs[service].append(vm)
        vm.set_service(service)

    def complete_service_task(self, controller, task, time):
        vm = task.getVM()
        if vm is None:
            print ("Error in complete_service_task(): completed service task is missing a VM")
            raise ValueError("invalid task")
        
        self.scheduler.busyVMs[task.service].remove(vm)
        self.scheduler.idleVMs[task.service].append(vm)
        
        if len(task.nextTask) > 0: 
            self.scheduler.addVMStartToTaskQ(task, time, time)
            while True:
                newTask = self.scheduler.schedule(time) 
                if newTask is not None:
                #print("In complete_service_task(): scheduling a VM reassign: " + str(newTask.service) + " to " + str(newTask.serviceToInstantiate))
                    controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, self.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask) 
                else:
                    break

    def complete_task(self, controller, task, time):
        if task.taskType == Task.TASK_TYPE_VM_START:
            self.complete_VM_start(controller, task, time)
        elif task.taskType == Task.TASK_TYPE_SERVICE:
            self.complete_service_task(controller, task, time)
        else:
            raise ValueError("Error in complete_task(): Invalid taskType " + str(task.taskType))
    
    def run_task(self, core_indx, aTask, time):
        self.scheduler.assign_task_to_cpu_core(core_indx, aTask, time)
        self.assign_task_to_vm(aTask)

    def assign_task_to_vm(self, aTask):
        aVM = self.scheduler.idleVMs[aTask.service].pop(0)
        if aTask.taskType == Task.TASK_TYPE_VM_START:
            self.scheduler.startingVMs[aTask.serviceToInstantiate].append(aVM)
        elif aTask.taskType == Task.TASK_TYPE_SERVICE:
            self.scheduler.busyVMs[aTask.service].append(aVM)
        else:
            raise ValueError("Invalid TaskType: " + str(aTask.taskType))
        aTask.setVM(aVM)
        aVM.setTask(aTask)

    def get_latest_service_task(self):
        """ Return the task with the largest expiry for any service task
        """
        latest_time = 0.0
        task = None

        # To accommodate coordinated strategy which schedules future service tasks to arrive.
        for aTask in self.scheduler.upcomingTaskQueue:
            if aTask.taskType == Task.TASK_TYPE_SERVICE and aTask.arrivalTime > latest_time:
                latest_time = aTask.arrivalTime
                task = aTask

        if task is not None:
            return task

        for aTask in self.scheduler._taskQueue:
            if self.scheduler.sched_policy == 'EDF':
                if aTask.taskType == Task.TASK_TYPE_SERVICE and aTask.expiry > latest_time:
                    latest_time = aTask.expiry
                    task = aTask
            elif self.scheduler.sched_policy == 'FIFO':
                if aTask.taskType == Task.TASK_TYPE_SERVICE and aTask.arrivalTime > latest_time:
                    latest_time = aTask.arrivalTime
                    task = aTask
            else:
                raise ValueError("In get_latest_service_task(): invalid schedule policy: " + str(self.scheduler.sched_policy))
        
        return task

    def get_latest_task(self, service):
        """ Return the task with the largst expiry for the given service.
        """

        latest_deadline = 0.0
        task = None
        for aTask in self.scheduler._taskQueue:
            if aTask.taskType == Task.TASK_TYPE_SERVICE and aTask.service == service and aTask.expiry > latest_deadline:
                latest_deadline = aTask.expiry
                task = aTask

        # Below is added to accommodate coordinated strategy
        for aTask in self.scheduler.upcomingTaskQueue:
            if aTask.taskType == Task.TASK_TYPE_SERVICE and aTask.service == service and aTask.expiry > latest_deadline:
                latest_deadline = aTask.expiry
                task = aTask
                print ("In get_latest_task(): task selected from upcomingTaskQueue:")
                task.print_task()

        return task
    
    def compute_completion_times(self, time, fail_if_no_vm = True, debug=False):
        """
        Simulate the execution of tasks in the taskQueue and compute each
        task's finish time. 

        Parameters:
        -----------
        taskQueue: a queue (List) of tasks (Task objects) waiting to be executed
        cpuFinishTime: Current Finish times of CPUs
        """
        if debug:
            print("\tComputing completion times for node: " + str(self.node))
        self.schedulerCopy = copy.copy(self.scheduler)
        # The below copy operations are necessary as copy.copy only makes 
        # a shallow copy, which simply copies references of the following two 
        # lists in scheduler to schedulerCopy. 
        self.schedulerCopy._taskQueue = copy.copy(self.scheduler._taskQueue)
        self.schedulerCopy.upcomingTaskQueue = copy.copy(self.scheduler.upcomingTaskQueue)
        self.schedulerCopy.coreFinishTime = copy.copy(self.scheduler.coreFinishTime)
        self.schedulerCopy.runningServices = copy.copy(self.scheduler.runningServices)

        numVMs = {x:len(self.scheduler.idleVMs[x]) + len(self.scheduler.busyVMs[x]) for x in range(len(self.services))}
        # Copy on read/write
        numStartingVMs = {x:len(self.scheduler.startingVMs[x]) for x in range(len(self.services))}
        upcomingVMStartEvents = []
        for service in range(self.service_population_size):
            for aVM in self.scheduler.startingVMs[service]:
                heapq.heappush(upcomingVMStartEvents, aVM.runningTask)
                
        numRunning = self.schedulerCopy.update_state(time, False)
        
        if debug:
            print ("Time = " + str(time))
            print ("\tNumber of VMs for node: " + str(self.node))
            print (numVMs)
            print("\tPrinting task queue for node: " + str(self.node))
            for task_indx in range(0, len(self.schedulerCopy._taskQueue)):
                self.schedulerCopy._taskQueue[task_indx].print_task()
            print("\tPrinting upcoming task queue for node: " + str(self.node))
            for task_indx in range(0, len(self.schedulerCopy.upcomingTaskQueue)):
                self.schedulerCopy.upcomingTaskQueue[task_indx].print_task()
            self.schedulerCopy.print_core_status()

        while len(self.schedulerCopy._taskQueue) > 0 or len(self.schedulerCopy.upcomingTaskQueue) > 0:
            taskToExecute = self.schedulerCopy.schedule_without_vm_allocation(time, upcomingVMStartEvents, numVMs, numStartingVMs, fail_if_no_vm, debug)
            #if fail_if_no_vm is False and taskToExecute is not None and taskToExecute.completionTime == float('inf'):
            if taskToExecute is not None and taskToExecute.completionTime == float('inf'):
                break
            if debug:
                print ("Time = " + str(time))
                if taskToExecute is not None:
                    print ("\tTask to execute for flow: " + str(taskToExecute.flow_id))
                else:
                    print ("\tNo task to execute")
                    print ("Current time: " + str(time))
                    if len(self.schedulerCopy.upcomingTaskQueue) > 0: 
                        print ("Upcoming task queue not empty: " + str(len(self.schedulerCopy.upcomingTaskQueue)))
                        for task in self.schedulerCopy.upcomingTaskQueue:
                            task.print_task()
                    if len(self.schedulerCopy._taskQueue) > 0: 
                        print ("task queue not empty: " + str(len(self.schedulerCopy._taskQueue)))
                        for task in self.schedulerCopy._taskQueue:
                            task.print_task()

            # add nextTask to the taskQ 
            if taskToExecute is not None and len(taskToExecute.nextTask) > 0: 
                self.schedulerCopy.addVMStartToTaskQ(taskToExecute, time, taskToExecute.completionTime, False)

            next_time = self.schedulerCopy.get_next_task_admit_time(time)

            if taskToExecute is None and next_time <= time:
                if len(self.schedulerCopy._taskQueue) == 0 and len(self.schedulerCopy.upcomingTaskQueue) > 0:
                    # if nothing left in the taskQueue but there are tasks
                    # in the upcomingTaskQueue, then fast-forward time
                    next_time = self.schedulerCopy.upcomingTaskQueue[0].arrivalTime
                elif len(self.schedulerCopy._taskQueue) > 0:
                    # it may happen that scheduler fails to execute 
                    # a task because of VM unavailability
                    # and not due to computational resource unavailability.
                    # In that case, fast-forward time.
                    next_time = self.schedulerCopy.get_next_finish_time(time)
            # print "Scheduling: " + repr(taskToExecute)
            time = next_time
            if time == float('inf'):
                print ("Error: Time is infinite!")
                break
            numRunning = self.schedulerCopy.update_state(time, False)
            if debug:
                print ("NumRunning: " + str(numRunning))

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
            aTask = Task(time, Task.TASK_TYPE_SERVICE, deadline, rtt_delay, self.node, service, serviceTime, flow_id, receiver)
            controller.add_event(time+serviceTime, receiver, service, self.node, flow_id, deadline, rtt_delay, TASK_COMPLETE, aTask)
            controller.execute_service(flow_id, service, self.node, time, self.is_cloud)
            if debug:
                print ("CLOUD: Accepting TASK")
            return [True, CLOUD]
        
        if self.numberOfVMInstances[service] == 0:
            return [False, NO_INSTANCES]
        
        if deadline - time - rtt_delay - serviceTime < 0:
            if debug:
                print ("Refusing TASK: deadline already missed")
            return [False, DEADLINE_MISSED]
        
        # check if there is a VM that already up; otherwise, reject. 
        # NOTE: numberOfVMInstances variable provides a count of VMs per services at steady state. 
        # It doesn't provide the actual count during VM instantiation (i.e., transition) period
        numVMs = len(self.scheduler.idleVMs[service]) + len(self.scheduler.busyVMs[service]) 
        if numVMs <= 0:
            return [False, NO_INSTANCES]
        aTask = Task(time, Task.TASK_TYPE_SERVICE, deadline, rtt_delay, self.node, service, serviceTime, flow_id, receiver)
        self.scheduler.addToTaskQueue(aTask, time)
        self.compute_completion_times(time, True, debug)
        for task in self.scheduler._taskQueue[:]:
            if debug:
                print("After simulate:")
                task.print_task()
            if task.taskType == Task.TASK_TYPE_VM_START:
                continue
            if task.completionTime is None: 
                print("Error: a task with invalid completion time: " + repr(task.completionTime))
                raise ValueError("Task completion time is invalid")
            if (task.expiry - task.rtt_delay) < task.completionTime:
                self.missed_requests[service] += 1
                if debug:
                    print("Refusing TASK: Congestion")
                self.scheduler.removeFromTaskQueue(aTask)
                return [False, CONGESTION]

        # New task can be admitted, add to service Queue
        self.running_requests[service] += 1
        # Run the next task (if there is any)
        newTask = self.scheduler.schedule(time) 
        if newTask is not None:
            controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, self.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask) 
        
        if self.numberOfVMInstances[service] == 0:
            print ("Error: this should not happen in admit_task_FIFO()")
       
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
            aTask = Task(time, Task.TASK_TYPE_SERVICE, deadline, rtt_delay, self.node, service, serviceTime, flow_id, receiver)
            controller.add_event(time+serviceTime, receiver, service, self.node, flow_id, deadline, rtt_delay, TASK_COMPLETE, aTask)
            controller.execute_service(flow_id, service, self.node, time, self.is_cloud)
            if debug:
                print ("CLOUD: Accepting TASK")
            return [True, CLOUD]
        
        if self.numberOfVMInstances[service] == 0:
            return [False, NO_INSTANCES]
        
        if len(self.scheduler.busyVMs[service]) + len(self.scheduler.idleVMs[service]) <= 0:
            return [False, NO_INSTANCES]
        
        if deadline - time - rtt_delay - serviceTime < 0:
            if debug:
                print ("Refusing TASK: deadline already missed")
            return [False, DEADLINE_MISSED]
        
        # check if there is a VM that already up; otherwise, reject. 
        # NOTE: numberOfVMInstances variable provides a count of VMs per services at steady state. 
        # It doesn't provide the actual count during VM instantiation (i.e., transition) period
        numVMs = len(self.scheduler.idleVMs[service]) + len(self.scheduler.busyVMs[service]) 
        if numVMs <= 0:
            return [False, NO_INSTANCES]

        aTask = Task(time, Task.TASK_TYPE_SERVICE, deadline, rtt_delay, self.node, service, serviceTime, flow_id, receiver)
        self.scheduler.addToTaskQueue(aTask, time)
        self.compute_completion_times(time, True, debug)
        for task in self.scheduler._taskQueue[:]:
            if debug:
                print("After simulate:")
                task.print_task()
            if task.taskType == Task.TASK_TYPE_VM_START:
                continue
            if task.completionTime is None: 
                print("Error: a task with invalid completion time: " + repr(task.completionTime))
                raise ValueError("Task completion time is invalid")
            if (task.expiry - task.rtt_delay) < task.completionTime:
                self.missed_requests[service] += 1
                if debug:
                    print("Refusing TASK: Congestion")
                self.scheduler.removeFromTaskQueue(aTask)
                return [False, CONGESTION]
        
        # New task can be admitted, add to service Queue
        self.running_requests[service] += 1
        # Run the next task (if there is any)
        newTask = self.scheduler.schedule(time) 
        if newTask is not None:
            controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, self.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask) 

        if self.numberOfVMInstances[service] == 0:
            print ("Error: this should not happen in admit_task_EDF()")
       
        if debug:
            print ("Accepting Task")
        return [True, SUCCESS]

    def admit_task(self, service, time, flow_id, deadline, receiver, rtt_delay, controller, debug):
        ret = None
        if self.scheduler.sched_policy == "EDF":
            ret = self.admit_task_EDF(service, time, flow_id, deadline, receiver, rtt_delay, controller, debug)
        elif self.scheduler.sched_policy == "FIFO":
            ret = self.admit_task_FIFO(service, time, flow_id, deadline, receiver, rtt_delay, controller, debug)
        else:
            print ("Error: This should not happen in admit_task(): " +repr(self.scheduler.sched_policy))
            
        return ret
    
    def admit_task_with_probability(self, time, aTask, perAccessPerNodePerServiceProbability, debug):
        availableInstances = self.numberOfVMInstances[aTask.service]
        if debug:
            print("Available instances for service: " + str(aTask.service) + " at node: " + str(self.node) + " is: " + str(availableInstances))
        if availableInstances == 0:
            return False
        
        #numQueuedAndRunning = self.scheduler.runningServices.count(aTask.service) + self.scheduler.queuedServices[aTask.service]
        numRunning = 0
        for bTask in self.scheduler.runningTasks:
            if bTask is None:
                continue
            if bTask.service == aTask.service and bTask.receiver == aTask.receiver:
                numRunning += 1
        numQueued = self.scheduler.queuedServicesPerReceiver[int(aTask.receiver[4:])][aTask.service]
        numQueuedAndRunning = numQueued + numRunning
        if debug:
            print("Number of running instances for service: " + str(aTask.service) + " is: " + str(numQueuedAndRunning))
        if numQueuedAndRunning >= availableInstances:
            return False
        elif (availableInstances - numQueuedAndRunning) == 1:
            r = random.random()
            ap = int(aTask.receiver[4:])
            node = int(aTask.node)
            if r < perAccessPerNodePerServiceProbability[ap][node][aTask.service]:
                if debug:
                    print("Admitted with probability: " + str(perAccessPerNodePerServiceProbability[ap][node][aTask.service]))
                ### Check if the task will satisfy the deadline
                self.scheduler.addToTaskQueue(aTask, time)
                self.compute_completion_times(time, True, debug)
                for task in self.scheduler._taskQueue[:]:
                    if debug:
                        print("After simulate:")
                        task.print_task()
                    if task.taskType == Task.TASK_TYPE_VM_START:
                        continue
                    if task.completionTime is None: 
                        print("Error: a task with invalid completion time: " + repr(task.completionTime))
                        raise ValueError("Task completion time is invalid")
                    if (task.expiry - task.rtt_delay) < task.completionTime:
                        if debug:
                            print("Refusing TASK: Congestion")
                        self.scheduler.removeFromTaskQueue(aTask)
                        return False
                return True
        elif (availableInstances - numQueuedAndRunning) > 1:
            ### Check if the task will satisfy the deadline
            self.scheduler.addToTaskQueue(aTask, time)
            self.compute_completion_times(time, True, debug)
            for task in self.scheduler._taskQueue[:]:
                if debug:
                    print("After simulate:")
                    task.print_task()
                if task.taskType == Task.TASK_TYPE_VM_START:
                    continue
                if task.completionTime is None: 
                    print("Error: a task with invalid completion time: " + repr(task.completionTime))
                    raise ValueError("Task completion time is invalid")
                if (task.expiry - task.rtt_delay) < task.completionTime:
                    if debug:
                        print("Refusing TASK: Congestion")
                    self.scheduler.removeFromTaskQueue(aTask)
                    return False
            if debug:
                print("Admitted")
            return True
        if debug:
            print("Rejected")
        return False

    def reassign_vm(self, controller, time, serviceToReplace, newService, debug):
        """
        Instantiate service at the given vm
        """
        if self.numberOfVMInstances[serviceToReplace] == 0:
            raise ValueError("Error in reassign_vm: the service to replace has no instances")

        if debug:
            print ("Replacing service: " + repr(serviceToReplace) + " with: " + repr(newService) + " at node: " + repr(self.node))
        if self.numberOfVMInstances[serviceToReplace] <= 0:
            print("Error in reassign_vm(): service to replace has no instances for service:" + str(serviceToReplace))
            raise ValueError("Invalid number of instances of service: " + str(newService) + " has " + str(self.numberOfVMInstances[serviceToReplace]))
        self.numberOfVMInstances[newService] += 1
        self.numberOfVMInstances[serviceToReplace] -= 1
        if len(self.scheduler.busyVMs[serviceToReplace]) + len(self.scheduler.idleVMs[serviceToReplace])  <= 0:
            print ("Error: No VM instance for the service that is to be replaced")
            print ("Length of the startingVMs is " + str(len(self.scheduler.startingVMs[serviceToReplace])))
            print ("Node: " + str(self.node))
            print ("Service to replace: " + str(serviceToReplace))
            print ("new service: " + str(newService))
            raise ValueError("Error in reassign_vm(): unable to find a VM instance to replace")
        
        #lastTask = self.get_latest_task(serviceToReplace)
        lastTask = self.get_latest_service_task()
        if lastTask is None:
            aTask = Task(time, Task.TASK_TYPE_VM_START, time + VM.instantiationDuration, 0, self.node, serviceToReplace, VM.instantiationDuration, None, None)
            aTask.setServiceToInstantiate(newService)
            self.scheduler.addToTaskQueue(aTask, time)
            newTask = self.scheduler.schedule(time)
            if newTask is not None:
                controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, self.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask) 
        else:
            aTask = Task(time, Task.TASK_TYPE_VM_START, float('inf'), 0, self.node, serviceToReplace, VM.instantiationDuration, None, None, float('inf'))
            self.scheduler.addToUpcomingTaskQueue(aTask, time)
            aTask.setServiceToInstantiate(newService)
            lastTask.setNextTasktoRun(aTask)
        
    def getIdleTime(self, time):
        """
        Get the total idle time of the node
        """
        return self.scheduler.get_idleTime(time)

