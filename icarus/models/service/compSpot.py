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
        self.queueArrivalTime = None
        if arrivalTime is None:
            self.arrivalTime = time
        else:
            self.arrivalTime = arrivalTime

    def print_task(self):
        print ("---Task.service: " + repr(self.service))
        print ("\tTask.time: " + repr(self.time))
        print ("\tTask.deadline: " + repr(self.expiry))
        print ("\tTask.rtt_delay: " + repr(self.rtt_delay))
        print ("\tTask.exec_time: " + repr(self.exec_time))
        print ("\tTask.node: " + repr(self.node))
        print ("\tTask.flow_id: " + repr(self.flow_id))
        print ("\tTask.receiver: " + repr(self.receiver))
        print ("\tTask.completionTime: " + repr(self.completionTime))
        if self.arrivalTime is not None:
            print ("\tTask.arrivalTime: " + repr(self.arrivalTime))
        print ("\n")

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

    def get_idleTime(self, time):
        
        # update the idle times
        for indx in range(0, self.numOfCores):
            if self.coreFinishTime[indx] < time:
                self.idleTime += time - self.coreFinishTime[indx]
                self.coreFinishTime[indx] = time
        
        return self.idleTime
    
    def removeFromTaskQueue(self, aTask):
        self._taskQueue.remove(aTask)
        aTask.queueArrivalTime = None
        self.queuedServices[aTask.service] -= 1
        self.queuedServicesPerReceiver[int(aTask.receiver[4:])][aTask.service] -= 1

    def addToTaskQueue(self, aTask, time):
        self._taskQueue.append(aTask)
        aTask.queueArrivalTime = time
        if self.sched_policy == 'EDF':
            self._taskQueue = sorted(self._taskQueue, key=lambda x: x.expiry)
        elif self.sched_policy == 'FIFO':
            self._taskQueue = sorted(self._taskQueue, key=lambda x: x.arrivalTime)
        else:
            raise ValueError("Invalid scheduling policy")

        self.queuedServices[aTask.service] += 1
        self.queuedServicesPerReceiver[int(aTask.receiver[4:])][aTask.service] += 1
        
    def get_available_core(self, time):
        
        self.update_state(time)   
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

        #if minFinishTime == time:
        #    raise ValueError("This should not happen in get_next_finish_time()");

        return minFinishTime                

    def assign_task_to_core(self, core_indx, aTask, time):
        
        if self.coreFinishTime[core_indx] > time:
            raise ValueError("Error in assign_task_to_core: there is a running task")
        
        self.runningServices[core_indx] = aTask.service
        self.runningTasks[core_indx] = aTask
        self.coreFinishTime[core_indx] = aTask.completionTime
        taskWaitingTime = time - aTask.arrivalTime 
        if taskWaitingTime < 0:
            raise ValueError("This should not happen: Queue waiting time is below 0")
        self.waitingTime += taskWaitingTime
        self.taskCount += 1

    def update_state(self, time):
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
                self.addToTaskQueue(task, time)
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
    
    def schedule(self, time):
        """
        Return the next task to be executed, if there is any.
        """

        core_indx = self.get_available_core(time)
        
        # The following is to accommodate coordinated routing
        if len(self.upcomingTaskQueue) > 0:
            new_addition = False
            self.upcomingTaskQueue = sorted(self.upcomingTaskQueue, key=lambda x: x.arrivalTime)
            for task in self.upcomingTaskQueue[:]: 
                if time < task.arrivalTime:
                    continue
                aTask = self.upcomingTaskQueue.remove(task)
                self.app
                self.addToTaskQueue(task, time)
                new_addition = True
        
        if (len(self._taskQueue) > 0) and (core_indx is not None):
            runningServices = self.runningServices
            runningTasks = self.runningTasks
            for aTask in self._taskQueue[:]:
                serv_count = runningServices.count(aTask.service)
                if self.cs.numberOfServiceInstances[aTask.service] > 0: 
                    available_vms = self.cs.numberOfServiceInstances[aTask.service] - serv_count
                    if available_vms > 0:
                        #self.taskQueue.remove(aTask)
                        self.removeFromTaskQueue(aTask)
                        aTask.completionTime = time + aTask.exec_time
                        self.assign_task_to_core(core_indx, aTask, time)
                        return aTask
                    else:
                        self.cs.insufficientVMEvents[aTask.service] += 1
                else: # This can happen during service replacement transitions
                    #self.taskQueue.remove(aTask)
                    self.removeFromTaskQueue(aTask)
                    aTask.completionTime = time + aTask.exec_time
                    self.assign_task_to_core(core_indx, aTask, time)
                    return aTask
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
        # num. of vms (memory capacity) #TODO rename this
        self.numOfVMs = numOfVMs
        ### num. of instances of each service in the memory #
        self.numberOfServiceInstances = [0]*self.service_population_size
        ### the below variable is for the optimal placement strategy
        self.serviceProbabilities = [0.0]*self.service_population_size

        # CPU info
        self.scheduler = Scheduler(sched_policy, self)
        # A copy of scheduler for making admission decisions
        self.schedulerCopy = None 
        # server missed requests (due to congestion)
        self.missed_requests = [0] * self.service_population_size
        # service requests that were missed due to insufficient VM
        self.insufficientVMEvents = [0] * self.service_population_size
        # service request count (per service)
        self.running_requests = [0 for x in range(0, self.service_population_size)]
        # delegated (i.e., upstream) service request counts (per service)
        self.delegated_requests = [0 for x in range(0, self.service_population_size)]
        
        self.view = None
        self.node = node
        self.rtt_upstream = [0.0 for x in range(0, self.service_population_size)]

        num_of_VMs = 0
        if dist is None and self.is_cloud == False:
            # setup a random set of services to run in the memory
            while num_of_VMs < self.numOfVMs:
                service_index = random.choice(range(0, self.service_population_size))
                #if self.numberOfServiceInstances[service_index] == 0:
                self.numberOfServiceInstances[service_index] = 1
                num_of_VMs += 1
                evicted = self.model.cache[node].put(service_index) # HACK: should use controller here   


    def compute_completion_times(self, time, debug=False):
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
        #self.schedulerCopy = copy.deepcopy(self.scheduler)
        self.schedulerCopy = copy.copy(self.scheduler)
        # The below copy operations are necessary as copy.copy only makes 
        # a shallow copy, which simply copies references of the following two 
        # lists in scheduler to schedulerCopy. 
        self.schedulerCopy._taskQueue = copy.copy(self.scheduler._taskQueue)
        self.schedulerCopy.upcomingTaskQueue = copy.copy(self.scheduler.upcomingTaskQueue)
        self.schedulerCopy.coreFinishTime = copy.copy(self.scheduler.coreFinishTime)
        self.schedulerCopy.runningServices = copy.copy(self.scheduler.runningServices)
        self.schedulerCopy.runningTasks = copy.copy(self.scheduler.runningTasks)
        numRunning = self.schedulerCopy.update_state(time)


        if debug:
            print("\tPrinting task queue for node: " + str(self.node))
            for task_indx in range(0, len(self.schedulerCopy._taskQueue)):
                self.schedulerCopy._taskQueue[task_indx].print_task()
            self.schedulerCopy.print_core_status()

        while len(self.schedulerCopy._taskQueue) > 0 or len(self.schedulerCopy.upcomingTaskQueue) > 0:
            taskToExecute = self.schedulerCopy.schedule(time)
            if debug:
                if taskToExecute is not None:
                    print ("\tTask to execute for flow: " + str(taskToExecute.flow_id))
                else:
                    print ("\tNo task to execute")
                    print ("Current time: " + str(time))
                    if len(self.schedulerCopy.upcomingTaskQueue) > 0: 
                        print "Upcoming task queue not empty: " + str(len(self.schedulerCopy.upcomingTaskQueue))
                        for task in self.schedulerCopy.upcomingTaskQueue:
                            task.print_task()
                    if len(self.schedulerCopy._taskQueue) > 0: 
                        print "task queue not empty: " + str(len(self.schedulerCopy._taskQueue))
                        for task in self.schedulerCopy._taskQueue:
                            task.print_task()

            next_time = self.schedulerCopy.get_next_task_admit_time(time)
            if taskToExecute == None and next_time <= time:
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
            numRunning = self.schedulerCopy.update_state(time)
            if debug:
                print ("NumRunning: " + str(numRunning))
            if numRunning == 0 and len(self.schedulerCopy._taskQueue) == 0 and len(self.schedulerCopy.upcomingTaskQueue) > 0:
                if time < self.schedulerCopy.upcomingTaskQueue[0].arrivalTime:  
                    time = self.schedulerCopy.upcomingTaskQueue[0].arrivalTime
                    numRunning = self.schedulerCopy.update_state(time)
        if debug:
            print("\tDONE Computing completion times for node: " + str(self.node))

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
        
        if self.numberOfServiceInstances[service] == 0:
            return [False, NO_INSTANCES]
        
        if deadline - time - rtt_delay - serviceTime < 0:
            if debug:
                print ("Refusing TASK: deadline already missed")
            return [False, DEADLINE_MISSED]
        
        aTask = Task(time, deadline, rtt_delay, self.node, service, serviceTime, flow_id, receiver)
        self.scheduler.addToTaskQueue(aTask, time)
        #self.scheduler.taskQueue.append(aTask)
        self.compute_completion_times(time, debug)
        for task in self.scheduler._taskQueue[:]:
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
                #self.scheduler.taskQueue.remove(aTask)
                self.scheduler.removeFromTaskQueue(aTask)
                return [False, CONGESTION]

        # New task can be admitted, add to service Queue
        self.running_requests[service] += 1
        # Run the next task (if there is any)
        newTask = self.schedule(time) 
        if newTask is not None:
            controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, self.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE) 
            controller.execute_service(newTask.flow_id, newTask.service, self.node, time, self.is_cloud)
        
        if self.numberOfServiceInstances[service] == 0:
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
        
        if self.numberOfServiceInstances[service] == 0:
            return [False, NO_INSTANCES]
        
        if deadline - time - rtt_delay - serviceTime < 0:
            if debug:
                print ("Refusing TASK: deadline already missed")
            return [False, DEADLINE_MISSED]
            
        aTask = Task(time, deadline, rtt_delay, self.node, service, serviceTime, flow_id, receiver)
        #self.scheduler.taskQueue.append(aTask)
        self.scheduler.addToTaskQueue(aTask, time)
        self.compute_completion_times(time, debug)
        for task in self.scheduler._taskQueue[:]:
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
                #self.scheduler.taskQueue.remove(aTask)
                self.scheduler.removeFromTaskQueue(aTask)
                return [False, CONGESTION]
        
        # New task can be admitted, add to service Queue
        self.running_requests[service] += 1
        # Run the next task (if there is any)
        newTask = self.scheduler.schedule(time) 
        if newTask is not None:
            controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, self.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE) 
            controller.execute_service(newTask.flow_id, newTask.service, self.node, time, self.is_cloud)

        if self.numberOfServiceInstances[service] == 0:
            print "Error: this should not happen in admit_task_EDF()"
       
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
        availableInstances = self.numberOfServiceInstances[aTask.service]
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
                self.compute_completion_times(time, debug)
                for task in self.scheduler._taskQueue[:]:
                    if debug:
                        print("After simulate:")
                        task.print_task()
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
            self.compute_completion_times(time, debug)
            for task in self.scheduler._taskQueue[:]:
                if debug:
                    print("After simulate:")
                    task.print_task()
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

    def reassign_vm(self, controller, serviceToReplace, newService, debug):
        """
        Instantiate service at the given vm
        """
        if self.numberOfServiceInstances[serviceToReplace] == 0:
            raise ValueError("Error in reassign_vm: the service to replace has no instances")
            
        if debug:
            print "Replacing service: " + repr(serviceToReplace) + " with: " + repr(newService) + " at node: " + repr(self.node)
        self.numberOfServiceInstances[newService] += 1
        self.numberOfServiceInstances[serviceToReplace] -= 1
        controller.reassign_vm(self.node, serviceToReplace, newService)

    def getIdleTime(self, time):
        """
        Get the total idle time of the node
        """
        return self.scheduler.get_idleTime(time)

