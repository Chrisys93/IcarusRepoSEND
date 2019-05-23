# -*- coding: utf-8 -*-
"""Implementations of all service-based strategies"""
from __future__ import division
from __future__ import print_function

import networkx as nx
import random
import sys
# for the optimizer
import cvxpy as cp
import numpy as np
import optparse

from icarus.registry import register_strategy
from icarus.util import inheritdoc, path_links
from .base import Strategy
from icarus.models.service import Task

__all__ = [
        'OptimalScheduling'
           ]

# Status codes
REQUEST = 0
RESPONSE = 1
TASK_COMPLETE = 2
@register_strategy('OPTIMAL_SCHED')
class OptimalScheduling(Strategy):
    """
    An optimal approach for scheduling 
    i) Use global congestion information on Computation Spots to route requests.
    ii) Use global demand distribution to place services on Computation Spots.
    """
    def __init__(self, view, controller, replacement_interval=10, debug=False, p = 0.5, **kwargs):
        super(OptimalScheduling, self).__init__(view,controller)
        self.last_replacement = 0
        self.replacement_interval = replacement_interval
        ### Count of requests for each service
        self.perServiceReceiverRequestCounts = {} 
        self.perAccessPerNodePerServiceProbability = {}
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.requestNumber = 0
        self.debug = debug
        ### inputs to optimizer
        self.riskAversionParameter = 0.0 #TODO: Ask Argyris
        self.S      = self.view.num_services()
        self.G      = len(self.receivers) #number of groups 
        self.H      = len(self.compSpots) #number of nodes
        self.C      = cp.Parameter(self.H, value=np.zeros(self.H)) 
        self.x_bar  = {} # #variable: maximum number of group-node requests forwarded per services
        self.A      = {} #nodes accessible for each group per service, i.e., nodes that can serve a request from a group with size (Sx)HxG 
        self.x      = {} #number of requests for each group per service and node with size (GxS)xH
        self.std_D  = {} #standing deviation of arriving requests rate per service
        self.avg_D  = {} #average arriving requests rate per service with size (Sx)G 

        ### initialise optimizer input 
        for i in range(self.S):
            self.x_bar[i]  = cp.Variable(self.G,self.H)
            self.avg_D[i]  = cp.Parameter(self.G,value=np.zeros(self.G))
            self.std_D[i]  = cp.Parameter(self.G,value=np.zeros(self.G))
            self.A[i]      = cp.Parameter((self.H,self.G))
            self.A[i]     = np.zeros((self.H,self.G))
            for g in range(self.G):
                self.x[g,i]  = cp.Variable(self.H)

        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            node = int(node)
            self.C.value[node]  = cs.numOfCores

        for ap in self.receivers:
            ap_int = int(ap[4:])
            parent = view.topology().graph['edge_routers'][ap_int]
            while parent is not None:
                cs = self.compSpots[parent]
                if cs.is_cloud:
                    continue
                node = int(parent)
                rtt_delay = 2*view.path_delay(ap, parent)
                if self.debug:
                    print ("RTT delay from " + str(ap) + " to " + str(parent) + " is: " + str(rtt_delay))
                service_indx = 0
                for s in view.services():
                    if s.deadline > rtt_delay + s.service_time:
                        self.A[service_indx][node][ap_int] = 1.0
                    else: # XXX I added this
                        self.A[service_indx][node][ap_int] = 0.0
                    service_indx += 1
                parent = view.topology().graph['parent'][parent]
                    
        # initially assign equal rates for each service at each AP
        rate = view.getRequestRate()
        for service in range(0, view.num_services()):
            for ap in self.receivers:
                ap = int(ap[4:])
                self.avg_D[service].value[ap] = rate/(len(self.receivers)*view.num_services())
                self.std_D[service].value[ap] = self.avg_D[service].value[ap]

        for service in range(0, view.num_services()):
            self.perServiceReceiverRequestCounts[service] = {}
            for ap in self.receivers:
                ap = int(ap[4:])
                self.perServiceReceiverRequestCounts[service][ap] = self.replacement_interval * (rate/(len(self.receivers)*view.num_services()))

        for ap in self.receivers:
            ap = int(ap[4:])
            self.perAccessPerNodePerServiceProbability[ap] = {}
            for node in self.compSpots.keys():
                cs = self.compSpots[node]
                if cs.is_cloud:
                    continue
                node = int(node)
                self.perAccessPerNodePerServiceProbability[ap][node] = {}
                for service in range(0, view.num_services()):
                    self.perAccessPerNodePerServiceProbability[ap][node][service] = 0.0

        #self.parser = optparse.OptionParser()
        #self.parser.add_option('--riskAversionParameter',action="store",type="float",default=0.0)
        #self.options, args = self.parser.parse_args()
        ### End of initializing optimizer input

        ### Assign large enough number of VMs at each node to assure that 
        #   the only constraint is the numOfCores
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            cs.numOfVMs = cs.service_population_size*cs.numOfCores
            cs.scheduler.numOfVMs = cs.numOfVMs
            ### Place numOfCores instances for each service type 
            ### at each cs to avoid insufficient VMs. 
            ### The only constraint is the numOfCores. 
            for serv in range(cs.service_population_size):
                cs.numberOfVMInstances[serv] = 0
            for vm in range(cs.numOfVMs):
                serv = vm % cs.service_population_size 
                cs.numberOfVMInstances[serv] += 1 
            
        self.compute_optimal_schedule()

    def max_request_forwarded(self):
        objective  = cp.Maximize(cp.sum_entries(cp.sum_entries(np.sum(map(lambda i:cp.mul_elemwise(self.A[i].T,self.x_bar[i]),range(self.S))), axis=0),axis=1))

        capacityConstrain  = cp.sum_entries(np.sum(map(lambda i:cp.mul_elemwise(self.A[i],self.x_bar[i].T),range(self.S))), axis=1)<=self.C #row_sums, if axis=0 it would be a column sum
        constraints  = [capacityConstrain]
        for i in range(self.S):
            r1          = cp.sum_entries(self.x_bar[i],axis=1)<= self.avg_D[i]
            constraints.append(r1)
            r2          = self.x_bar[i]>=0.0
            constraints.append(r2)
        lp1 = cp.Problem(objective,constraints)
        result = lp1.solve()

    def groupSpecificExecution(self):
        for g in xrange(self.G):
            for s in xrange(self.S):
                objective = cp.Maximize(cp.sum_entries(self.x[g,s]))
                r1 = cp.sum_entries(self.x[g,s]) <= self.avg_D[s][g]-self.riskAversionParameter*self.std_D[s][g]
                r2 = self.x[g,s] <= self.x_bar[s].value[g].T
                r3 = self.x[g,s] >= 0.0
                constraints = [r1,r2,r3]
                lp1 = cp.Problem(objective,constraints)
                result = lp1.solve()
        
    def noiseFilter(self):
        for group in xrange(self.G):
            for service in xrange(self.S):
                for node in xrange(self.H):
                    if self.x[group,service].value[node]<0.1e-9:
                        self.x[group,service].value[node]=0.0
    def finalizeResults(self):
        for group in xrange(self.G):
            print ('Group '+str(group))
            for service in xrange(self.S):
                print ('\tService: '+str(service))
                for node in xrange(self.H):
                    print ('\t\t\tnode: '+str(node)+', rate: '+str(self.x[group,service].value[node]))
    
    def compute_optimal_schedule(self):
        """
        Compute an optimal scheduling based on the service demand
        """
        if self.debug:
            print ("Computing optimal schedule: ")
        # initialise the probability array here (not in initialise because it is to be used during the next replacement interval)
        for ap in self.receivers:
            ap = int(ap[4:])
            for node in self.compSpots.keys():
                cs = self.compSpots[node]
                if cs.is_cloud:
                    continue
                node = int(node)
                for service in range(0, self.view.num_services()):
                    self.perAccessPerNodePerServiceProbability[ap][node][service] = 0.0
        for service in range(0, self.view.num_services()):
            for ap in self.receivers:
                ap = int(ap[4:])
                self.avg_D[service].value[ap] = (1.0*self.perServiceReceiverRequestCounts[service][ap])/self.replacement_interval
                self.std_D[service].value[ap] = self.avg_D[service].value[ap]
        self.max_request_forwarded()
        self.groupSpecificExecution()
        self.noiseFilter()#the optimisers essentially they are using interior point methods so they migh assign negligible values to variables
        for ap in self.receivers:
            ap = int(ap[4:])
            for node in self.compSpots.keys():
                cs = self.compSpots[node]
                if cs.is_cloud:
                    continue
                node = int(node)
                for service in range(0, self.view.num_services()):
                    if self.debug:
                        print("Count for ap: " + str(ap) + " service: " + str(service) + " is: " + str( self.perServiceReceiverRequestCounts[service][ap])) 
                    if self.perServiceReceiverRequestCounts[service][ap] > 0:
                        rate = self.perServiceReceiverRequestCounts[service][ap]/self.replacement_interval 
                        if self.debug:
                            print ("Type: " + str(type(self.x[ap, service].value[node])))
                            print ("x[ap, service].value[node] = " + str(self.x[ap, service].value[node].item(0,0)))
                        self.perAccessPerNodePerServiceProbability[ap][node][service] = (self.x[ap, service].value[node].item(0,0))/ rate
                        if self.perAccessPerNodePerServiceProbability[ap][node][service] > 1.01:
                            raise ValueError("Invalid probability: " + str(self.perAccessPerNodePerServiceProbability[ap][node][service]))
                            

        if self.debug:
            for service in range(0, self.view.num_services()):
                for ap in self.receivers:
                    ap = int(ap[4:])
                    if self.perServiceReceiverRequestCounts[service][ap] > 0:
                        for node in self.compSpots.keys():
                            cs = self.compSpots[node]
                            if cs.is_cloud:
                                continue
                            node = int(node)
                            print ("Probability for access: " + str(ap) + " at node: " + str(node) + " service: " + str(service) + " is " + str(self.perAccessPerNodePerServiceProbability[ap][node][service]))

    def initialise_metrics(self):
        """Initialise counts and metrics between periods of optimized solution computations
        """
        for service in range(0, self.view.num_services()):
            for ap in self.receivers:
                ap = int(ap[4:])
                self.perServiceReceiverRequestCounts[service][ap] = 0

    def pickExecutionNode(self, receiver, service, cloud):
        """
        Pick the execution node for a given request according to the optimizer output.
        """
        r = random.random()
        cum = 0.0
        parent = self.view.topology().graph['edge_routers'][receiver]
        receiver = int(receiver[4:])
        while parent is not None:
            cs = self.compSpots[parent]
            if cs.is_cloud:
                continue
            node = int(parent)
            cum += self.perAccessPerNodePerServiceProbability[receiver][node][service]
            if r <= cum:
                return parent
            if cum >= 1.0:
                raise ValueError("Cumulative probability exceeded 1.0 in Optimal Strategy.")
            parent = self.view.topology().graph['parent'][parent]
        
        return cloud

    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, rtt_delay, status):
        """
        status : Request, response arrival event or task_complete event
        deadline : deadline for the request 
        flow_id : Id of the flow that the request/response is part of
        node : the current node at which the request/response arrived
        """

        service = content
        if self.debug:
            print ("\nEvent\n time: " + repr(time) + " receiver  " + repr(receiver) + " service " + repr(service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(deadline) + " status " + repr(status)) 
        source = self.view.content_source(service)
        
        if time - self.last_replacement > self.replacement_interval:
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, time)
            self.compute_optimal_schedule()
            self.last_replacement = time
            self.initialise_metrics()

        if receiver == node and status == REQUEST:
            self.controller.start_session(time, receiver, service, log, flow_id, deadline)
            self.perServiceReceiverRequestCounts[service][int(receiver[4:])] += 1
            path = self.view.shortest_path(node, source)
            r = random.random()
            upstream_node = self.pickExecutionNode(receiver, service, source)
            delay = self.view.path_delay(node, upstream_node)
            rtt_delay += delay*2
            if upstream_node != source:
                cs = self.view.compSpot(upstream_node)
                aTask = Task(time, deadline, rtt_delay, upstream_node, service, cs.services[service].service_time, flow_id, receiver, time+delay)
                cs.scheduler.upcomingTaskQueue.append(aTask)
                cs.scheduler.upcomingTaskQueue = sorted(cs.scheduler.upcomingTaskQueue, key=lambda x: x.arrivalTime)
                self.controller.add_event(time+delay, receiver, service, upstream_node, flow_id, deadline, rtt_delay, REQUEST)
                if self.debug:
                    print ("Request is scheduled to run at: " + str(upstream_node))
            else: #request is to be executed in the cloud and returned to receiver
                services = self.view.services() 
                serviceTime = services[service].service_time
                self.controller.add_event(time+rtt_delay+serviceTime, receiver, service, receiver, flow_id, deadline, rtt_delay, RESPONSE)
                if self.debug:
                    print ("Request is scheduled to run at the CLOUD")
            return
        elif status == REQUEST and node != source:
            compSpot = self.view.compSpot(node)
            #check if the request is in the upcomingTaskQueue
            for thisTask in compSpot.scheduler.upcomingTaskQueue:
                if thisTask.flow_id == flow_id:
                    break
            else: # task for the flow_id is not in the Queue
                raise ValueError("No task with the given flow id in the upcomingTaskQueue")
            newTask = compSpot.scheduler.schedule(time)
            if newTask is not None:
                self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, compSpot.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE) 
                self.controller.execute_service(newTask.flow_id, newTask.service, compSpot.node, time, compSpot.is_cloud)
                if self.debug and False:
                    print ("Task is scheduled to run: ")
                    newTask.print_task()
            else:
                if self.debug and False:
                    print ("Task is not ready to be executed: ")
                    compSpot.scheduler.print_core_status()
                    print (str(len(compSpot.scheduler.taskQueue)) + " tasks in the taskQueue")
                    for aTask in compSpot.scheduler._taskQueue:
                        aTask.print_task()
                    print (str(len(compSpot.scheduler.upcomingTaskQueue)) + " tasks in the upcomingtaskQueue")
                    for aTask in compSpot.scheduler.upcomingTaskQueue:
                        aTask.print_task()

        elif status == TASK_COMPLETE:
            compSpot = self.view.compSpot(node)
            newTask = compSpot.scheduler.schedule(time)
            if newTask is not None:
                self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, compSpot.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE) 
                self.controller.execute_service(newTask.flow_id, newTask.service, compSpot.node, time, compSpot.is_cloud)
            
            delay = self.view.path_delay(node, receiver)
            self.controller.add_event(time+delay, receiver, service, receiver, flow_id, deadline, rtt_delay, RESPONSE)
            if self.debug and False:
                print("Flow id: " + str(flow_id) + " is scheduled to arrive at the receiver at time: " + str(time+delay))
                if (time+delay > deadline):
                    print ("Response at receiver at time: " + str(time+delay) + " deadline: " + str(deadline))
        elif status == RESPONSE and node == receiver:
            self.controller.end_session(True, time, flow_id) 
            if self.debug and False:
                print ("Response reached the receiver at time: " + str(time) + " deadline: " + str(deadline))
            return
        else:
            print("This should not happen in Coordinated")
            sys.exit("Unexpected request in Coordinated strategy")
