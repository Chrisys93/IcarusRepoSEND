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
import math # for ceil()

from icarus.registry import register_strategy
from icarus.util import inheritdoc, path_links
from .base import Strategy
from icarus.models.service import Task

__all__ = [
        'OptimalPlacementAndScheduling'
           ]

# Status codes
REQUEST = 0
RESPONSE = 1
TASK_COMPLETE = 2
@register_strategy('OPTIMAL_PLACEMENT_SCHED')
class OptimalPlacementAndScheduling(Strategy):
    """
    An optimal approach for scheduling 
    i) Use global congestion information on Computation Spots to route requests.
    ii) Use global demand distribution to place services on Computation Spots.
    """
    def __init__(self, view, controller, replacement_interval=10, debug=False, p = 0.5, **kwargs):
        super(OptimalPlacementAndScheduling, self).__init__(view,controller)
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
        self.H      = len(self.compSpots) - 1 #number of nodes
        self.C      = cp.Parameter(self.H, value=np.zeros(self.H)) 
        self.x_bar  = {} # #variable: maximum number of group-node requests forwarded per services with size (Sx)GxH
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
            #print ("Node: " + str(node) + " has " + str(self.C.value[node]) + " cores.")
        #print ("Lenght of compSpots: " + str(len(self.compSpots)))

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
                        #if node == 0:
                        #    print ("\t\tService executed for 0")
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
            
        self.compute_optimal_placement_schedule()

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
        for ap in self.receivers:
            ap = int(ap[4:])
            for service in range(0, self.view.num_services()):
                for node in self.compSpots.keys():
                    cs = self.compSpots[node]
                    if cs.is_cloud:
                        continue
                    self.x_bar[service].value[ap, node] = self.x_bar[service].value[ap, node]*self.A[service][node][ap]
                    if self.x_bar[service].value[ap, node]<0.1e-9:
                        self.x_bar[service].value[ap, node] = 0.0

    def finalizeResults(self):
        for group in xrange(self.G):
            print ('Group '+str(group))
            for service in xrange(self.S):
                print ('\tService: '+str(service))
                for node in xrange(self.H):
                    print ('\t\t\tnode: '+str(node)+', rate: '+str(self.x[group,service].value[node]))
    
    def compute_optimal_placement_schedule(self):
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
        #self.groupSpecificExecution()
        self.noiseFilter()#the optimisers essentially they are using interior point methods so they migh assign negligible values to variables
        
        # Do the placement now
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for service in range(0, self.view.num_services()):
                cs.numberOfVMInstances[service] = 0

        for service in range(0, self.view.num_services()):
            for ap in self.receivers:
                ap = int(ap[4:])
                for node in self.compSpots.keys():
                    cs = self.compSpots[node]
                    if cs.is_cloud:
                        continue
                    node = int(node)
                    #print (str((cp.sum_entries(np.sum(map(lambda ap:cp.mul_elemwise(self.A[ap],self.x_bar[ap].T),range(self.S))), axis=1)).value))
                    #if node == 3:
                    #    print("Service: " + str(service) + " ap: " + str(ap) + " node: " + str(node) + " x_bar = " + str(self.x_bar[service].value[ap,node]))
                    cs.serviceProbabilities[service] += self.x_bar[service].value[ap, node]
                    #cs.numberOfVMInstances[service] += int(math.ceil(self.x_bar[service].value[ap, node]))
                    self.perAccessPerNodePerServiceProbability[ap][node][service] = self.x_bar[service].value[ap, node] - math.floor(self.x_bar[service].value[ap, node])

        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for service in range(0, self.view.num_services()):
                node = int(node)
                cs.numberOfVMInstances[service] = int(math.ceil(cs.serviceProbabilities[service]))

        if True:
            for node in self.compSpots.keys():
                cs = self.compSpots[node]
                if cs.is_cloud:
                    continue
                if cs.node != 14 and cs.node!=6:
                    continue
                for service in range(0, self.view.num_services()):
                    if cs.numberOfVMInstances[service] > 0:
                        print ("Node: " + str(node) + " has " + str(cs.numberOfVMInstances[service]) + " instance of " + str(service))

    def initialise_metrics(self):
        """Initialise counts and metrics between periods of optimized solution computations
        """
        for service in range(0, self.view.num_services()):
            for ap in self.receivers:
                ap = int(ap[4:])
                self.perServiceReceiverRequestCounts[service][ap] = 0

        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            if cs.is_cloud:
                continue
            for service in range(0, self.view.num_services()):
                cs.serviceProbabilities[service] = 0.0
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
        source = self.view.content_source(service)
        if self.debug:
            print ("\nEvent\n time: " + repr(time) + " receiver  " + repr(receiver) + " service " + repr(service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(deadline) + " status " + repr(status)) 
        
        if time - self.last_replacement > self.replacement_interval:
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, time)
            self.compute_optimal_placement_schedule()
            self.last_replacement = time
            self.initialise_metrics()

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        
        # Request reached the cloud
        if source == node and status == REQUEST:
            ret, reason = compSpot.admit_task(service, time, flow_id, deadline, receiver, rtt_delay, self.controller, self.debug)
            if ret == False:
                print("This should not happen in Hybrid.")
                raise ValueError("Task should not be rejected at the cloud.")
            return 
        # Request at the receiver 
        if receiver == node and status == REQUEST:
            self.controller.start_session(time, receiver, service, log, flow_id, deadline)
            self.perServiceReceiverRequestCounts[service][int(receiver[4:])] += 1
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay*2
            self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)

            return
        # Request at an edge cloud
        elif status == REQUEST and node != source:
            aTask = Task(time, deadline, rtt_delay, node, service, compSpot.services[service].service_time, flow_id, receiver)
            ret = compSpot.admit_task_with_probability(time, aTask, self.perAccessPerNodePerServiceProbability, self.debug)
            newTask = compSpot.scheduler.schedule(time)
            if newTask is not None:
                self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, compSpot.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE) 
                self.controller.execute_service(newTask.flow_id, newTask.service, compSpot.node, time, compSpot.is_cloud)
                if self.debug:
                    print (str(time) + ". flow_id: " + str(flow_id) + " is being executed and to complete at: " + str(newTask.completionTime))
                    newTask.print_task()

        elif status == TASK_COMPLETE:
            if node != source:
                newTask = compSpot.scheduler.schedule(time)
                if newTask is not None:
                    self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.service, compSpot.node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE) 
                    self.controller.execute_service(newTask.flow_id, newTask.service, compSpot.node, time, compSpot.is_cloud)
            
            delay = self.view.path_delay(node, receiver)
            ### Send the response directly to the receiver
            self.controller.add_event(time+delay, receiver, service, receiver, flow_id, deadline, rtt_delay, RESPONSE)
        elif status == RESPONSE:
            if node != receiver:
                raise ValueError("This should not happen in placement.py")

            self.controller.end_session(True, time, flow_id) 
            if self.debug:
                print (str(time) + ". response for flow_id: " + str(flow_id) + " reached the receiver at time: " + str(time) + " deadline: " + str(deadline))
            return
        else:
            print("This should not happen in Coordinated")
            sys.exit("Unexpected request in Coordinated strategy")
