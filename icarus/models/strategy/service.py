# -*- coding: utf-8 -*-
"""Implementations of all service-based strategies"""
from __future__ import division
from __future__ import print_function

import networkx as nx
import random

from icarus.registry import register_strategy
from icarus.util import inheritdoc, path_links
from .base import Strategy

__all__ = [
       'StrictestDeadlineFirst',
       'MostFrequentlyUsed',
       'Hybrid',
       'Lru'
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

# LRU
@register_strategy('LRU')
class Lru(Strategy):
    """A distributed approach for service-centric routing
    """
    def __init__(self, view, controller, replacement_interval=10, debug=False, p = 0.5, **kwargs):
        super(Lru, self).__init__(view,controller)
        self.last_replacement = 0
        self.replacement_interval = replacement_interval
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        self.p = p

    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            cs.running_requests = [0 for x in range(0, self.num_services)]
            cs.missed_requests = [0 for x in range(0, self.num_services)]
            cs.cpuInfo.idleTime = 0.0

    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, rtt_delay, status):
        """
        response : True, if this is a response from the cloudlet/cloud
        deadline : deadline for the request 
        flow_id : Id of the flow that the request/response is part of
        node : the current node at which the request/response arrived
        """
        
        service = content

        if time - self.last_replacement > self.replacement_interval:
            #self.print_stats()
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, time)
            self.last_replacement = time
            self.initialise_metrics()
        if receiver == node and status == REQUEST:
            self.controller.start_session(time, receiver, service, log, flow_id, deadline)
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay*2
            self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
            return

        if self.debug:
            print ("\nEvent\n time: " + repr(time) + " receiver  " + repr(receiver) + " service " + repr(service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(deadline) + " status " + repr(status)) 

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        else: # the node has no computational spots (0 services)
            if status is not RESPONSE:
                source = self.view.content_source(service)
                if node == source:
                    print ("Error: reached the source node: " + repr(node) + " this should not happen!")
                    return
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.debug:
                    print ("Pass upstream (no compSpot) to node: " + repr(next_node) + " " + repr(time+delay))
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay+2*delay, REQUEST)
                return
        
        if status == RESPONSE: 
            # response is on its way back to the receiver
            if node == receiver:
                self.controller.end_session(True, time, flow_id) #TODO add flow_time
                return
            else:
                path = self.view.shortest_path(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, RESPONSE)

        elif status == TASK_COMPLETE:
            task = compSpot.schedule(time)
            #schedule the next queued task at this node
            if task is not None:
                self.controller.add_event(task.finishTime, task.receiver, task.service, node, task.flow_id, task.expiry, task.rtt_delay, TASK_COMPLETE)

            # forward the completed task
            path = self.view.shortest_path(node, receiver)
            next_node = path[1]
            delay = self.view.link_delay(node, next_node)
            self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, RESPONSE)
            
        elif status == REQUEST:
            # Processing a request
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            ret, reason = compSpot.admit_task(service, time, flow_id, deadline, receiver, rtt_delay, self.controller, self.debug)
            if ret == False:
                if reason == NO_INSTANCES:
                    # is upstream possible to execute
                    if deadline - time - rtt_delay - 2*delay < compSpot.services[service].service_time:
                        evicted = self.controller.put_content(node, service)
                        compSpot.reassign_vm(evicted, service, self.debug)
                    elif self.p == 1.0 or random.random() <= self.p:
                        evicted = self.controller.put_content(node, service)
                        compSpot.reassign_vm(evicted, service, self.debug)
                rtt_delay += 2*delay
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
            else:
                self.controller.get_content(node, service)

@register_strategy('HYBRID')
class Hybrid(Strategy):
    """A distributed approach for service-centric routing
    """
    
    def __init__(self, view, controller, replacement_interval=10, debug=False, n_replacements=1, **kwargs):
        super(Hybrid, self).__init__(view,controller)
        self.replacement_interval = replacement_interval
        self.n_replacements = n_replacements
        self.last_replacement = 0
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        # metric to rank each VM of Comp. Spot
        self.deadline_metric = {x : {} for x in range(0, self.num_nodes)}
        self.cand_deadline_metric = {x : {} for x in range(0, self.num_nodes)}
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0
    
    # Hybrid
    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            cs.running_requests = [0 for x in range(0, self.num_services)]
            cs.missed_requests = [0 for x in range(0, self.num_services)]
            cs.cpuInfo.idleTime = 0.0
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0
    #HYBRID 
    def replace_services(self, k, time):
        """
        This method does the following:
        1. Evaluate instantiated and stored services at each computational spot for the past time interval, ie, [t-interval, t]. 
        2. Decide which services to instantiate in the next time interval [t, t+interval].
        Parameters:
        k : max number of instances to replace at each computational spot
        interval: the length of interval
        """
        # First sort services by deadline strictness
        for node, cs in self.compSpots.items():
            if cs.is_cloud:
                continue
            n_replacements = k
            service_deadlines = []
            service_util = [] #dict
            n_requests = {}
            utilisation_metrics = []
            cand_services = []
            vm_metrics = self.deadline_metric[node]
            cand_metric = self.cand_deadline_metric[node]
            if self.debug:
                print ("Replacement at node " + repr(node))
            for service in range(0, self.num_services):
                d_metric = 0.0
                u_metric = 0.0
                if cs.numberOfInstances[service] == 0 and cs.missed_requests[service] > 0:
                    d_metric = cand_metric[service]/cs.missed_requests[service]
                    u_metric = cs.missed_requests[service] * cs.services[service].service_time
                elif cs.numberOfInstances[service] > 0 and cs.running_requests[service] > 0: 
                    d_metric = vm_metrics[service]/cs.running_requests[service]
                    u_metric = cs.running_requests[service] * cs.services[service].service_time
                if d_metric > 0.0:
                    service_deadlines.append([service, d_metric])
                if u_metric > 0.0:
                   service_util.append([service, u_metric])
                n_requests[service] = cs.running_requests[service] + cs.missed_requests[service]
            service_deadlines = sorted(service_deadlines, key=lambda x: x[1]) # small to large
            services = []
            # Fill the memory with time critical services 
            for indx in range(0, len(service_deadlines)):
                service = service_deadlines[indx][0]
                metric = service_deadlines[indx][1]
                if cs.rtt_upstream[service] == 0:
                    #compute rtt to upstream node
                    source = self.view.content_source(service)
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    cs.rtt_upstream[service] = self.view.path_delay(node, next_node)
                if metric > cs.rtt_upstream[service]:
                    break
                services.append([service, n_requests[service]])
            
            # services that are not delegatable upstream and have decreasing utilisations
            services = sorted(services, key=lambda x: x[1], reverse=True) #large to small
            n_services = 0
            cs.numberOfInstances = [0]*cs.service_population 
            for indx in range(0, len(services)):
                service = services[indx][0]
                if n_requests[service] == 1:
                    break
                cs.numberOfInstances[service] = 1
                n_services += 1
                if n_services == cs.n_services:
                    break

            # fill the remaining slots with high utilisation services
            if (cs.n_services > n_services):
                print(repr(cs.n_services-n_services) + " slots filled with popular services")
                service_util = sorted(service_util, key=lambda x: x[1], reverse=True) # large to small
            else:
                print ("No slots for popular services")
            indx = 0
            while (cs.n_services > n_services) and (indx < len(service_util)):
                service = service_util[indx][0]
                indx += 1
                cs.numberOfInstances[service] += 1
                n_services += 1

    #HYBRID 
    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, rtt_delay, status):
        """
        response : True, if this is a response from the cloudlet/cloud
        deadline : deadline for the request 
        flow_id : Id of the flow that the request/response is part of
        node : the current node at which the request/response arrived
        """
        #self.debug = False
        #if node == 12:
        #    self.debug = True

        service = content

        if time - self.last_replacement > self.replacement_interval:
            #self.print_stats()
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, time)
            self.replace_services(self.n_replacements, time)
            self.last_replacement = time
            self.initialise_metrics()
        
        if receiver == node and status == REQUEST:
            self.controller.start_session(time, receiver, service, log, flow_id, deadline)
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay*2
            self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
            return

        if self.debug:
            print ("\nEvent\n time: " + repr(time) + " receiver  " + repr(receiver) + " service " + repr(service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(deadline) + " status " + repr(status)) 

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        else: # the node has no computational spots (0 services)
            if status is not RESPONSE:
                source = self.view.content_source(service)
                if node == source:
                    print ("Error: reached the source node: " + repr(node) + " this should not happen!")
                    return
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.debug:
                    print ("Pass upstream (no compSpot) to node: " + repr(next_node) + " " + repr(time+delay))
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay+2*delay, REQUEST)
                return
            
        if status == RESPONSE: 
            # response is on its way back to the receiver
            if node == receiver:
                self.controller.end_session(True, time, flow_id) #TODO add flow_time
                return
            else:
                path = self.view.shortest_path(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, RESPONSE)

        elif status == TASK_COMPLETE:
            task = compSpot.schedule(time)
            #schedule the next queued task at this node
            if task is not None:
                self.controller.add_event(task.finishTime, task.receiver, task.service, node, task.flow_id, task.expiry, task.rtt_delay, TASK_COMPLETE)

            # forward the completed task
            path = self.view.shortest_path(node, receiver)
            next_node = path[1]
            delay = self.view.link_delay(node, next_node)
            self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, RESPONSE)
            
        elif status == REQUEST:
            # Processing a request
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            deadline_metric = (deadline - time - rtt_delay - compSpot.services[service].service_time)/deadline
            if self.debug:
                print ("Deadline metric: " + repr(deadline_metric))
            if self.view.has_service(node, service):
                if self.debug:
                    print ("Calling admit_task")
                ret, reason = compSpot.admit_task(service, time, flow_id, deadline, receiver, rtt_delay, self.controller, self.debug)
                if self.debug:
                    print ("Done Calling admit_task")
                if ret is False:    
                    # Pass the Request upstream
                    rtt_delay += delay*2
                    self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service] += deadline_metric
                    if self.debug:
                        print ("Pass upstream to node: " + repr(next_node))
                else:
                    if deadline_metric > 0:
                        self.deadline_metric[node][service] += deadline_metric
            else: #Not running the service
                compSpot.missed_requests[service] += 1
                if self.debug:
                    print ("Not running the service: Pass upstream to node: " + repr(next_node))
                rtt_delay += delay*2
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
                if deadline_metric > 0:
                    self.cand_deadline_metric[node][service] += deadline_metric
        else:
            print ("Error: unrecognised status value : " + repr(status))


# Highest Utilisation First Strategy 
@register_strategy('MFU')
class MostFrequentlyUsed(Strategy):
    """A distributed approach for service-centric routing
    """
    
    def __init__(self, view, controller, replacement_interval=10, debug=False, n_replacements=1, **kwargs):
        super(MostFrequentlyUsed, self).__init__(view,controller)
        self.replacement_interval = replacement_interval
        self.n_replacements = n_replacements
        self.last_replacement = 0
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        # metric to rank each VM of Comp. Spot
        self.deadline_metric = {x : {} for x in range(0, self.num_nodes)}
        self.cand_deadline_metric = {x : {} for x in range(0, self.num_nodes)}
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0
    
    # MFU
    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            cs.running_requests = [0 for x in range(0, self.num_services)]
            cs.missed_requests = [0 for x in range(0, self.num_services)]
            cs.cpuInfo.idleTime = 0.0
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0
    # MFU
    def replace_services(self, k, time):
        """
        This method does the following:
        1. Evaluate instantiated and stored services at each computational spot for the past time interval, ie, [t-interval, t]. 
        2. Decide which services to instantiate in the next time interval [t, t+interval].
        Parameters:
        k : max number of instances to replace at each computational spot
        interval: the length of interval
        """

        for node, cs in self.compSpots.items():
            if cs.is_cloud:
                continue
            n_replacements = k
            vms = []
            cand_services = []
            vm_metrics = self.deadline_metric[node]
            cand_metric = self.cand_deadline_metric[node]
            if self.debug:
                print ("Replacement at node " + repr(node))
            for service in range(0, self.num_services):
                usage_metric = 0.0
                if cs.numberOfInstances[service] == 0:
                    if self.debug:
                        print ("No running instances of service: " + repr(service)) 
                    continue
                if cs.running_requests[service] == 0:
                    usage_metric = 0.0 
                    if self.debug:
                        print ("No scheduled requests for service: " + repr(service)) 
                else:
                    usage_metric = cs.running_requests[service] * cs.services[service].service_time
                    if self.debug:
                        print ("Usage metric for service: " + repr(service) + " is " + repr(usage_metric))

                vms.append([service, usage_metric])
            
            for service in range(0, self.num_services):
                #if cs.numberOfInstances[service] > 0:
                #    continue
                if cs.missed_requests[service] == 0:
                    usage_metric = 0.0
                else:
                    usage_metric = cs.missed_requests[service] * cs.services[service].service_time
                    if self.debug:
                        print ("Usage metric for Upstream Service: " + repr(service) + " is " + repr(usage_metric))
                cand_services.append([service, usage_metric])

            # sort vms and virtual_vms arrays according to metric
            vms = sorted(vms, key=lambda x: x[1]) #small to large
            cand_services = sorted(cand_services, key=lambda x: x[1], reverse=True) #large to small
            if self.debug:
                print ("VMs: " + repr(vms))
                print ("Cand. Services: " + repr(cand_services))
            # Small metric is better
            indx = 0
            for vm in vms: 
                if vm[1] > cand_services[indx][1]:
                    break
                else:
                    # Are they the same service? This should not happen really
                    if vm[0] != cand_services[indx][0]:
                        cs.reassign_vm(vm[0], cand_services[indx][0], self.debug)
                        n_replacements -= 1
                
                if n_replacements == 0 or indx == len(cand_services):
                    break
                indx += 1

    # MFU
    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, rtt_delay, status):
        """
        response : True, if this is a response from the cloudlet/cloud
        deadline : deadline for the request 
        flow_id : Id of the flow that the request/response is part of
        node : the current node at which the request/response arrived
        """
        service = content

        if time - self.last_replacement > self.replacement_interval:
            #self.print_stats()
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, time)
            self.replace_services(self.n_replacements, time)
            self.last_replacement = time
            self.initialise_metrics()
        
        if receiver == node and status == REQUEST:
            self.controller.start_session(time, receiver, service, log, flow_id, deadline)
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay*2
            self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
            return

        if self.debug:
            print ("\nEvent\n time: " + repr(time) + " receiver  " + repr(receiver) + " service " + repr(service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(deadline) + " status " + repr(status)) 

        # MFU
        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        else: # the node has no computational spots (0 services)
            if status is not RESPONSE:
                source = self.view.content_source(service)
                if node == source:
                    print ("Error: reached the source node: " + repr(node) + " this should not happen!")
                    return
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.debug:
                    print ("Pass upstream (no compSpot) to node: " + repr(next_node) + " " + repr(time+delay))
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay+2*delay, REQUEST)
                return
            
        if status == RESPONSE: 
            # response is on its way back to the receiver
            if node == receiver:
                self.controller.end_session(True, time, flow_id) #TODO add flow_time
                return
            else:
                path = self.view.shortest_path(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, RESPONSE)

        elif status == TASK_COMPLETE:
            task = compSpot.schedule(time)
            #schedule the next queued task at this node
            if task is not None:
                self.controller.add_event(task.finishTime, task.receiver, task.service, node, task.flow_id, task.expiry, task.rtt_delay, TASK_COMPLETE)

            # forward the completed task
            path = self.view.shortest_path(node, receiver)
            next_node = path[1]
            delay = self.view.link_delay(node, next_node)
            self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, RESPONSE)
            
        # MFU
        elif status == REQUEST:
            # Processing a request
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            deadline_metric = (deadline - time - rtt_delay - compSpot.services[service].service_time)/deadline
            if self.debug:
                print ("Deadline metric: " + repr(deadline_metric))
            if self.view.has_service(node, service):
                if self.debug:
                    print ("Calling admit_task")
                ret, reason = compSpot.admit_task(service, time, flow_id, deadline, receiver, rtt_delay, self.controller, self.debug)
                if self.debug:
                    print ("Done Calling admit_task")
                if ret is False:    
                    # Pass the Request upstream
                    rtt_delay += delay*2
                    self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service] += deadline_metric
                    if self.debug:
                        print ("Pass upstream to node: " + repr(next_node))
                else:
                    if deadline_metric > 0:
                        self.deadline_metric[node][service] += deadline_metric
            else: #Not running the service
                compSpot.missed_requests[service] += 1
                if self.debug:
                    print ("Not running the service: Pass upstream to node: " + repr(next_node))
                rtt_delay += delay*2
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
                if deadline_metric > 0:
                    self.cand_deadline_metric[node][service] += deadline_metric
        else:
            print ("Error: unrecognised status value : " + repr(status))

# Strictest Deadline First Strategy
@register_strategy('SDF')
class StrictestDeadlineFirst(Strategy):
    """ A distributed approach for service-centric routing
    """
    # SDF  
    def __init__(self, view, controller, replacement_interval=10, debug=False, n_replacements=1, **kwargs):
        super(StrictestDeadlineFirst, self).__init__(view,controller)
        self.replacement_interval = replacement_interval
        self.n_replacements = n_replacements
        self.last_replacement = 0
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        # metric to rank each VM of Comp. Spot
        self.deadline_metric = {x : {} for x in range(0, self.num_nodes)}
        self.cand_deadline_metric = {x : {} for x in range(0, self.num_nodes)}
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0
    
    # SDF  
    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            cs.running_requests = [0 for x in range(0, self.num_services)]
            cs.missed_requests = [0 for x in range(0, self.num_services)]
            cs.cpuInfo.idleTime = 0.0
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0.0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0.0
    # SDF  
    def replace_services(self, k, time):
        """
        This method does the following:
        1. Evaluate instantiated and stored services at each computational spot for the past time interval, ie, [t-interval, t]. 
        2. Decide which services to instantiate in the next time interval [t, t+interval].
        Parameters:
        k : max number of instances to replace at each computational spot
        interval: the length of interval
        """

        for node, cs in self.compSpots.items():
            if cs.is_cloud:
                continue
            n_replacements = k
            vms = []
            cand_services = []
            vm_metrics = self.deadline_metric[node]
            cand_metric = self.cand_deadline_metric[node]
            if self.debug:
                print ("Replacement at node " + repr(node))
            for service in range(0, self.num_services):
                if cs.numberOfInstances[service] == 0:
                    if self.debug:
                        print ("No running instances of service: " + repr(service)) 
                    continue
                d_metric = 0.0
                if cs.running_requests[service] == 0:
                    d_metric = 1.0 
                    if self.debug:
                        print ("No scheduled requests for service: " + repr(service)) 
                else:
                    d_metric = vm_metrics[service]/cs.running_requests[service]
                    if self.debug:
                        print ("Deadline metric for service: " + repr(service) + " is " + repr(d_metric))

                vms.append([service, d_metric])
            # SDF  
            for service in range(0, self.num_services):
                #if self.debug:
                #    print ("\tNumber of Requests for upstream service " + repr(service) + " is "  + repr(cs.missed_requests[service]))
                d_metric = 0.0
                if cs.missed_requests[service] == 0:
                    d_metric = 1.0
                else:
                    d_metric = cand_metric[service]/cs.missed_requests[service]
                    if self.debug:
                        print ("Deadline metric for Upstream Service: " + repr(service) + " is " + repr(d_metric))
                cand_services.append([service, d_metric])
            # sort vms and virtual_vms arrays according to metric
            vms = sorted(vms, key=lambda x: x[1], reverse=True) #larger to smaller
            cand_services = sorted(cand_services, key=lambda x: x[1]) #smaller to larger
            if self.debug:
                print ("VMs: " + repr(vms))
                print ("Cand. Services: " + repr(cand_services))
            # Small metric is better
            indx = 0
            for vm in vms:
                # vm's metric is worse than the cand. and they are not the same service
                if vm[1] < cand_services[indx][1]:
                    break
                else:
                    if vm[0] != cand_services[indx][0]:
                       cs.reassign_vm(vm[0], cand_services[indx][0], self.debug)
                       n_replacements -= 1
                
                if n_replacements == 0 or indx == len(cand_services):
                    break
                indx += 1
    # SDF  
    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, rtt_delay, status):
        """
        response : True, if this is a response from the cloudlet/cloud
        deadline : deadline for the request 
        flow_id : Id of the flow that the request/response is part of
        node : the current node at which the request/response arrived
        """
        service = content

        if time - self.last_replacement > self.replacement_interval:
            #self.print_stats()
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, time)
            self.replace_services(self.n_replacements, time)
            self.last_replacement = time
            self.initialise_metrics()
        
        if receiver == node and status == REQUEST:
            self.controller.start_session(time, receiver, service, log, flow_id, deadline)
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay*2
            self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
            return

        if self.debug:
            print ("\nEvent\n time: " + repr(time) + " receiver  " + repr(receiver) + " service " + repr(service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(deadline) + " status " + repr(status)) 

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        else: # the node has no computational spots (0 services)
            if status is not RESPONSE:
                source = self.view.content_source(service)
                if node == source:
                    print ("Error: reached the source node: " + repr(node) + " this should not happen!")
                    return
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.debug:
                    print ("Pass upstream (no compSpot) to node: " + repr(next_node) + " " + repr(time+delay))
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay+2*delay, REQUEST)
                return
        # SDF  
        if status == RESPONSE: 
            # response is on its way back to the receiver
            if node == receiver:
                self.controller.end_session(True, time, flow_id) #TODO add flow_time
                return
            else:
                path = self.view.shortest_path(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, RESPONSE)

        elif status == TASK_COMPLETE:
            task = compSpot.schedule(time)
            #schedule the next queued task at this node
            if task is not None:
                self.controller.add_event(task.finishTime, task.receiver, task.service, node, task.flow_id, task.expiry, task.rtt_delay, TASK_COMPLETE)

            # forward the completed task
            path = self.view.shortest_path(node, receiver)
            next_node = path[1]
            delay = self.view.link_delay(node, next_node)
            self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, RESPONSE)
            
        # SDF  
        elif status == REQUEST:
            # Processing a request
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            deadline_metric = (deadline - time - rtt_delay - compSpot.services[service].service_time)/deadline
            if self.debug:
                print ("Deadline metric: " + repr(deadline_metric))
            if self.view.has_service(node, service):
                if self.debug:
                    print ("Calling admit_task")
                ret, reason = compSpot.admit_task(service, time, flow_id, deadline, receiver, rtt_delay, self.controller, self.debug)
                if self.debug:
                    print ("Done Calling admit_task")
                if ret is False:    
                    # Pass the Request upstream
                    rtt_delay += delay*2
                    self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service] += deadline_metric
                    if self.debug:
                        print ("Pass upstream to node: " + repr(next_node))
                else:
                    if deadline_metric > 0:
                        self.deadline_metric[node][service] += deadline_metric
            else: #Not running the service
                compSpot.missed_requests[service] += 1
                if self.debug:
                    print ("Not running the service: Pass upstream to node: " + repr(next_node))
                rtt_delay += delay*2
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
                if deadline_metric > 0:
                    self.cand_deadline_metric[node][service] += deadline_metric
        else:
            print ("Error: unrecognised status value : " + repr(status))


