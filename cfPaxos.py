#!/usr/bin/env python

'''
This program is used to simulate the basic Paxos algorithm.
The Proposer is also the Learner who finally learns which proposal is chosen. The Acceptor receives proposal and make desision.
We simulate the case that network is broken so the message can not reach to a target. But we do not simulate the absence of participators.
The global unique proposal_id is generated with the equation : proposal_id = N*max_proposer_num + proposer_ID

To make it simple, please reference the figures in http://www.jianshu.com/p/d9d067a8a086
'''

from Queue import Empty
from Queue import Queue
import random
import threading
import time


mutex = threading.Lock()

gvalues = [ "[Operation A]",
            "[Operation B]",
            "[Operation C]",
            "[Operation D]",
            "[Operation E]",
            "[Operation F]"]


live_proposer_num = 0
live_acceptor_num = 0
acceptors_num = 3
proposers_num = 3
max_proposer_num = 20           # It is used to generate unique sequence ID for each proposer
acceptor_can_ignore = False     # Used to simulate whether an acceptor sends response to proposer when rejecting proposal
debug_level = 1

def printStr(string, level=0):
    if level <= debug_level:
        mutex.acquire()
        print "%s::%s" % (str(time.time()), string)
        mutex.release()

# Counting how many proposer is proposing
def proposer_live(inc):
    mutex.acquire()
    global live_proposer_num
    if inc:
        live_proposer_num += 1
    else:
        live_proposer_num -= 1
    mutex.release()


# Counting how many acceptor is listening
def acceptor_live(inc):
    mutex.acquire()
    global live_acceptor_num
    if inc:
        live_acceptor_num += 1
    else:
        live_acceptor_num -= 1
    mutex.release()


class Proposer(threading.Thread):
    def __init__(self, t_name,          # Name of the thread
                 queue_from_acceptor,   # Queue to get message from acceptors
                 queue_to_acceptors,    # Queues to send message to acceptors
                 id,                    # The ID of current proposer
                 m_acceptor_num):       # Total number of of acceptor
        super(Proposer, self).__init__()
        self.queue_recv = queue_from_acceptor
        self.queue_send_list = queue_to_acceptors
        self.m_acceptor_num = m_acceptor_num
        self.live_acceptor_num = 0      # Number of acceptors this thread talks to
        self.max_proposal_id = 0        # The max proposal ID return from acceptors
        self.name = t_name
        self.id = id
        self.reject = 0
        self.accept = 0
        self.status = None
        self.N = 0                      # Used to generate global unique proposal ID
        self.value = None
        proposer_live(True)


    def run(self):
        # raise a proposal
        self.status = "prepare"
        self.value = gvalues[random.randrange(0, 6)]    # It can be None until sending accept to acceptors

        while True:
            # In "prepare" stage, send proposal to acceptors
            self.sendPropose()
            # Receive response from all acceptors
            while True:
                try:
                    var = self.queue_recv.get(True, 4)
                    self.processMsg(var)
                    if (self.reject + self.accept) == self.live_acceptor_num:
                        printStr("Stage %s; %s get response from all acceptors." % (self.status, self.name), 2)
                        break
                except Empty:
                    # At least one acceptor dose not send response to the proposer
                    printStr("Stage %s; %s get timeout when waiting response from acceptors" % (self.status, self.name), 2)
                    break

            # Summarize all response from acceptors
            # If not majority agree on the proposal, increase proposal ID and raise proposal gain
            if not self.summarize():
                continue

            # In "accept" stage, send proposal to acceptors
            self.sendPropose()
            # Receive response from all acceptors
            while True:
                try:
                    var = self.queue_recv.get(True, 1)
                    self.processMsg(var)
                    if (self.reject + self.accept) == self.live_acceptor_num:
                        printStr("Stage %s; %s get response from all acceptors." % (self.status, self.name), 2)
                        break
                except Empty:
                    # At least one acceptor dose not send response to the proposer
                    printStr("Stage %s; %s get timeout when waiting response from acceptors" % (self.status, self.name), 2)
                    break

            # Summarize all response from acceptors
            # If not majority agree on the proposal, increase proposal ID and raise proposal gain
            if not self.summarize():
                continue

            # If majority agree on the proposal, print the learned "value" and exit
            printStr("\033[1;32m####  %s get final agreement from majority. Agree on proposal : %s. Thread exit.\033[0m" % (self.name, self.value), 0)
            proposer_live(False)
            break


    def processMsg(self, var):
        if self.status == "prepare" and var["type"] == "prepare":
            if var["result"] == "reject":
                self.reject += 1
            elif var["result"] == "accept":
                self.accept += 1
                # In prepare stage, multiple acceptors respond to the proposer, chose the response with valid "value" and max proposal_id
                # and use the associated value as its own value and go to the accept stage
                # else the proposor uses its own original value
                if var.get("value", None) and var.get("proposal_id", None) and var["proposal_id"] > self.max_proposal_id:
                    # remember the max proposal_id
                    self.max_proposal_id = var["proposal_id"]
                    self.value = var["value"]
        elif self.status == "accept" and var["type"] == "accept":
            if var["result"] == "reject":
                self.reject += 1
            elif var["result"] == "accept":
                self.accept += 1


    def sendPropose(self):
        self.reject = 0
        self.accept = 0

        time.sleep(1/random.randrange(1, 20))
        body = {
            "type": self.status,
            "proposal_id": (self.N * max_proposer_num + self.id),
            "value": self.value,
            "proposer": self.id
        }

        # Send message to acceptors, simulating network broken with 10% ratio
        selected_acceptor = []
        selected_id = []
        i = 0
        for acceptor in self.queue_send_list:
            if random.randrange(100) < 90:
                selected_acceptor.append(acceptor)
                selected_id.append(i)
            i += 1
        self.live_acceptor_num = len(selected_acceptor)

        printStr("Stage %s; %s propose with proposal ID = %d, value = %s. Message reaches Acceptor %s" % (self.status, self.name, body["proposal_id"], self.value, str(selected_id)), 1)
        for acceptor in selected_acceptor:
            acceptor.put(body)
            time.sleep(1/random.randrange(1, 10))


    def summarize(self):
        getAgree = True
        printStr("Stage %s; %s proposes %s summary : %d accept and %d reject" % (self.status, self.name, self.value, self.accept, self.reject), 1)
        if self.accept > self.m_acceptor_num / 2:
            # If get majority accept, go to next stage
            if self.status == "prepare":
                self.status = "accept"
            else:
                self.status = "prepare"
        else:
            # else increase proposal ID and raise proposal again
            self.N += 1
            getAgree = False
            self.status = "prepare"

        return getAgree



class Acceptor(threading.Thread):
    def __init__(self, t_name, queue_from_proposer, queue_to_proposers, id):
        super(Acceptor, self).__init__()
        self.name = t_name
        self.queue_recv = queue_from_proposer
        self.queue_to_proposers = queue_to_proposers
        self.id = id
        self.max_responded_proposal_id = None   # It is the max proposal ID which it responds to proposor in "prepare" stage
        self.max_accepted_proposal_id = None    # It is the max proposal ID which it accepts in "accept" stage
        self.value = None
        acceptor_live(True)

    def run(self):
        while True:
            try:
                var = self.queue_recv.get(True, 1)
                ignore, resp =self.processPropose(var)

                if ignore and acceptor_can_ignore:
                    continue

                # Simulating the network failure with 10% ratio
                if random.randrange(100) < 90:
                    self.queue_to_proposers[var["proposer"]].put(resp)
                    printStr("Stage %s; %s responds to Proposer%d with %s" % (var["type"], self.name, var["proposer"], str(resp)), 2)
                    # printStr("Stage %s; %s responds to Proposer%d with proposal_id = %d, result = %s, value = %s" % (var["type"], self.name, var["proposer"], vars.get("proposal_id", -1), vars["result"], vars.get("value", None)), 2)
                else:
                    printStr("Stage %s; %s fails to respond to Proposer%d." % (var["type"], self.name, var["proposer"]), 2)
                    pass
            except Empty:
                pass

            if live_proposer_num == 0:
                acceptor_live(False)
                break
            continue

    def processPropose(self, var):
        ignore = False
        res = {"type":var["type"], "acceptor":self.id}

        if var["type"] == "prepare":
            if not self.max_responded_proposal_id:
                # If it never seen a proposal, promise never accept a proposal with proposal ID less than var["proposal_id"] in future
                self.max_responded_proposal_id = var["proposal_id"]
                res["result"] = "accept"
                res["proposal_id"] = var["proposal_id"]     # return the proposal ID as the max ID that the accepter has ever seen
            elif self.max_responded_proposal_id > var["proposal_id"]:
                # If ever seen a proposal with higher proposal ID, ignore the message or respond with "reject"
                # Responding the message can optimize performance to avoid network timeout in proposer side
                res["result"] = "reject"
                ignore = True
            elif self.max_responded_proposal_id == var["proposal_id"]:
                # Should never go into this case
                res["result"] = "reject"
                ignore = True
            else:   # self.max_accepted_proposal_id && var["proposal_id"] > self.max_responded_proposal_id
                # If it receives a proposal with a higher proposal ID than what it has ever seen, accept the proposal
                # and respond with the max proposal ID and its associated value (if there is a value)
                res["result"] = "accept"
                res["proposal_id"] = self.max_accepted_proposal_id      # return the max ID it has ever accepted (It can be None)
                res["value"] = self.value                               # return the value associated with the max ID (It can be None)
                self.max_responded_proposal_id = var["proposal_id"]     # promise it will never accept a proposal ID less than var["proposal_id" in future
        elif var["type"] == "accept":
            if self.max_responded_proposal_id > var["proposal_id"]:
                # If has ever seen a proposal with higher proposal ID, ignore the message or respond with "reject"
                # Responding the message can optimize performance to avoid network timeout in proposer side
                res["result"] = "reject"
                ignore = True
            else:
                # If it receives a proposal with a higher proposal ID that what it has ever seen, accept the proposal.
                # The message should be sent to a logical leaner. Because there is no leaner here, so respond to proposer
                res["result"] = "accept"
                self.max_accepted_proposal_id = var["proposal_id"]
                self.value = var["value"]
        return ignore, res




if __name__ == '__main__':
    q_to_acceptors = []
    q_to_proposers = []
    proposers = []
    acceptors = []

    q_leader_to_proposers = []
    q_to_leader = Queue()

    for i in range(0, acceptors_num):
        q_to_acceptors.append(Queue())

    for i in range(0, proposers_num):
        q_to_proposers.append(Queue())
        q_leader_to_proposers.append(Queue())

    for i in range(0, proposers_num):
        proposers.append(Proposer("Proposer%d" % i,
                                 q_to_proposers[i],
                                 q_to_acceptors,
                                 i,
                                 acceptors_num))

    for i in range(0, acceptors_num):
        acceptors.append(Acceptor("Acceptor%d" % i,
                                 q_to_acceptors[i],
                                 q_to_proposers,
                                 i))

    for i in range(0, acceptors_num):
        acceptors[i].setDaemon(True)
        acceptors[i].start()

    for i in range(0, proposers_num):
        proposers[i].setDaemon(True)
        proposers[i].start()

    while True:
        time.sleep(1)
        if live_acceptor_num == 0:
            break
