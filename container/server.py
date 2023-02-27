import json
import os
import socket
import threading
import time
import random
import queue

NODE_ID = os.environ['NODE_ID']
OTHER_NODES = os.environ['OTHER_NODES'].split('|')


def read_state():
    if not os.path.exists('state.json'):
        return {'voted_for': None, 'current_term': 0, 'log': [{'Term': 0, 'Key': None, 'Value': None}]}
    try:
        with open('state.json', 'r') as f:
            data = json.load(f)

        if len(data['log']) == 0 or (len(data['log']) > 0 and data['log'][0]['Key'] is not None):
            data['log'] = [{'Term': 0, 'Key': None, 'Value': None}] + data['log']
    except:
        print('Failed to load Json data, among files:', os.listdir("."))
        raise
    return data


def save_state(data):
    with open('state.json', 'w') as f:

        if len(data['log']) > 0 and data['log'][0]['Key'] is None:
            data['log'] = data['log'][1:]

        json.dump(data, f)


class RAFT:
    def __init__(self):
        self.stop_heartbeat = False
        self.stop_election = False
        self.force_timeout = False
        self.accept_requests = True

        self.other_nodes = OTHER_NODES
        self.id = NODE_ID

        self.state = 'follower'
        self.leader_id = None
        self.inbox = queue.Queue()
        self.outbox = queue.Queue()

        self.election_thread = None
        self.match_index = None
        self.commit_index = 0

        # log format: [(<term>, <cmd>)]
        data = read_state()
        self.term = data['current_term']
        self.log = data['log']
        self.voted_for = data['voted_for']

        # next-index keeps track of the log entry that is to be send to given node in next heartbeat
        # since log has 1 base entry, and 0 is the prev-log-index hence next-index has to be 1
        self.next_index = {node: self.commit_index+1 for node in OTHER_NODES}
        self.timeout_tick = time.perf_counter()
        self.heartbeat_freq = 0.2  # 200 ms
        # Election timeout ranges from 10*HEARTBEAT to 30*HEARTBEAT (i.e 700 ms to 2.1 s)
        self.election_timeout = 4 * self.heartbeat_freq + (random.random() * 6 * self.heartbeat_freq)

        # print(f"{NODE_ID} - Timeout - {self.election_timeout:.3f}")

        self.votes_received = 0

        # setup socket
        self.PORT = 5555
        self.SKT = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.SKT.bind(("0.0.0.0", self.PORT))
        self.SKT.setblocking(False)

    def persist_state(self):
        data = {'log': self.log, 'current_term': self.term, 'voted_for':self.voted_for}
        threading.Thread(target=save_state, args=[data]).start()
        return

    def handle_incoming(self, incoming_request):
        # priority 1 - controller requests
        if incoming_request['sender_name'] == 'Controller':
            self.handle_controller_requests(incoming_request)
        else:
            # because timeout check is not asynchronous now
            if incoming_request['term'] < self.term:
                if incoming_request['request'] == 'APPEND_ENTRY':
                    msg = self.prepare_msg({'request': 'APPEND_REPLY', 'success': False})
                    self.outbox.put((msg, incoming_request['sender_name']))
                # otherwise ignore

                return

            elif incoming_request['term'] > self.term:
                print(f"{self.id} becoming follower,"
                      f" recvd updated term msg {incoming_request['term']} from {incoming_request['sender_name']}")
                self.become_follower()
                self.update_term(incoming_request['term'])

            # incoming_request['term'] >= self.term
            if incoming_request['request'] == 'APPEND_ENTRY':  # DONE
                self.handle_append_entry(incoming_request)
            elif incoming_request['request'] == 'REQUEST_VOTE':  # DONE
                self.handle_vote_request(incoming_request)
            elif incoming_request['request'] == 'VOTE_ACK':  # DONE
                self.handle_vote_ack(incoming_request)
            elif incoming_request['request'] == 'APPEND_REPLY':
                self.handle_append_reply(incoming_request)

        return

    def process(self):
        # reset time counter once before start processing
        self.reset_time_counter()
        print(f'{NODE_ID} Starts Processing')
        while self.accept_requests:
            try:
                incoming_request = self.inbox.get(block=False)
                self.handle_incoming(incoming_request)
            except queue.Empty:
                pass

            if self.state == 'follower' and self.reached_timeout() and not self.has_voted():
                self.stop_election = False
                # Only way thread would exit is if you win election(become leader) or someone else does (become
                # follower)
                if self.election_thread is None:
                    self.election_thread = threading.Thread(target=self.start_election)
                    self.election_thread.start()

        pass

    def msg_listener(self):
        # print(f'Starting message listener on {self.id}', file=sys.stderr)
        while self.accept_requests:
            try:
                msg, addr = self.SKT.recvfrom(1024)
                decoded_msg = json.loads(msg.decode('utf-8'))
                self.inbox.put(decoded_msg)
            except socket.error as e:
                pass
        return

    def handle_append_entry(self, incoming_request):
        self.reset_time_counter()
        # reset time counter
        self.update_term(incoming_request['term'])

        if self.state == 'leader':
            # Ideally this case shouldn't arise, still ignore, and notify of such race condition
            # term = my_term and state == leader
            print('ERROR: WIERD CONDITION! CURRENT LEADER JUST RECEIVED APPEND ENTRY')
        elif self.state == 'candidate':
            self.become_follower()
            self.update_leader(incoming_request['sender_name'])
        elif self.state == 'follower':
            self.update_leader(incoming_request['sender_name'])

        # append reply
        debug = {'sender': incoming_request['sender_name'],
                 'sender_prev_log_idx': incoming_request['prevLogIndex'],
                 'sender_prev_log_term': incoming_request['prevLogTerm'],
                 'my_prev_log_term': self.get_prev_log_term(self.id),
                 'my_prev_log_idx': self.get_prev_log_index(self.id)}
        if self.log_consistency_check(incoming_request['prevLogIndex'], incoming_request['prevLogTerm']):
            # If I have additional logs that don't match, truncate my logs
            if incoming_request['prevLogIndex'] < len(self.log):
                self.log = self.log[:incoming_request['prevLogIndex'] + 1]

            # append to own log if new valid entry
            if incoming_request['entry'] is not None and incoming_request['entry'] not in self.log:
                self.log.append(incoming_request['entry'])
                # print(f"+++++Successful Append:{incoming_request['entry']}, self.log:{self.log}")
                print(f"+++++Successful Append: self.log:{self.log[1:]}")

            self.persist_state()
            self.commit_index = incoming_request['commitIndex']
            append_success = True

        else:
            # debug['consistency_check'] = 'failed'
            # print(f"*&*&*&Failed Append Entry: {debug}")
            append_success = False

        msg = self.prepare_msg({'request': 'APPEND_REPLY', 'success': append_success})
        self.outbox.put((msg, incoming_request['sender_name']))

        return

    def handle_vote_request(self, incoming_request):
        # print(f"{NODE_ID} received Voting Request from {incoming_request['sender_name']}")
        if (not self.has_voted()) and self.state != 'leader' and self.is_eligible_candidate(incoming_request):
            print(f"VOTING FOR {incoming_request['sender_name']} term-{incoming_request['term']} \
                  timeout:{time.perf_counter() - self.timeout_tick:.3f}/{self.election_timeout:.3f}")
            msg = self.prepare_msg({'request': 'VOTE_ACK', 'success': True})
            self.outbox.put((msg, incoming_request['sender_name']))
            self.voted_for = incoming_request['sender_name']
            self.persist_state()
        else:
            msg = self.prepare_msg({'request': 'VOTE_ACK', 'success': False})
            self.outbox.put((msg, incoming_request['sender_name']))

        return

    def handle_vote_ack(self, incoming_request):
        # count vote only if success
        if incoming_request['success']:
            print(f"RECEIVED VOTE FROM {incoming_request['sender_name']}")
            self.votes_received += 1
        return

    def send_heartbeat(self):
        """
        :return:
        """
        print('Starting Send Heartbeat')
        # Init Start time such that first heartbeat is sent right away!
        start_time = time.perf_counter() - self.heartbeat_freq - 1

        while self.accept_requests and (not self.stop_heartbeat):
            if time.perf_counter() - start_time > self.heartbeat_freq:
                # print(f'{NODE_ID} sending heartbeat')
                for node in self.other_nodes:
                    # logs are up to date
                    # send empty append entry

                    # Empty append entry (just the heartbeat)
                    msg = {'request': 'APPEND_ENTRY',
                     'prevLogIndex': self.get_prev_log_index(node),
                     'prevLogTerm': self.get_prev_log_term(node),
                     'entry': None if self.logs_updated(node) else self.log[self.next_index[node]],
                     'commitIndex': self.commit_index}
                    # print(f"heartbeat sent to {node}: {msg}")
                    msg = self.prepare_msg(msg)
                    self.outbox.put((msg, node))
                start_time = time.perf_counter()

        return

    def has_won(self):
        return self.votes_received > 0.5 * (len(self.other_nodes) + 1)

    def wait_for_election_results(self):
        while not (self.stop_election or self.reached_timeout() or self.has_won()):
            # if self.timeout_tick % 100 == 0:
            # print('****Waiting for Votes****')
            # we're waiting for votes here
            continue
        return

    def contest_election(self):
        # check if it has received heartbeat or not
        self.reset_time_counter()
        # print(f'Before becoming candidate: stop_election={self.stop_election}')
        if not self.stop_election:
            self.become_candidate()
            self.update_term(self.term + 1)
            self.votes_received += 1
            self.voted_for = self.id
            self.persist_state()
            for node in self.other_nodes:
                msg = self.prepare_msg({'request': 'REQUEST_VOTE', 'last_log_index': len(self.log) - 1,
                                        'last_log_term': self.log[-1]['Term']})
                self.outbox.put((msg, node))
        return

    def start_election(self):
        # become candidate
        # Increment term
        # Vote for yourself
        # Send request Vote RPC

        # Reset Force-timeout request in-case it triggered
        self.force_timeout = False
        while not (self.stop_election or self.state == 'leader'):
            # keep on reelecting
            print(f'{NODE_ID} started election for term:{self.term + 1}')
            self.contest_election()
            self.wait_for_election_results()

            # if self.stop_election:
            #     return

            if self.has_won():
                print(
                    f'!!!!!{NODE_ID} won election for term-{self.term} with {self.votes_received}/{len(OTHER_NODES) + 1} votes!!!!!')
                self.become_leader()
            elif self.stop_election:
                print(f'ABORTING Election {self.leader_id} is the new leader!')
            else:
                print(
                    f'{NODE_ID} Lost Election: for term-{self.term}. {self.votes_received}/{len(OTHER_NODES) + 1} votes received :(')

        # reset election thread
        self.election_thread = None
        return

    def update_leader(self, new_leader_id):
        # change state to follower
        self.leader_id = new_leader_id
        return

    def update_term(self, new_term):
        self.term = new_term
        # print(f'Toggling voted_for to None from {self.voted_for}')
        self.voted_for = None
        self.votes_received = 0
        self.persist_state()
        return

    def become_candidate(self):
        self.state = 'candidate'
        return

    def become_follower(self):
        self.state = 'follower'
        self.stop_election = True
        self.stop_heartbeat = True
        self.reset_time_counter()
        return

    def become_leader(self):
        self.state = 'leader'
        self.update_leader(self.id)
        self.stop_heartbeat = False
        # Assume that all followers' logs are as updated as yours
        self.next_index = {node: self.commit_index+1 for node in OTHER_NODES}
        self.match_index = {node: 0 for node in OTHER_NODES}
        self.match_index[self.id] = len(self.log) - 1
        threading.Thread(target=self.send_heartbeat).start()
        # reset voted_for and votes_received
        self.update_term(self.term)

        return

    def reset_time_counter(self):
        self.timeout_tick = time.perf_counter()
        return

    def reached_timeout(self):
        return (time.perf_counter() - self.timeout_tick > self.election_timeout) or self.force_timeout

    def has_voted(self):
        return self.voted_for is not None

    def prepare_msg(self, data):
        msg = {"sender_name": self.id, "request": "", "term": self.term, "key": "", "value": "",
               'prevLogIndex': self.get_prev_log_index(self.id), 'prevLogTerm': self.get_prev_log_term(self.id)}
        msg.update(data)
        return msg

    def msg_sender(self):
        # delay start of thread
        # print(f'Starting message sender on {self.id}', file=sys.stderr)
        while self.accept_requests:
            try:
                msg, dst = self.outbox.get(block=False)
                encoded_msg = json.dumps(msg).encode()
                self.SKT.sendto(encoded_msg, (dst, self.PORT))
            except queue.Empty:
                pass
            except socket.gaierror:
                print(f'{dst} seems down')
        return

    def handle_controller_requests(self, incoming_request):
        print(f"Received Controller Request: {incoming_request['request']}")
        if incoming_request['request'] == 'STORE':
            if self.state != 'leader':
                msg = self.prepare_msg({'request': 'LEADER_INFO', 'key': 'LEADER', 'value': self.leader_id})

                self.outbox.put((msg, incoming_request['sender_name']))
                # print('outbox:', incoming_request['sender_name'], msg)
            else:
                self.log.append({'Term': self.term, 'Key': incoming_request['Key'], 'Value': incoming_request['Value']})
                self.match_index[self.id] = len(self.log) - 1
                self.persist_state()

        elif incoming_request['request'] == 'RETRIEVE':
            if self.state != 'leader':
                msg = self.prepare_msg({'request': 'LEADER_INFO', 'key': 'LEADER', 'value': self.leader_id})
                self.outbox.put((msg, incoming_request['sender_name']))
            else:
                msg = self.prepare_msg({'request': 'RETRIEVE', 'key': 'COMMITTED_LOGS',
                                        'value': self.log[1: self.commit_index+1]})
                # print('Replying with:', msg)
                self.outbox.put((msg, incoming_request['sender_name']))

        elif incoming_request['request'] == 'CONVERT_FOLLOWER':
            self.become_follower()
            self.update_leader(None)
        elif incoming_request['request'] == 'TIMEOUT':
            self.force_timeout = True
        elif incoming_request['request'] == 'SHUTDOWN':
            self.accept_requests = False
        elif incoming_request['request'] == 'LEADER_INFO':
            msg = self.prepare_msg({'request': 'LEADER_INFO', 'key': 'LEADER', 'value': self.leader_id})
            self.outbox.put((msg, incoming_request['sender_name']))

        return

    def log_consistency_check(self, node_prev_log_index, node_prev_log_term):
        if node_prev_log_index >= len(self.log):
            return False
        return self.log[node_prev_log_index]['Term'] == node_prev_log_term

    def get_prev_log_term(self, node):
        if node == self.id:
            log_entry = self.log[-1]
        else:
            try:
                log_entry = self.log[self.get_prev_log_index(node)]
            except:
                print(f"Index Error: {node}, prev_log_index(node): {self.get_prev_log_index(node)}, log: {self.log}")
                raise
        if log_entry is None:
            print('~~~~NONE LOG ENTRY', node, log_entry, self.get_prev_log_index(node), self.log)
        return log_entry['Term']

    def get_prev_log_index(self, node):
        if node == self.id:
            return len(self.log) - 1
        return max(0, self.next_index[node] - 1)

    def is_eligible_candidate(self, incoming_request):
        outdated_log_term = incoming_request['last_log_term'] < self.log[-1]['Term']
        outdated_log_index = (incoming_request['last_log_term'] == self.log[-1]['Term'] and
                              incoming_request['last_log_index'] < len(self.log) - 1)

        # print(f"incoming: {incoming_request['sender_name']}, incoming['last_log_term']: {incoming_request['last_log_term']},\
        #  incoming['last_log_index'], {incoming_request['last_log_index']}, self.log:{self.log}")
        return not (outdated_log_term or outdated_log_index)

    def handle_append_reply(self, incoming_request):

        # case 1 - append entry succeeded
        node = incoming_request['sender_name']
        if incoming_request['success']:
            self.match_index[node] = incoming_request['prevLogIndex']
            self.next_index[node] = incoming_request['prevLogIndex'] + 1
            self.update_commit_idx()
        # case 2 - append entry failed
        else:
            print(f"AppendEntry {self.next_index[incoming_request['sender_name']]} Failed for:{incoming_request['sender_name']}")
            if self.next_index[node] <= 1:
                print('Wierd, failed 1st append entry, was about to decrement to base index')
            else:
                self.next_index[node] -= 1
            # self.next_index[incoming_request['sender_name']] = max(0,
            # self.next_index[incoming_request['sender_name']] - 1)
        return

    def update_commit_idx(self):
        max_n = sorted(self.match_index.values())[(len(self.other_nodes) + 1) // 2]
        possible_ns = range(self.commit_index, min(max_n, len(self.log) - 1) + 1)
        n = None
        for i in possible_ns:
            if self.log[i]['Term'] == self.term:
                n = i
        if n is not None:
            self.commit_index = n
        return

    def logs_updated(self, node):
        return self.next_index[node] >= len(self.log)


def main():
    node = RAFT()
    threading.Thread(target=node.msg_listener).start()
    print(f'{node.id} IS UP')
    # let all nodes start listener first
    time.sleep(5)
    threading.Thread(target=node.process).start()

    threading.Thread(target=node.msg_sender).start()

    while True:
        continue


if __name__ == '__main__':
    main()
