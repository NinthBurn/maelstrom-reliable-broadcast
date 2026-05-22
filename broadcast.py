#!/usr/bin/env python3

import json
import queue
import sys
import threading
import time
from collections import defaultdict


class Event:
    def __init__(self, event_type, **data):
        self.type = event_type
        self.data = data
        self.parent = None
        self.children = []

    def child(self, event_type, **data):
        child = Event(event_type, **data)
        child.parent = self
        self.children.append(child)
        return child



# NODE / PERFECT LINKS
class Node:
    def __init__(self):
        self.node_id = None
        self.node_ids = []

        self.msg_id = 0
        self.running = True

        self.event_queue = queue.Queue()


    def log(self, message):
        sys.stderr.write(message + "\n")
        sys.stderr.flush()


    def next_msg_id(self):
        msg_id = self.msg_id
        self.msg_id += 1
        return msg_id


    def send(self, dest, body):
        if "msg_id" not in body:
            body["msg_id"] = self.next_msg_id()

        msg = {
            "src": self.node_id,
            "dest": dest,
            "body": body
        }

        sys.stdout.write(json.dumps(msg) + "\n")
        sys.stdout.flush()


    def reply(self, req, body):
        body["in_reply_to"] = req["body"]["msg_id"]
        self.send(req["src"], body)


    # PL Deliver
    # stdin -> event queue
    def stdin_loop(self):
        while self.running:
            line = sys.stdin.readline()

            if not line:
                break

            try:
                message = json.loads(line)

                self.event_queue.put(
                    Event(
                        "pl_deliver",
                        message=message
                    )
                )

            except Exception as e:
                self.log(f"stdin error: {e}")



class PerfectFailureDetector:
    HEARTBEAT_REQUEST = "heartbeat_request"
    HEARTBEAT_REPLY = "heartbeat_reply"

    def __init__(self, event_handler):
        self.event_handler = event_handler
        self.alive = set()
        self.detected = set()
        self.delta = 1.0

    def init(self):
        node = self.event_handler.node
        self.alive = set(node.node_ids)
        self.detected = set()


    def timer_loop(self):
        while self.event_handler.node.running:
            time.sleep(self.delta)

            self.event_handler.node.event_queue.put(
                Event("pfd_timeout")
            )


    def handle_timeout(self):

        node = self.event_handler.node

        for process in node.node_ids:

            if process == node.node_id:
                continue

            if (process not in self.alive and process not in self.detected):
                self.detected.add(process)

                node.event_queue.put(
                    Event(
                        "pfd_crash",
                        process=process
                    )
                )

            node.send(process, { "type": self.HEARTBEAT_REQUEST })

        self.alive.clear()


    def handle_request(self, src):
        self.event_handler.node.send(src, { "type": self.HEARTBEAT_REPLY })


    def handle_reply(self, src):
        self.alive.add(src)



class BestEffortBroadcast:
    DATA = "rb_data"

    def __init__(self, event_handler):
        self.event_handler = event_handler


    def broadcast(self, body):
        node = self.event_handler.node

        for process in node.node_ids:
            if process == node.node_id:

                node.event_queue.put(
                    Event(
                        "beb_deliver",
                        sender=node.node_id,
                        body=body
                    )
                )

            else:
                node.send(process, body)


    def handle_pl_deliver(self, src, body):
        self.event_handler.node.event_queue.put(
            Event(
                "beb_deliver",
                sender=src,
                body=body
            )
        )



class ReliableBroadcast:
    def __init__(self, event_handler):
        self.event_handler = event_handler
        self.node = event_handler.node
        self.correct = set()
        self.delivered = set()

        # from[p][rb_msg_id] = payload
        self.from_messages = defaultdict(dict)

        # application-visible messages
        self.app_messages = set()

        self.sequence = 0


    def handle_rb_broadcast(self, event):
        payload = event.data["message"]
        rb_msg_id = ( f"{self.node.node_id}:{self.sequence}" )
        self.sequence += 1

        self.event_handler.beb.broadcast({
            "type": BestEffortBroadcast.DATA,
            "origin": self.node.node_id,
            "rb_msg_id": rb_msg_id,
            "message": payload
        })


    def handle_beb_deliver(self, event):
        body = event.data["body"]
        origin = body["origin"]
        rb_msg_id = body["rb_msg_id"]
        payload = body["message"]

        identity = (origin, rb_msg_id)

        # if m ∉ from[s]
        if identity in self.delivered:
            return

        # from[s] := from[s] U {m}
        self.from_messages[origin][rb_msg_id] = payload
        self.delivered.add(identity)

        # trigger rbDeliver(s,m)
        self.node.event_queue.put(
            event.child(
                "rb_deliver",
                sender=origin,
                rb_msg_id=rb_msg_id,
                message=payload
            )
        )

        # if s ∈ correct
        if origin in self.correct:
            self.event_handler.beb.broadcast({
                "type": BestEffortBroadcast.DATA,
                "origin": origin,
                "rb_msg_id": rb_msg_id,
                "message": payload
            })


    def handle_rb_deliver(self, event):
        sender = event.data["sender"]
        payload = event.data["message"]

        self.app_messages.add(payload)

        self.node.log(
            f"rbDeliver({sender}, {payload})"
        )


    def handle_crash(self, event):

        process = event.data["process"]

        if process not in self.correct:
            return

        self.correct.remove(process)

        self.node.log(
            f"PFD detected crash: {process}"
        )

        for rb_msg_id, payload in (
            self.from_messages[process].items()
        ):

            self.event_handler.beb.broadcast({
                "type": BestEffortBroadcast.DATA,
                "origin": process,
                "rb_msg_id": rb_msg_id,
                "message": payload
            })



class EventHandler:
    def __init__(self, node):
        self.node = node
        self.pfd = PerfectFailureDetector(self)
        self.beb = BestEffortBroadcast(self)
        self.rb = ReliableBroadcast(self)

    def event_loop(self):
        while True:
            event = self.node.event_queue.get()

            try:
                if event.type == "shutdown":
                    return

                elif event.type == "pl_deliver":
                    self.handle_pl_deliver(event)

                elif event.type == "rb_broadcast":
                    self.rb.handle_rb_broadcast(event)

                elif event.type == "beb_deliver":
                    self.rb.handle_beb_deliver(event)

                elif event.type == "rb_deliver":
                    self.rb.handle_rb_deliver(event)

                elif event.type == "pfd_timeout":
                    self.pfd.handle_timeout()

                elif event.type == "pfd_crash":
                    self.rb.handle_crash(event)

            except Exception as e:
                self.node.log(
                    f"event error: {e}"
                )


    def handle_pl_deliver(self, event):
        message = event.data["message"]
        body = message["body"]
        msg_type = body["type"]
        src = message["src"]

        # INIT
        if msg_type == "init":
            self.node.node_id = body["node_id"]
            self.node.node_ids = body["node_ids"]

            self.rb.correct = set(self.node.node_ids)
            self.pfd.init()

            self.node.reply(message, { "type": "init_ok" })

        # maelstrom stuff
        elif msg_type == "topology":
            self.node.reply(message, { "type": "topology_ok" })

        # APP -> RB
        elif msg_type == "broadcast":
            rb_event = event.child(
                "rb_broadcast",
                message=body["message"]
            )

            self.node.event_queue.put(
                rb_event
            )

            self.node.reply(message, {
                "type": "broadcast_ok"
            })

        # APP READ
        elif msg_type == "read":
            self.node.reply(message, {
                "type": "read_ok",
                "messages": sorted(
                    list(
                        self.rb.app_messages
                    )
                )
            })

        # HEARTBEATS
        elif msg_type == (self.pfd.HEARTBEAT_REQUEST):
            self.pfd.handle_request(src)

        elif msg_type == (self.pfd.HEARTBEAT_REPLY):
            self.pfd.handle_reply(src)

        # RB DATA -> BEB
        elif msg_type == (BestEffortBroadcast.DATA):
            self.beb.handle_pl_deliver(src, body)



def main():

    node = Node()

    event_handler = EventHandler(node)

    stdin_thread = threading.Thread(
        target=node.stdin_loop,
        daemon=True
    )

    timer_thread = threading.Thread(
        target=event_handler.pfd.timer_loop,
        daemon=True
    )

    event_thread = threading.Thread(
        target=event_handler.event_loop,
        daemon=True
    )

    stdin_thread.start()
    timer_thread.start()
    event_thread.start()

    stdin_thread.join()

    node.event_queue.put(Event("shutdown"))

    event_thread.join()


if __name__ == "__main__":
    main()
