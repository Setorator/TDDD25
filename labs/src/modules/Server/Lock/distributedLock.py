# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Module for the distributed mutual exclusion implementation.

This implementation is based on the second Ricart-Agrawala algorithm.
The implementation should satisfy the following requests:
    --  when starting, the peer with the smallest id in the peer list
        should get the token.
    --  access to the state of each peer (dictionaries: request, token,
        and peer_list) should be protected.
    --  the implementation should graciously handle situations when a
        peer dies unexpectedly. All exceptions coming from calling
        peers that have died, should be handled such as the rest of the
        peers in the system are still working. Whenever a peer has been
        detected as dead, the token, request, and peer_list
        dictionaries should be updated accordingly.
    --  when the peer that has the token (either TOKEN_PRESENT or
        TOKEN_HELD) quits, it should pass the token to some other peer.
    --  For simplicity, we shall not handle the case when the peer
        holding the token dies unexpectedly.

"""

NO_TOKEN = 0
TOKEN_PRESENT = 1
TOKEN_HELD = 2


class DistributedLock(object):

    """Implementation of distributed mutual exclusion for a list of peers.

    Public methods:
        --  __init__(owner, peer_list)
        --  initialize()
        --  destroy()
        --  register_peer(pid)
        --  unregister_peer(pid)
        --  acquire()
        --  release()
        --  request_token(time, pid)
        --  obtain_token(token)
        --  display_status()

    """

    def __init__(self, owner, peer_list):
        self.peer_list = peer_list
        self.owner = owner
        self.time = 0
        self.token = None
        self.request = {}
        self.state = NO_TOKEN

    def _prepare(self, token):
        """Prepare the token to be sent as a JSON message.

        This step is necessary because in the JSON standard, the key to
        a dictionary must be a string whild in the token the key is
        integer.
        """
        return list(token.items())

    def _unprepare(self, token):
        """The reverse operation to the one above."""
        return dict(token)

    def get_order(self):
        higher = []
        lower = []
        for peer_id in self.peer_list.get_peers():
            if peer_id < self.owner.id:
                lower.append(peer_id)
            elif peer_id > self.owner.id:
                higher.append(peer_id)
        # Return IDs with higher first
        return higher + lower

    # Public methods

    def initialize(self):
        """ Initialize the state, request, and token dicts of the lock.

        Since the state of the distributed lock is linked with the
        number of peers among which the lock is distributed, we can
        utilize the lock of peer_list to protect the state of the
        distributed lock (strongly suggested).

        NOTE: peer_list must already be populated when this
        function is called.

        """
        peers = self.peer_list.get_peers()
        
        # If there already exist peers other than ourselves
        if peers:
            for peer in peers:
                self.request[peer] = 0
            
        # We are the first peer to connect, take the token
        else:
            self.state = TOKEN_HELD
            print("I got the token!")
    

        self.request[self.owner.id] = 0
        self.token = 0
        self.time += 1
        

    def destroy(self):
        """ The object is being destroyed.

        If we have the token (TOKEN_PRESENT or TOKEN_HELD), we must
        give it to someone else.

        """
        #
        # Your code here.
        #
        pass

    def register_peer(self, pid):
        """Called when a new peer joins the system."""
        self.time += 1
        self.request[pid] = 0
        

    def unregister_peer(self, pid):
        """Called when a peer leaves the system."""
        self.time += 1
        del self.request[pid]

    def acquire(self):
        """Called when this object tries to acquire the lock."""
        print("Trying to acquire the lock...")
        self.time += 1
        if self.state == NO_TOKEN:
            peers = self.peer_list.get_peers()
            for peer_id in peers:
                token = self.peer_list.peer(peer_id).__getattr__("request_token")(self.time, self.owner.id)
            # Wait until token is received
            while self.state != TOKEN_PRESENT:
                continue
                
        # Enter CS
        self.state = TOKEN_HELD
        

    def release(self):
        """Called when this object releases the lock."""
        print("Releasing the lock...")
        self.state = TOKEN_PRESENT
        order = self.get_order()
        # Call obtain_token() on the process which we gives the token to
        

    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""
        self.time += 1
        self.request[pid] = max(self.request[pid], time)
        if self.state == TOKEN_PRESENT:
            release()           
        

    def obtain_token(self, token):
        """Called when some other object is giving us the token."""
        print("Receiving the token...")
        #
        # Your code here.
        #
        pass

    def display_status(self):
        """Print the status of this peer."""
        self.peer_list.lock.acquire()
        try:
            nt = self.state == NO_TOKEN
            tp = self.state == TOKEN_PRESENT
            th = self.state == TOKEN_HELD
            print("State   :: no token      : {0}".format(nt))
            print("           token present : {0}".format(tp))
            print("           token held    : {0}".format(th))
            print("Request :: {0}".format(self.request))
            print("Token   :: {0}".format(self.token))
            print("Time    :: {0}".format(self.time))
        finally:
            self.peer_list.lock.release()
