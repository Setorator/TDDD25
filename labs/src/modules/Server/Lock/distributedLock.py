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
        # Lock peer_list so that no other peer can enter while initializing
        self.peer_list.lock.acquire()

        try:
            self.request[self.owner.id] = 0
            self.token = {self.owner.id: 0}
            peers = self.peer_list.get_peers()
        
            # If there already exist peers other than ourselves
            if peers:
                for peer in peers:
                    print("Peer: {}".format(peer))
                    self.request[peer] = 0
                    self.token[peer] = 0
            
            # We are the first peer to connect, take the token
            else:
                self.state = TOKEN_PRESENT
                print("I got the token!")

        except Exception as e:
            print("Could not initialize properly!")
            print("Exception: {}".format(e))
            # call destroy?
        finally:
            self.peer_list.lock.release()
            

    def destroy(self):
        """ The object is being destroyed.

        If we have the token (TOKEN_PRESENT or TOKEN_HELD), we must
        give it to someone else.

        """
        self.peer_list.lock.acquire()
        self.time += 1
        # release has its own try-catch
        if self.state != NO_TOKEN:
            self.peer_list.lock.release()
            self.release()
            self.peer_list.lock.acquire()

        # No one to claim the token => Just give it to next in line
        if self.state == TOKEN_PRESENT:
            order = self.get_order()
            for peer_id in order:
                try:
                    peer = self.peer_list.peer(peer_id)
                    # Send the token to next in line
                    self.peer_list.lock.release()
                    peer.obtain_token(self._prepare(self.token))
                    self.peer_list.lock.acquire()
                    self.state = NO_TOKEN
                    break
                except Exception as e:
                    # There is no need to remove it since we are about to remove the whole list anyway.
                    print("Could not send token. Peer is down.")
                    self.peer_list.lock.acquire()
                    continue
                        
        self.peer_list.lock.release()
        
        
            

    def register_peer(self, pid):
        """Called when a new peer joins the system."""
        self.peer_list.lock.acquire()
        self.time += 1
        self.request[pid] = 0
        self.token[pid] = 0
        self.peer_list.lock.release()
        

    def unregister_peer(self, pid):
        """Called when a peer leaves the system."""
        self.peer_list.lock.acquire()
        self.time += 1
        del self.request[pid]
        del self.token[pid]
        self.peer_list.lock.release()

    def acquire(self):
        """Called when this object tries to acquire the lock."""
        print("Trying to acquire the lock...")
        self.peer_list.lock.acquire()        
        self.time += 1
        if self.state == NO_TOKEN:
           
            peers = self.peer_list.get_peers()
            for peer_id in peers:
                try:
                    # We need to release the lock in order for the called peer to not lock down
                    # Otherwise the other peer would lock-down but would not be able to access 
                    # the time and id sent from us, thus would not be able to continue to function
                    # while we have not released the lock.
                    self.peer_list.lock.release()
                    self.peer_list.peer(peer_id).request_token(self.time, self.owner.id)
                    
                except Exception as e:
                    # Peer has failed and is down, remove it
                    self.peer_list.lock.acquire()
                    del self.request[peer_id]
                    del self.token[peer_id]
                    self.peer_list.unregister_peer(peer_id)                    
                    self.peer_list.lock.release()
                finally:
                    self.peer_list.lock.acquire()
                    

            # Wait until token is received
            while self.state != TOKEN_PRESENT:
                print("Waiting...")
                self.peer_list.lock.wait() # Suspend peer until notified
                
        # Enter CS
        print("Lock acquired, entering critical section...")
        self.state = TOKEN_HELD
        self.token[self.owner.id] = self.time
        self.peer_list.lock.release()
        

    def release(self):
        """Called when this object releases the lock."""
        print("Releasing the lock...")
        # Lock list since we do not want any modifications to the list while we try to send token
        self.peer_list.lock.acquire()
        self.time += 1

        # If we released the token ourselves then update token times
        if self.state == TOKEN_HELD:
            self.token[self.owner.id] = self.time
            self.state = TOKEN_PRESENT

        if self.state == TOKEN_PRESENT:
            order = self.get_order()
            try:
                for peer_id in order:
                    peer = self.peer_list.peer(peer_id)
                    if self.request[peer_id] > self.token[peer_id]:
                        try:
                            # Try to send the token
                            self.peer_list.lock.release()
                            peer.obtain_token(self._prepare(self.token))
                            self.peer_list.lock.acquire()
                            self.state = NO_TOKEN
                            print("Token sent to: " + str(peer_id))
                            break
                        except Exception:
                            # If we could not send the token, remove it and continue to the next peer
                            print("Could not send token to: {}".format(peer_id))
                            self.state = TOKEN_PRESENT
                            del self.request[peer_id]
                            del self.token[peer_id]
                            self.peer_list.unregister_peer(peer_id)
                            continue

                # If no one has claimed the token
                if self.state == TOKEN_PRESENT:
                    print("No one claimed the token.")
            except Exception as e:
                # Unexpected error
                print("Exception: {}".format(e))
            finally:
                self.peer_list.lock.release()
        

    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""
        self.peer_list.lock.acquire()
        self.time = max(self.time+1, time+1)
        self.request[pid] = max(self.request[pid], self.time)
        self.peer_list.lock.release() # self.release() acquire lock on its own
        if self.state == TOKEN_PRESENT:
            self.release()           
        

    def obtain_token(self, token):
        """Called when some other object is giving us the token."""
        print("Receiving the token...")
        self.peer_list.lock.acquire()
        self.time += 1
        try:
            msg = self._unprepare(token)
            for peer_id in msg:
                self.token[peer_id] = msg[peer_id]
                self.time = max(self.time, msg[peer_id]+1)
        except KeyError as k:
            # The sender might have crashed after sending the token
            print("No such index: {}.".format(k))
        except Exception as e:
            print("Unknown error occurred: {}.".format(e))
        finally:
            print("Token obtained!")
            self.state = TOKEN_PRESENT
            self.peer_list.lock.notify_all()
            self.peer_list.lock.release()

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
