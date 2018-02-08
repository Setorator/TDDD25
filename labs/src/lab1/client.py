#!/usr/bin/env python3

# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 16 March 2017
#
# Copyright 2012-2017 Linkoping University
# -----------------------------------------------------------------------------

"""Client reader/writer for a fortune database."""

import sys
import socket
import json
import argparse

# -----------------------------------------------------------------------------
# Initialize and read the command line arguments
# -----------------------------------------------------------------------------


def address(path):
    addr = path.split(":")
    if len(addr) == 2 and addr[1].isdigit():
        return((addr[0], int(addr[1])))
    else:
        msg = "{} is not a correct server address.".format(path)
        raise argparse.ArgumentTypeError(msg)

description = """\
Client for a fortune database. It reads a random fortune from the database.\
"""
parser = argparse.ArgumentParser(description=description)
parser.add_argument(
    "-w", "--write", metavar="FORTUNE", dest="fortune",
    help="Write a new fortune to the database."
)
parser.add_argument(
    "-i", "--interactive", action="store_true", dest="interactive",
    default=False, help="Interactive session with the fortune database."
)
parser.add_argument(
    "address", type=address, nargs=1, metavar="addr:port",
    help="Server address."
)
opts = parser.parse_args()
server_address = opts.address[0]

# -----------------------------------------------------------------------------
# Auxiliary classes
# -----------------------------------------------------------------------------


class CommunicationError(Exception):
    
    """ Class used for modeling errors occured in the communication between server and client. """

    def __init__(self, type, args):
        self.type = type
        self.args = args

    def __str__(self):
        return self.type    


class DatabaseProxy(object):

    """Class that simulates the behavior of the database class."""

    def __init__(self, server_address):
        self.address = server_address

    # Public methods
    
    def remote_method_invokation(self, method, args=[]):
        message = json.dumps(
            {
                "method": method,
                "args": args
            })
        message += "\n"
        
        # Open socket and treat as a file stream
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.address)
        worker = s.makefile(mode="rw")

        # Send message to server
        worker.write(message)
        worker.flush()
        
        # Receive the respons from the server and close the connection
        response = worker.readline()
        s.close()

        return json.loads(response)


    def handle_server_response(self, response):
        if ("result" in response):
            return response["result"]
        elif ("error" in response):
            # Get the type of the error and state it to be a subclass of "Exception"
            ex = type(response["error"]["name"], (Exception, ), {})
            # Raise said exception
            raise ex(response["error"]["args"])
        else:
            # If something undefined happened
            raise CommunicationError("CommunicationError", ["Something went wrong with the communication"])
            

    def read(self):
        response = self.remote_method_invokation("read")
        return self.handle_server_response(response)
        
    def write(self, fortune):
        response = self.remote_method_invokation("write", [fortune])
        return self.handle_server_response(response)

# -----------------------------------------------------------------------------
# The main program
# -----------------------------------------------------------------------------

# Create the database object.
db = DatabaseProxy(server_address)

if not opts.interactive:
    # Run in the normal mode.
    if opts.fortune is not None:
        db.write(opts.fortune)
    else:
        print(db.read())

else:
    # Run in the interactive mode.
    def menu():
        print("""\
Choose one of the following commands:
    r            ::  read a random fortune from the database,
    w <FORTUNE>  ::  write a new fortune into the database,
    h            ::  print this menu,
    q            ::  exit.\
""")

    command = ""
    menu()
    while command != "q":
        sys.stdout.write("Command> ")
        command = input()
        if command == "r":
            print(db.read())
        elif (len(command) > 1 and command[0] == "w" and
                command[1] in [" ", "\t"]):
            db.write(command[2:].strip())
        elif command == "h":
            menu()
