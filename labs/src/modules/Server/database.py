# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 24 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Implementation of a simple database class."""

import random


class Database(object):

    """Class containing a database implementation."""

    def __init__(self, db_file):
        self.db_file = db_file
        self.rand = random.Random()
        self.rand.seed()

        self.fortuneList = []
        self.__read_from_database()
        pass

    def read(self):
        """Read a random location in the database."""
        return self.fortuneList[self.rand.randint(0, len(self.fortuneList))]
        pass

    def write(self, fortune):
        """Write a new fortune to the database."""
        self.fortuneList.append(fortune)
        self.__update_database(fortune)
        pass

    def __read_from_database(self):
        self.f = open(self.db_file, "r")
        tmp = ""
        for line in self.f:
            if line == "%\n":
                self.fortuneList.append(tmp)
                tmp = ""
                continue
            else:
                tmp += line
        self.f.close()

    def __update_database(self, fortune):
        self.f = open(self.db_file, "a")
        self.f.write(fortune + "\n")
        self.f.write("%\n")
        self.f.close()
        
        

