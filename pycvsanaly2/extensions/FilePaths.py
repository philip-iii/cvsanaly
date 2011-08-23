# Copyright (C) 2008 LibreSoft
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors :
#       Carlos Garcia Campos <carlosgc@gsyc.escet.urjc.es>
#        Zhongpeng Lin  <zlin5@ucsc.edu>

if __name__ == '__main__':
    import sys
    sys.path.insert(0, "../../")

from pycvsanaly2.Database import statement
from pycvsanaly2.utils import printdbg
from pycvsanaly2.profile import profiler_start, profiler_stop
from pycvsanaly2.Config import Config
from copy import deepcopy
import shelve
import os
from time import time


config = Config()


class Adj(object):
    def __init__(self):
        self.files = {}
        self.adj = {}


class FilePaths(object):
    __shared_state = {'rev': None,
                      'adj': None,
                      'files': None,
                      'cached_adj': {},
                      'db': None}

    def __init__(self, db):
        self.__dict__ = self.__shared_state
        self.__dict__['db'] = db
  
    def update_for_revision(self, cursor, commit_id, repo_id):
        db = self.__dict__['db']

        if commit_id == self.__dict__['rev']:
            return
        prev_commit_id = self.__dict__['rev']
        self.__dict__['rev'] = commit_id

        profiler_start("Updating adjacency matrix for commit %d", (commit_id,))
        if self.__dict__['adj'] is None:
            adj = Adj()
            self.__dict__['adj'] = adj
        else:
            adj = self.__dict__['adj']

        rf = self.__dict__['files']
        if rf is not None:
            repo_files_id, repo_files = rf
            if repo_files_id != repo_id:
                del self.__dict__['files']
                repo_files = {}
        else:
            repo_files = {}

        if not repo_files:
            # Get and cache all the files table
            query = "select id, file_name from files where repository_id = ?"
            # profiler_start("Getting files for repository %d", (repo_id,))
            cursor.execute(statement(query, db.place_holder), (repo_id,))
            # profiler_stop("Getting files for repository %d", (repo_id,), 
            # True)
            rs = cursor.fetchmany()
            while rs:
                for id, file_name in rs:
                    repo_files[id] = file_name
                rs = cursor.fetchmany()
            self.__dict__['files'] = (repo_id, repo_files)
            adj.files = repo_files

        # Get the files that have been renamed
        # with the new name for the given rev
        query = "select af.file_id, af.new_file_name " + \
                "from actions_file_names af, files f " + \
                "where af.file_id = f.id " + \
                "and af.commit_id = ? " + \
                "and af.type = 'V' " + \
                "and f.repository_id = ?"
        # profiler_start("Getting new file names for commit %d", (commit_id,))
        cursor.execute(statement(query, db.place_holder), (commit_id, repo_id))
        # profiler_stop("Getting new file names for commit %d", (commit_id,), 
        # True)
        rs = cursor.fetchmany()
        while rs:
            for id, file_name in rs:
                adj.files[id] = file_name
            rs = cursor.fetchmany()

        # Get the new file links since the last time
        query = "select fl.parent_id, fl.file_id " + \
                "from file_links fl, files f " + \
                "where fl.file_id = f.id "
        if prev_commit_id is None:
            query += "and fl.commit_id = ? "
            args = (commit_id, repo_id)
        else:
            query += "and fl.commit_id between ? and ? "
            args = (prev_commit_id, commit_id, repo_id)
        query += "and f.repository_id = ?"
#        profiler_start("Getting file links for commit %d", (commit_id,))
        cursor.execute(statement(query, db.place_holder), args)
#        profiler_stop("Getting file links for commit %d", (commit_id,), True)
        rs = cursor.fetchmany()
        while rs:
            for f1, f2 in rs:
                adj.adj[f2] = f1
            rs = cursor.fetchmany()

        profiler_stop("Updating adjacency matrix for commit %d",
                       (commit_id,), True)

    def __build_path(self, file_id, adj):
        if file_id not in adj.adj:
            return None

        profiler_start("Building path for file %d", (file_id,))
        
        tokens = []
        id = file_id
        
        while id is not None and id != -1:
            tokens.insert(0, adj.files[id])
            #use get instead of index to avoid key error
            id = adj.adj.get(id) 

        profiler_stop("Building path for file %d", (file_id,), True)

        return "/" + "/".join(tokens)

    def get_path_from_database(self, file_id, commit_id):
        """Returns the last valid path for a given file_id at commit_id
           (May have been removed afterwords!)"""
        
        if config.debug:
            profiler_start("Getting full file path for file_id %d and \
                            commit_id %d", (file_id, commit_id))
        
        db = self.__dict__['db']
        cnn = db.connect()
        
        cursor = cnn.cursor()
        query = """SELECT current_file_path from actions
                   WHERE file_id=? AND commit_id <= ?
                   ORDER BY commit_id DESC LIMIT 1"""
        cursor.execute(statement(query, db.place_holder), (file_id, commit_id))
        try:
            file_path = cursor.fetchone()[0]
        except:
            file_path = None
        
        cursor.close()
        cnn.close()
        
        printdbg("get_path_from_database:\
                  Path for file_id %d at commit_id %d: %s",
                 (file_id, commit_id, file_path))
        if config.debug:
            profiler_stop("Getting full file path for file_id %d and\
                             commit_id %d", (file_id, commit_id), True)
        return file_path

    def get_path(self, file_id, commit_id, repo_id):
        """
        Unless update_all is called up-front, commit_id 
        passed into this method should be sequentially
        from first commit to the last, though the same
        commit_id can be passed several times.
        """
        adj = self.__dict__['cached_adj'].get(str(commit_id))
        if adj is not None:
            self.__dict__['adj'] = adj
            self.__dict__['rev'] = commit_id
        else:
            cnn = self.__dict__['db'].connect()
            cursor = cnn.cursor()
            self.update_for_revision(cursor, commit_id, repo_id)
            cursor.close()
            cnn.commit()
            cnn.close()
            adj = self.__dict__['adj']
            self.__dict__['cached_adj'][str(commit_id)] = deepcopy(adj)
        path = self.__build_path(file_id, adj)

        return path

    def get_filename(self, file_id):
        adj = self.__dict__['adj']
        assert adj is not None, "Matrix no updated"
        try:
            return adj.files[file_id]
        except KeyError:
            return None

    def get_file_id(self, file_path, commit_id):
        """Ask for the file_id for a given file_path and commit_id"""
        
        if config.debug:
            profiler_start("Getting file id for file_path %s and commit_id %d",
                            (file_path, commit_id))
        
        db = self.__dict__['db']
        cnn = db.connect()
        cursor = cnn.cursor()
        query = """SELECT file_id from actions
                   WHERE current_file_path = ? AND commit_id <= ?
                   ORDER BY commit_id DESC LIMIT 1"""
        cursor.execute(statement(query, db.place_holder),
                        (file_path, commit_id))
        try:
            file_id = cursor.fetchone()[0]
        except:
            file_id = None
        
        cursor.close()
        cnn.close()
        
        if config.debug:
            profiler_stop("Getting file id for file_path %s and commit_id %d",
                           (file_path, commit_id), True)
        
        return file_id

    def get_commit_id(self):
        return self.__dict__['rev']

    def update_all(self, repo_id):
        """
        update_all enable cache for adjacency matrices
        Pros: File paths in different revisions can be
        accessed randomly, i.e. after calling update_all,
        get_path can be called with any revision in any
        order.
        Cons: It consumes significant memory to store
        the adjacency matrices

        If the config has low_memory set to true, shelve will
        be used instead, to write the cache out to disk.
        """
        profiler_start("Update all file paths")
        
        if Config().low_memory:
            self.shelve_file_name = str(time()) + "-shelve.db"
            
            # If there is an old file, shelf will complain viciously
            if os.path.exists(self.shelve_file_name):
                os.remove(self.shelve_file_name)
            
            self.__dict__['cached_adj'] = shelve.open(self.shelve_file_name, 
                                                        writeback=False)
        
        db = self.__dict__['db']
        cnn = db.connect()

        cursor = cnn.cursor()
        query = """select distinct(s.id) from scmlog s, actions a
                    where s.id = a.commit_id and repository_id=?
                    order by s.commit_date"""
        cursor.execute(statement(query, db.place_holder), (repo_id,))
        
        old_id = -1
        all_commits = [i[0] for i in cursor.fetchall()]
        for id in all_commits:
            if old_id != id:
                adj = self.__dict__['cached_adj'].get(str(id))

                if adj is None:
                    self.update_for_revision(cursor, id, repo_id)
                    self.__dict__['cached_adj'][str(id)] = \
                    deepcopy(self.__dict__['adj'])
                old_id = id
        cursor.close()
        cnn.close()
        profiler_stop("Update all file paths", delete=True)
        
    def close(self):
        """Closes FilePaths to ensure all caches are deleted"""
        
        if Config().low_memory:
            # FIXME: This should be closed, but sometimes shelve
            # just won't do it. The best way is to timeout the try,
            # but not closing and just deleting will do the same
            # think, just in a more yucky way
            printdbg("Syncing shelf")
            self.__dict__['cached_adj'].sync()
            printdbg("Closing shelf")
            self.__dict__['cached_adj'].close()
            printdbg("Deleting shelve " + self.shelve_file_name)
            os.remove(self.shelve_file_name)
            # Clean up cached adj in case this gets called without
            # update_all later
            self.__dict__['cached_adj'] = {}

if __name__ == '__main__':
    import sys
    from pycvsanaly2.Database import create_database
    from pycvsanaly2.Config import Config

    db = create_database('sqlite', sys.argv[1])
    cnn = db.connect()

    fp = FilePaths(db)

    config = Config()
    config.profile = True

    cursor = cnn.cursor()
    cursor.execute("select s.id, file_id from scmlog s, actions a " + \
                   "where s.id = a.commit_id")
    old_id = -1
    for id, file_id in cursor.fetchall():
        if old_id != id:
            print "Commit ", id
            fp.update_for_revision(cursor, id, 1)
            old_id = id
        print fp.get_path(file_id, id, 1)

    cursor.close()
    
    cnn.close()
