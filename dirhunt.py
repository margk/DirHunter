# -*- coding: utf-8 -*-
"""
Created on 2016-11-11

@author: meister

Python3 version
"""

import logging
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


import traceback
import os.path
import math
import sys
import time
import multiprocessing
import datetime

try:
    import readline
except ImportError:
    logging.warning('Proceeding without readline functionality. Error during importing this module:\n{}'.format(traceback.format_exc()))

import cmd



#===========================================================================



class Sizer(object):
    """
    Performs the size analysis for a specified directory and displays the results.
    """
    def __init__(self, directory=None):
        """
        Initialisation. If a directory is specified, its analysis is triggered.

        @param directory - [optional] string, path of directory to analyse
        """
        self._unit_scale = 1000.0   # scaling between unit prefixes
        self._units = 'kMGT'     # list of unit prefixes

        self.base_dir = None    # init attribute for base-dir path
        self.base_dir_info = None   # init attribute for base-dir-info object
        self._dir_list = []
        self._info_chain = []    # init attribute for list of current info object and its parent info objects
        self._dir_chain = []    # init attribute for list of current dir name and its parent dir names
        self._last_info = None     # init internal cache for insertion of dir infos into info tree
        self._last_path = ''       # init internal cache for insertion of dir infos into info tree
        self._last_counter = [0, 0]    # just for debugging/info: init counters for insertion cache misses & hits

        self._info_stock = None     # init attribute for dir-info object to integrate/re-use in analysis
        self._dir_stock = ''        # init attribute for path of dir info to integrate/re-use

        if directory is not None:
            # dir specified  ->  change to it
            self.cd(directory)

    def cd(self, directory=None):
        """
        Changes to the specified directory and analyses it.
        If the specified dir is a subdir of the current dir, no file-system access
        is necessary and things will be fast(er).

        @param directory - string, path of the dir to analyse
        """
        if directory is None:
            # no dir specified...
            if self.base_dir is None:
                # no base dir set  ->  do nothing
                return
            else:
                # base dir set  ->  change to base dir
                self.cdi()

        # check if the specified dir is a subdir of the base dir or vice versa
        if self.base_dir is None:
            # no base dir set  ->  no common path part possible
            common_prefix = ''
        else:
            # base dir set  ->  check for common path part
            if directory[0] != os.sep:
                # specified directory starts not with slash  ->  relative path, convert to absolute path
                directory = os.path.abspath(os.path.join(self._get_current_dir(), directory))

            # check for poss. common path part
            common_prefix = os.path.commonpath((self._get_current_dir(), directory))


        # change to the specified dir, depending on the relation between the specified dir and
        # the current dir or base dir, respectively
        if not ((common_prefix == self._get_current_dir()) or (common_prefix == directory)):
            # no common path part  ->  new dir, complete analysis required
            logging.debug('no common prefix')

            self._set_base_dir(directory)   # set specified dir as new base dir
            self._analyse_base_dir()    # run analysis
            self.cdi(_quiet=True)   # init internal dir-change system

        else:
            # common path part  ->  use current analysis results
            logging.debug('common prefix: ' + common_prefix)

            if directory == self.base_dir:
                # specified dir is base dir  ->  no new analysis required, just change into base dir
                self.cdi(_quiet=True)

            elif len(directory) > len(self.base_dir):
                # specified dir is subdir of base dir  ->  no new analysis required, just change into subdir
                if len(directory) <= len(self._get_current_dir()):
                    # current dir (not base dir) is subdir of specified dir
                    # -> change back into base dir at first
                    self.cdi(_quiet=True)
                    common_prefix = os.path.commonprefix((self.base_dir, directory))     # update common path part

                # split the incommon part of the specified dir into single dir names
                # and use index-based change-dir method ("cdi") to reach the target dir
                for subdir in directory[len(common_prefix):].split(os.sep):
                    if subdir:  # ignore empty parts (leading & trailing slash)
                        self.cdi(self._current_subdirs.index(subdir), _quiet=True)

            else:
                # base dir is subdir of specified dir
                # -> trigger new analysis, but re-use analysis of current base dir
                self._dir_stock = self.base_dir     # store current base-dir path for info re-usage
                self._info_stock = self.base_dir_info   # store current base-dir info for re-usage

                self._set_base_dir(directory)   # set specified dir as new base dir
                self._analyse_base_dir()    # run analysis
                self.cdi(_quiet=True)   # init internal dir-change system


        # finally display analysis results of new dir
        self.ls()

    def cdi(self, index=None, _quiet=False):
        """
        Changes to one of the subdirectories of the current directory (i.e. makes this
        one the current directory).
        Optionally, changes to a parent directory (negative indices).
        Optionally, changes to the base directory (index is None / not specified).

        @param index - [optional] int, index of the subdir to change to (indices are
            displayed by 'ls' method); negative indices change to parent directories
            (-1 changes to parent, -2 to parent of parent, and so on); if not specified
            at all changes to base directory
        """
        if index is None:
            # no index  ->  change to base dir
            self._info_chain = []
            self._dir_chain = []

        elif index < 0:
            # negative index  ->  change to parent dir of current dir
            while index < 0:
                self._info_chain.pop()
                self._dir_chain.pop()
                index += 1
        else:
            # non-negative index  ->  change to subdir of current dir
            try:
                dir_info = self._info_chain[-1]
            except IndexError:
                dir_info = self.base_dir_info

            dir_name = self._current_subdirs[index]
            self._info_chain.append(dir_info['dirs'][dir_name])
            self._dir_chain.append(dir_name)

        # epilogue: update list of current subdir names
        try:
            dir_info = self._info_chain[-1]
        except IndexError:
            dir_info = self.base_dir_info

        subdirs = sorted(dir_info['dirs'].items(), key=lambda info : info[1]['size'], reverse=True)
        self._current_subdirs = [d[0] for d in subdirs]

        # epilogue: call list method
        if not _quiet:
            self.ls()

    def ls(self):
        """
        Displays information about the current directory:
        Lists the subdirectories and their sizes.
        """
        # determine current dir-info object
        try:
            # try to use the last info object from the path stack
            dir_info = self._info_chain[-1]
        except IndexError:
            # path stack is empty  ->  use base info object
            dir_info = self.base_dir_info

        # assemble the path string for the current directory
        dir_path = self.base_dir
        last_info = self.base_dir_info
        for info in self._info_chain:
            dir_name = None
            for lobster, fish in last_info['dirs'].items():
                if fish is info:
                    dir_name = lobster
                    break
            dir_path = os.path.join(dir_path, dir_name)
            last_info = info

        # print the path string and the current directory's size
        print('{}: {}\n'.format(dir_path, self._format_size(dir_info['size'], unit_indent=False)))

        # assemble the subdirectories info: collect dir names, sizes and incompleteness, sort by size
        subdirs = sorted(dir_info['dirs'].items(), key=lambda info : info[1]['size'], reverse=True)
        if not subdirs:
            print('  no subdirectories')
        else:
            subdirs = [(d[0], self._format_size(d[1]['size']), d[1]['size'], self._check_incompleteness(d[1])) for d in subdirs]

            # assemble formating pattern for displaying the subdirectories info
            size_max_width = max([len(d[1]) for d in subdirs])
            size_steps = 11
            fish = '{{:>{:.0f}}} '.format(size_max_width+3)  # formating pattern for size value
            fish += '{} '      # formating pattern for poss. incomplete flag
            fish += '{{:<{:.0f}}}'.format(size_steps+2)   # formating pattern for size bar
            fish += '{:>5} '   # formating pattern for size rank
            fish += '{}'      # formating pattern for dir name

            # display the subdirectories info
            # max_size = max([d[2] for d in subdirs])
            max_size = dir_info['size']
            for i, d in enumerate(subdirs):
                if d[3]:
                    incomplete_flag = '?'
                else:
                    incomplete_flag = ' '
                crab = '[{{:-<{:.0f}}}]'.format(size_steps)
                if d[2] > 0:
                    size_bar = crab.format('#' * (1 + int(d[2] / max_size * (size_steps - 1))))
                else:
                    size_bar = crab.format('')
                print(fish.format(d[1], incomplete_flag, size_bar, '[{:.0f}]'.format(i), d[0]))

        print('\n[Unit scale: 1{}B = {:.0f}B]'.format(self._units[0], self._unit_scale))

    def _get_current_dir(self, full_path=True):
        """
        Returns the current directory of the sizer, i.e. the dir whose size information
        would be listed via the "ls" method.

        @param full_path - [optional] bool, flag to control whether full path is returned
            True -> return fullpath (absolute path)
            False -> return last dir name only (last subdir)
        @retval dir - string, current directory or None if no dir is set
        """
        if self.base_dir is None:
            # no base dir set  ->  return None
            return None
        else:
            if full_path:
                # full path requested
                # -> join the base dir and the dir-chain elements (chain is filled by "cdi" method)
                return os.path.join(self.base_dir, *self._dir_chain)
            else:
                # only last dir name (name of last subdir) requested
                if self._dir_chain:
                    # dir chain is filled  ->  return last element
                    return self._dir_chain[-1]
                else:
                    # dir chain is empty (i.e. currently in base dir)
                    # -> try to extract last dir component of base dir
                    dir_list = [dir for dir in os.path.split(self.base_dir) if dir]
                    return str(dir_list[-1])

    def _set_base_dir(self, directory):
        """
        Sets the base directory and prepares the size analysis.

        @param directory - string, path of the dir
        """
        self.base_dir = os.path.abspath(directory)   # store full dir path
        self.base_dir_info = None
        self._dir_list = [self.base_dir,]    # init list of dir paths (used during analysis)
        self._last_info = None     # clear insertion cache (used during analysis)
        self._last_path = ''       # clear insertion cache (used during analysis)

    def _analyse_base_dir(self):
        """
        Starts the analysis of the currently set base dir, iterates over all its subdirs.
        """
        time_start = datetime.datetime.now()    # just for performance info: note start time

        # handle re-using of an existing info object
        if self._dir_stock:
            self._iterate_dir_list()    # perform a single analysis iteration to initialise the info tree
            self._insert_info(self._info_stock, self._dir_stock)    # insert the existing info object into the info tree

        # iteratively analyse until the list of dirs to analyse is empty
        while self._dir_list:
            self._iterate_dir_list()

        self._sum_sizes()   # finally calculate the dir sizes

        # delete any existing, re-used info object
        self._dir_stock = ''
        self._info_stock = None

        time_end = datetime.datetime.now()  # just for performance info: note end time

        print('===== elapsed time: ', str(time_end - time_start))
        print('===== insertion cache: {} hits, {} misses'.format(self._last_counter[1], self._last_counter[0]))
        self._last_counter = [0, 0]    # just for debugging/info: init counters for insertion cache misses & hits

    def _iterate_dir_list(self):
        """
        Triggers the size analysis for the first element of the internal dir list.
        """
        dir_path = self._dir_list.pop(0)    # fetch the first entry of the dir list

        if self._dir_stock == dir_path:
            # if an existing info object is re-used, its path is stored in "_dir_stock"
            # -> skip this path during the current analysis
            logging.debug('Re-using existing info for dir: {}'.format(dir_path))
            self._dir_stock = ''
            return

        dir_info, subdir_list = self._analyse_dir(dir_path)     # analyse the dir
        self._insert_info(dir_info, dir_path)       # insert the dir-info object into the info tree
        self._dir_list = subdir_list + self._dir_list   # prepend any found subdirs to the dir list

    def _analyse_dir(self, dir_path):
        """
        Performs the actual analysis for the specified dir.
        (Analysis means: sum size of files in dir, determine names of subdirs)

        @param dir_path - string, path of the dir to analyse
        @retval dir_info, subdir_list - created dir-info object (dict) and list of found subdirs
        """
        # init return values
        dir_info = self._create_info()      # create new dir-info object
        subdir_list = []    # create empty list for subdirs

        # process the directory's entries (files / subdirs)
        try:
            for dir_entry in os.scandir(dir_path):
                try:
                    if dir_entry.is_file(follow_symlinks=False):
                        # current entry is a file  ->  add its size to dir size
                        stat = dir_entry.stat(follow_symlinks=False)
                        dir_info['files_size'] += float(stat.st_size)
                        # print('\t{}: {}'.format(dir_entry.path, float(stat.st_size)))

                    elif dir_entry.is_dir(follow_symlinks=False):
                        # current entry is a subdir  ->  create a subdir-queue entry with its name & path
                        subdir_list.append(dir_entry.path)

                except OSError:
                    # entry could not be accessed
                    logging.info('Access denied to {}'.format(dir_entry.path))
                    dir_info['incomplete'] = True

        except FileNotFoundError:
            raise

        except OSError:
            # directory's content could not be accessed
            logging.info('Access denied to {}'.format(dir_path))
            dir_info['incomplete'] = True

        return dir_info, subdir_list

    def _create_info(self):
        """
        Creates a new dir-info object which can hold the size of a dir's files and a
        list of its subdirs' info objects.

        @retval dir_info - dict, initialised dir-info object
            key 'size' - float, sum of subdirs & files sizes
            key 'files_size' - float, sum of files sizes
            key 'incomplete' - bool, flag to indicate incomplete size analysis (due to denied access)
            key 'dirs' - dict, container for subdirs' info objects (keys are dir names, values are dir infos)
        """
        dir_info = {'size': 0.0, 'files_size': 0.0, 'incomplete': False, 'dirs': {}}

        return dir_info

    def _insert_info(self, dir_info, dir_path):
        """
        Inserts the specified dir-info object into the internal dir-info tree according
        to the specified dir path.

        @param dir_info - dir-info object (dict), dir info to insert
        @param dir_path - string, path of the dir to which the dir info belongs
        """
        if dir_path == self.base_dir:
            # specified dir path is the base dir  ->  store dir info as base info (i.e. tree root)
            if self.base_dir_info is None:
                self.base_dir_info = dir_info
            else:
                self._merge_info(self.base_dir_info, dir_info)

        else:
            if self.base_dir_info is None:
                self.base_dir_info = self._create_info()

            # insert the dir info into the internal info tree
            try:
                # try using the internal cache, which holds the last inserted dir info
                # (works if the analysis (caller) is working down a dir branch)
                # reason is to speed things up
                if not self._last_path or (len(dir_path) < len(self._last_path)):
                    # last path is not yet set or current path too short to be a potential subdir
                    # -> cache cannot be used
                    raise ValueError

                if dir_path[:len(self._last_path)] != self._last_path:
                    # specified dir path is not a subdir of last path
                    # -> cache cannot be used
                    raise ValueError

                # check if the specified dir is exactly a subdir of the last dir (and not e.g. a sub-subdir)
                path_remainder = dir_path[len(self._last_path):].split(os.sep)     # split remainder of specified dir
                path_remainder = [p for p in path_remainder if p]   # remove split "artifacts" (from initial or trailing slashes)
                if len(path_remainder) != 1:
                    # TODO: remove this condition? should work without
                    # specified dir is a deeper-level subdir of last path
                    # -> cache cannot be used
                    raise ValueError

                # cache check was positive  ->  use the cache
                dir_list = path_remainder   # use subdir remainder as final dir list
                parent_dir_info = self._last_info  # use cache's info object as parent object for insertion

                self._last_counter[1] += 1     # just used for debugging info: count the cache "hits"

            except ValueError:
                # cache "miss"  ->  set insertion start at root of dir-info tree
                dir_list = dir_path[len(self.base_dir):].split(os.sep)   # split path part after base-dir part into dir names
                parent_dir_info = self.base_dir_info     # use base-dir info as starting point for insertion

                self._last_counter[0] += 1     # just used for debugging info: count the cache "misses"


            # locate the parent info object for the insertion of the specified dir info into the info tree,
            # loop over list of subdir names leading to target dir
            for dir_name in dir_list:
                if not dir_name:    # skip path-splitting "artifacts" (from initial or trailing slashes)
                    continue

                # expand the tree branch if necessary (i.e. insert any missing info objects between
                # the current tree-branch end and the dir info to insert)
                if dir_name not in parent_dir_info['dirs']:
                    parent_dir_info['dirs'][dir_name] = self._create_info()

                # jump to the current subdir's info object
                parent_dir_info = parent_dir_info['dirs'][dir_name]

            # the actual insertion of the specified dir info into the info tree
            self._merge_info(parent_dir_info, dir_info)

            # update cache for poss. next insertion
            self._last_path = dir_path
            self._last_info = parent_dir_info

    def _merge_info(self, dir_info_main, dir_info_add):
        """
        Merges the dir_info_add object into the dir_info_main object.

        @param dir_info_main - dir-info object (dict), dir info to update (merge into)
        @param dir_info_add - dir-info object (dict), dir info to add to the main info
        """
        dir_info_main['files_size'] += dir_info_add['files_size']
        dir_info_main['incomplete'] |= dir_info_add['incomplete']

        # merge the subdirs, i.e. the "dirs" part of the info objects
        for subdir in dir_info_add['dirs']:
            if subdir in dir_info_main['dirs']:
                # recursively merge if current subdir is present in both info objects
                self._merge_info(dir_info_main['dirs'][subdir], dir_info_add['dirs'][subdir])
            else:
                # new dir  ->  just copy over
                dir_info_main['dirs'][subdir] = dir_info_add['dirs'][subdir]


    def _sum_sizes(self, dir_info=None):
        """
        Adds up the sizes of the specified dir-info object.
        Recursively processes all subdir infos.

        @param dir_info - [optional] DirInfo, info object (with complete subdir info),
            if not specified, the class' dir_info attribute will be used
        @retval size - float, summed size of the dir-info object
        """
        if dir_info is None:
            # no info object specified (top-level call in the recursion)  ->  use base info
            dir_info = self.base_dir_info

        # calculate the dir's size: sum the sizes of its subdirs and add sizes of files in the dir
        subdirs_size = sum([self._sum_sizes(info) for info in dir_info['dirs'].values()])
        dir_info['size'] = dir_info['files_size'] + subdirs_size

        return dir_info['size']

    def _check_incompleteness(self, dir_info):
        """
        Checks if the dir or any subdir of the specified dir info was incompletely
        analysed (i.e. deneid access).

        @param dir_info - DirInfo, info object (with subdir info)
        @retval incompleteness - bool, True if info is incomplete, False otherwise
        """
        incompleteness = dir_info['incomplete']
        if not incompleteness:
            for info in dir_info['dirs'].values():
                incompleteness = self._check_incompleteness(info)
                if incompleteness:
                    break

        return incompleteness

    def _format_size(self, size, unit_indent=True):
        """
        Converts numerical size value into displayable string.
        Automatically determines which unit to use (B, kB, MB, GB etc.)

        @param size float
        @retval size_string string
        """
        # units = 'KMGTP'     # list of unit prefixes

        # determine order of magnitude of specified size
        try:
            order = min(int(math.log(size, self._unit_scale)), len(self._units))
        except ValueError:
            order = 0

        # assemble output string
        if order > 0:
            # prefix is required; tune number of decimal places
            if unit_indent:
                crab = ' ' * (order + 1)
            else:
                crab = ' '
            fish = '{{:.{:.0f}f}}{}{}B'.format(order+1, crab, self._units[order-1])
            fish = fish.format(size / math.pow(self._unit_scale, order))
        else:
            # no prefix required (size in Bytes)
            fish = '{:.0f} B '.format(size)

        if unit_indent:
            fish += ' ' * (len(self._units) - order)

        return fish


#===========================================================================


class _BackgroundSizer(Sizer):
    """
    Wraps the "Sizer" class for usage in a seperate process.
    Adds communication abilities.
    Allows to run the sizer until a quit message is sent.
    """
    def __init__(self, connection, worker_id=None):
        """
        Initialisation.

        @param connection - multiprocessing.Connection object
        @param worker_id - [optional] arbitrary object to use as ID in any displayed messages
        """
        super().__init__()      # init base class
        self.id = worker_id     # store worker ID
        self._connection = connection   # store connection object (pipe end)
        self.is_idle = True     # init idle flag

    def run(self):
        """
        Main method of the class.
        Handles messaging via the connection object:
            - Receives "process" messages, which trigger the analysis of a dir
            - Sends "done" messages when an analyis is finished
            - Receives and responds to "share" messages, which allow to "source out"
              a part of the current analysis
            - Exits when "quit" message is received
        """
        time_start = datetime.datetime.now()    # init start time object (just for debugging / performance measurment)

        while True:     # main loop

            # define waiting time for messages depending on idleness
            if self.is_idle:
                polling_time = 0.2      # wait longer if idle
            else:
                polling_time = 0

            # check for and handle any messages
            if self._connection.poll(polling_time):

                message = self._connection.recv()      # fetch message from connection

                #-------- quit request
                if message['type'] == 'quit':
                    return      # exit

                #-------- request to analyse a dir
                elif message['type'] == 'process':
                    dir_path = message['dir']   # unpack requested dir from message

                    if not self.is_idle:
                        # a busy worker cannot be assigned to another dir
                        raise WorkerError('Worker [{}] is currently busy. Could not assign worker to new dir "{}".'.format(self.id, dir_path))

                    # start analysis
                    self.is_idle = False    # set state flag to busy
                    self._set_base_dir(dir_path)   # set specified dir as new base dir
                    time_start = datetime.datetime.now()    # just for performance info: note start time
                    self._iterate_dir_list()    # perform a single analysis iteration to initialise the info tree
                    if self._dir_stock:
                        # handle re-using of an existing info object
                        self._insert_info(self._info_stock, self._dir_stock)    # insert the existing info object into the info tree

                #-------- request to hand over some of the dirs from the queue of the current analysis
                elif message['type'] == 'share':
                    if datetime.datetime.now() < message['expiration']:     # ignore request if it has expired
                        if self.is_idle:
                            # worker is idle  ->  nothing to share, send empty list
                            # raise WorkerError('Worker [{}] is idle. Hand-over impossible.'.format(self.id))
                            logging.warning('Worker [{}] is idle. Hand-over impossible.'.format(self.id))
                            dir_list = []
                        else:
                            # worker is busy  ->  try to remove requested number of dirs from current analysis
                            n_dirs = message['n_dirs']      # requested number of dirs
                            if len(self._dir_list) > 1:
                                split_index = max(1, (len(self._dir_list) - n_dirs))    # don't cut off more dirs than available
                                dir_list = self._dir_list[split_index:]     # copy hand-over dirs from analysis queue
                                self._dir_list = self._dir_list[:split_index]   # shorten analysis queue to remove hand-over dirs
                            else:
                                # worker has only a single dir in its analysis queue  ->  nothing to share, send empty list
                                dir_list = []

                        # finally hand over the dirs by sending back a share message with the dir list
                        self._connection.send({'type': 'share', 'dirs': dir_list})

                else:
                    #---- unknown message type  ->  guru meditation
                    raise WorkerError('Worker [{}]: Received unhandled message: "{}".'.format(worker.worker_id, message))


            # advance current analysis if there is any
            if not self.is_idle:
                # iterate analysis for a certain number of steps or until the list of dirs to analyse is empty
                n_iterations = 20   # number of iteration steps to perform at most until next message checking
                while n_iterations and self._dir_list:
                    self._iterate_dir_list()
                    n_iterations -= 1

                # send results and signalise idleness if analyis is complete
                if not self._dir_list:
                    # delete any existing, re-used info object
                    self._dir_stock = ''
                    self._info_stock = None

                    time_end = datetime.datetime.now()  # just for performance info: note end time

                    # print('===== worker [{}]: analysis finished'.format(self.id))
                    # print('===== elapsed time: ', str(time_end - time_start))
                    # print('===== insertion cache: {} hits, {} misses'.format(self._last_counter[1], self._last_counter[0]))
                    self._last_counter = [0, 0]    # just for debugging/info: init counters for insertion cache misses & hits

                    # finally propagate the analysis result
                    self._connection.send({'type': 'done', 'info': self.base_dir_info, 'dir': self.base_dir})

                    self.is_idle = True     # set worker state to idle


class WorkerError(Exception):
    """
    Exception raised by the _BackgroundSizer.
    Causes: invalid assigment, invalid message
    """
    def __init__(self, message):
        super().__init__(self, message)


class MultiSizer(object):
    """
    Performs the size analysis for a specified directory and displays the results.
    Runs the analysis in background processes to distribute and speed up the work.
    Can/should be used as a context manager for automatic clean-up of background processes.
    """
    def __init__(self):
        """
        Initialisation.
        """
        self.sizer = Sizer()    # create the sizer object for collecting the results
        self._workers = []  # init list of background workers

    def __del__(self):
        """
        Deconstruction.
        Stops any running background workers.
        """
        self._stop_workers()

    def __enter__(self):
        """
        Context-manager initialisation:
        Nothing special here, just returns itself.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context-manager exit:
        Stops any running background workers.
        Passes any exception.
        """
        self._stop_workers()    # stop workers
        return False        # signalise to raise any exception

    def _create_worker(self, worker_id):
        """
        Creates a new worker: a process prepared to run a _BackgroundSizer.

        @param worker_id - arbitrary object, used as identifier in any messages (debug, error, etc.)
        @retval worker - multiprocessing.Process object with added attributes
        """
        # create pipe for communication with worker process
        connection_here, connection_there = multiprocessing.Pipe()

        # create a new worker
        worker = multiprocessing.Process(target=_worker_main, args=(connection_there, worker_id))

        # set worker attributes
        worker.worker_id = worker_id    # store ID (used in any messages)
        worker.connection = connection_here     # store one end of the communication pipe
        worker.is_idle = True   # set flag for indicating whether worker is idle
        worker.task_count = 0   # set info counter for number of accomplished tasks
        # worker.start()

        return worker

    def _start_workers(self, n_workers=None):
        """
        Starts the background workers.

        @param n_workers - [optional] int, number of workers to start, default: multiprocessing.cpu_count()
        """
        self._stop_workers()    # stop any running workers at first

        if n_workers is None:
            # number of workers not specified  ->  use number of processors/cores
            n_workers = multiprocessing.cpu_count()
            # n_workers = 8

        # create & start the workers
        for i in range(n_workers):
            worker = self._create_worker(i)     # create new worker
            worker.start()      # start the worker's process
            self._workers.append(worker)    # store worker in internal list
            logging.debug('Started background worker [{}].'.format(worker.worker_id))

    def _stop_workers(self):
        """
        Stops (ends) any running background workers.
        """
        # send quit signal
        for worker in self._workers:
            if worker.is_alive():
                worker.connection.send({'type': 'quit'})

        # ensure workers have quit, terminate if still running
        while self._workers:
            worker = self._workers.pop()
            if worker.is_alive():
                worker.join(3)
                worker.terminate()

            logging.debug('Stopped background worker [{}], task count: {}.'.format(worker.worker_id, worker.task_count))

    def _get_busy_workers(self):
        """
        Returns the workers which are currently busy (i.e. running an analysis).

        @retval busy_workers - list of multiprocessing.Process objects as create by "_create_worker"
        """
        return [worker for worker in self._workers if not worker.is_idle]

    def _get_idle_workers(self):
        """
        Returns the workers which are currently idle.

        @retval busy_workers - list of multiprocessing.Process objects as create by "_create_worker"
        """
        return [worker for worker in self._workers if worker.is_idle]

    def _run(self):
        """
        Main analysis loop.
        Handles the distribution of work.
        Returns when the analysis is complete.
        """
        # init the analysis if necessary
        if not self._get_busy_workers():
            # all workers idle  ->  assign the base dir to the first worker
            self._workers[0].connection.send({'type': 'process', 'dir': self.sizer.base_dir})
            self._workers[0].is_idle = False

        # prepare the sharing/hand-over mechanism:
        # - only a single share request can be active at a time
        # - define a time span after which a share request expires to avoid waiting infinitely
        #   for a response to a share request
        share_request_expiration = datetime.timedelta(seconds=5)    # time span for expiration
        pending_share_request = None    # set current share request to none

        # main loop: iterate until all workers are idle
        while self._get_busy_workers():

            for worker in self._workers:
                # check if current worker is running
                if not worker.is_alive():
                    # worker has crashed  ->  cancel
                    logging.error('Worker [{}] has terminated unexpectedly. Cancelling analysis.'.format(worker.worker_id))
                    return False

                # current worker: check for and handle any messages
                while worker.connection.poll():
                    message = worker.connection.recv()      # fetch message from connection

                    if message['type'] == 'done':
                        #---- worker has finished analysis
                        dir_path = message['dir']
                        dir_info = message['info']
                        self.sizer._insert_info(dir_info, dir_path)
                        worker.is_idle = True
                        worker.task_count += 1
                        logging.debug('Worker [{}] finished dir: {}'.format(worker.worker_id, dir_path))
                        # logging.debug('Inserted beneath {}: {}'.format(dir_path, ', '.join(dir_info['dirs'].keys())))

                    elif message['type'] == 'share':
                        #---- worker hands over dirs from its analysis queue
                        logging.debug('Share response from worker [{}]: dirs={}'.format(worker.worker_id, message['dirs']))
                        assert pending_share_request is not None, 'Internal inconsistency: Got share response from worker [{}] without pending_share_request being set.'.format(worker.worker_id)
                        dir_list = message['dirs']
                        if dir_list:
                            idle_workers = self._get_idle_workers()
                            assert (len(dir_list) <= len(idle_workers)), 'Internal inconsistency: More dirs to distribute than idle workers.'
                            for worker, dir_path in zip(idle_workers, dir_list):
                                worker.connection.send({'type': 'process', 'dir': dir_path})
                                worker.is_idle = False
                                logging.debug('Worker [{}] assigned to dir: {}'.format(worker.worker_id, dir_path))
                        pending_share_request = None

                    else:
                        #---- unknown message type  ->  guru meditation
                        raise TypeError('Unhandled message "{}" received from worker process [{}]'.format(message, worker.worker_id))

                # current worker: if workers is doing something and there is no currently pending
                # share request then send a share request to the current worker
                if not worker.is_idle and not pending_share_request:
                    idle_workers = self._get_idle_workers()     # determine number of idle workers (= number of requested dirs)
                    if idle_workers:
                        # there are idle workers  ->  prepare a shar request with an expiration time
                        # and send it to the current worker
                        pending_share_request = {'type': 'share', 'n_dirs': len(idle_workers),
                                                 'expiration': (datetime.datetime.now() + share_request_expiration),
                                                 'expired': False,
                                                 'worker': worker.worker_id}
                        worker.connection.send(pending_share_request)       # send share request to current worker
                        logging.debug('Sent share request to worker [{}]: {} dirs'.format(worker.worker_id, pending_share_request['n_dirs']))


            # main loop (not worker loop): if there is a pending share request, check if it has expired
            if pending_share_request:
                if pending_share_request['expired']:
                    # share request has already expired  ->  discard it
                    logging.debug('Discarding share request to worker [{}] ({} dirs)'.format(pending_share_request['worker'], pending_share_request['n_dirs']))
                    pending_share_request = None
                elif datetime.datetime.now() > pending_share_request['expiration']:
                    # expiration time of the share request has passed  ->  flag it to be expired
                    logging.debug('Expiring share request to worker [{}] ({} dirs)'.format(pending_share_request['worker'], pending_share_request['n_dirs']))
                    pending_share_request['expired'] = True

            # check whether all workers are busy and wait a bit before the next iteration if so
            idle_workers = self._get_idle_workers()
            if not idle_workers:
                time.sleep(1)

        # final step: return True to signalise successfull analysis
        return True


    def _set_dir(self, directory, _quiet=False):
        """
        Sets the directory to analyse and starts the analysis.

        @param directory - string, path of directory to analyse
        """
        time_start = datetime.datetime.now()    # record start time (just for debugging/info)

        self.sizer._set_base_dir(directory)     # set specified dir in main sizer object
        self._start_workers()   # start the background workers
        success = self._run()             # perform the analysis
        self._stop_workers()    # stop the background workers

        if success:
            self.sizer._sum_sizes()     # calculate all directories' sizes

            time_end = datetime.datetime.now()      # record end time (just for debugging/info)
            print('===== elapsed time: ', str(time_end - time_start))
            print('===== insertion cache: {} hits, {} misses'.format(self.sizer._last_counter[1], self.sizer._last_counter[0]))
            self.sizer._last_counter = [0, 0]    # just for debugging/info: init counters for insertion cache misses & hits

            self.sizer.cdi(_quiet=_quiet)    # prepare for subdir changes, poss. display the results
        else:
            self.sizer.base_dir = None
            self.sizer.base_dir_info = None
            # raise SizerError('')

    def _get_current_dir(self, full_path=True):
        """
        Returns the current directory of the sizer, i.e. the dir whose size information
        would be listed via the "ls" method.

        @param full_path - [optional] bool, flag to control whether full path is returned
            True -> return fullpath (absolute path)
            False -> return last dir name only (last subdir)
        @retval dir - string, current directory or None if no dir is set
        """
        return self.sizer._get_current_dir(full_path=full_path)

    def ls(self):
        """
        """
        self.sizer.ls()

    def cd(self, directory=None, _quiet=False):
        """
        """
        # self.sizer.cd(directory=directory)
        self._set_dir(directory, _quiet=_quiet)

    def cdi(self, index=None):
        """
        """
        self.sizer.cdi(index=index)

    def pwd(self):
        """
        """
        print(self.sizer._get_current_dir())


def _worker_main(connection, worker_id):
    """
    Function which is passed to the multiprocessing.Process object to run a background sizer.

    @param connection - multiprocessing.Connection object
    @param worker_id - arbitrary object to use as ID in any displayed messages
    """
    sizer = _BackgroundSizer(connection, worker_id)     # create the background-sizer object
    sizer.run()     # run the sizer


#===========================================================================


class DirHunterShell(cmd.Cmd):
    """
    Simple shell for dir hunting.
    """
    intro = '\nWelcome to the dir-hunter shell.   Type help or ? to list commands.\n'

    def __init__(self, sizer, directory=None):
        """
        Initialisation.

        @param sizer - Sizer object to use for the actual analysis, either instance
            of Sizer class or MultiSizer class
        @param directory - [optional] string, path of directory to analyse
        """
        super().__init__()
        self.sizer = sizer

        if directory is not None:
            self.sizer.cd(directory, _quiet=True)

        self._update_prompt()

    def onecmd(self, cmd):
        """
        Overloaded base-class method to catch sizer errors.
        """
        try:
            stop = super().onecmd(cmd)

        except:
            # handle errors by displaying but then ignoring them
            print('*** An internal error occurred:\n')
            traceback.print_exc()
            print('')
            stop = False

        return stop


    def do_cd(self, arg):
        """
        Change to directory and analyse it.
        If directory is not specified, changes to the current base directory.
        """
        directory = str(arg)
        if directory:
            self.sizer.cd()
        else:
            self.sizer.cdi()
        self._update_prompt()

    def do_cdi(self, arg):
        """
        Index-based directory changing.
        Change to subdirectory and display its analysis results.
        Indices correspond to rank indices of ls output.
        If index is not specified, changes to the current base directory.

        Examples: "cdi 0" changes to largest subdirectory in current directory
                  "cdi 1" changes to 2nd largest subdirectory
                  "cdi -1" changes to parent directory (up to base directory of analysis)
        """
        index = None
        try:
            index = int(arg)
        except ValueError:
            if arg:
                print('Error: Invalid argument "{}"'.format(arg))
                self.do_help('cdi')
                return

        self.sizer.cdi(index=index)
        self._update_prompt()

    def do_ls(self, arg):
        """
        Display analysis results of current directory.
        """
        self.sizer.ls()

    def do_pwd(self, arg):
        """
        Display the current directory.
        """
        self.sizer.pwd()

    def do_x(self, arg):
        """
        Quit the shell.
        """
        return True

    def _update_prompt(self):
        """
        Updates the shell prompt to show the current (sub)dir.
        """
        self.prompt = 'dirhunt({})> '.format(self.sizer._get_current_dir(False))


#===========================================================================


def test_sizer(directory=None):
    """
    """
    if directory is None:
        os.path.expanduser('~')

    with MultiSizer() as msizer:
        msizer._set_dir(directory)

    return msizer


def test_shell(directory=None):
    """
    """
    if directory is None:
        os.path.expanduser('~')

    with MultiSizer() as sizer:
        shell = DirHunterShell(sizer, directory)
        shell.cmdloop()



#===========================================================================
#===========================================================================


if __name__ == '__main__':

    try:
        dir_path = sys.argv[1]
    except IndexError:
        dir_path = os.path.expanduser('~')

    # sizer = test_sizer(dir_path)
    test_shell(dir_path)
