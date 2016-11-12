# -*- coding: utf-8 -*-
"""
Created on 2016-11-11

@author: meister

Python3 version
"""

import os.path
import math
import sys
import multiprocessing
import datetime
# import traceback


import logging
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


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
        self._unit_scale = 1000.0
        self._units = 'kMGT'     # list of unit prefixes

        self.base_dir = None    # init attribute for base-dir path
        self.base_dir_info = None   # init attribute for base-dir-info object
        self._info_chain = []    # init attribute for list of current info object and its parent info objects
        self._dir_chain = []    # init attribute for list of current dir name and its parent dir names
        self.__last_info = None     # init internal cache for insertion of dir infos into info tree
        self.__last_path = ''       # init internal cache for insertion of dir infos into info tree
        self.__last_counter = [0, 0]    # just for debugging/info: init counters for insertion cache misses & hits

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
            common_prefix = None
        else:
            # base dir set  ->  check for common path part
            if directory[0] != os.sep:
                # specified directory starts not with slash  ->  relative path, convert to absolute path
                directory = os.path.abspath(os.path.join(self._get_current_dir(), directory))

            # check for poss. common path part
            common_prefix = os.path.commonprefix((self._get_current_dir(), directory))


        # change to the specified dir, depending on the relation between the specified dir and
        # the current dir or base dir, respectively
        if not common_prefix or (common_prefix == os.sep):
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
                raise NotImplementedError('Restraint yourself, apprentice.')


                dir_info_in = dict(self.base_dir_info)
                dir_info_in['path'] = self.base_dir
                self.base_dir = None     # init attribute for base-dir path (string)
                self.base_dir_info = None    # init attribute for dir-info dict
                self._info_chain = []    # init attribute for list of current info object and its parent info objects
                self._current_subdirs = []   # init attribute for list of names of subdirs of current dir
                self._set_base_dir(new_dir, dir_info_in=dir_info_in)     # trigger analysis of specified dir
                self.cdi(_quiet=True)


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

    def _get_current_dir(self):
        """
        Returns the current directory of the sizer, i.e. the dir whose size information
        would be listed via the "ls" method.

        @retval dir - string, current directory path or None if no dir is set
        """
        if self.base_dir is None:
            # no base dir set  ->  return None
            return None
        else:
            # join the base dir and the dir-chain elements (chain is filled by "cdi" method)
            return os.path.join(self.base_dir, *self._dir_chain)

    def _set_base_dir(self, directory):
        """
        Sets the base directory and prepares the size analysis.

        @param directory - string, path of the dir
        """
        self.base_dir = os.path.abspath(directory)   # store full dir path
        self._dir_list = [self.base_dir,]    # init list of dir paths (used during analysis)
        self.__last_info = None     # clear insertion cache (used during analysis)
        self.__last_path = ''       # clear insertion cache (used during analysis)

    def _analyse_base_dir(self):
        """
        Starts the analysis of the currently set base dir, iterates over all its subdirs.
        """
        time_start = datetime.datetime.now()    # just for performance info: note start time

        # iteratively analyse until the list of dirs to analyse is empty
        while self._dir_list:
            self._iterate_dir_list()
        self._sum_sizes()   # finally calculate the dir sizes

        time_end = datetime.datetime.now()  # just for performance info: note end time

        print('===== elapsed time: ', str(time_end - time_start))
        print('===== insertion cache: {} hits, {} misses'.format(self.__last_counter[1], self.__last_counter[0]))

    def _iterate_dir_list(self):
        """
        Triggers the size analysis for the first element of the internal dir list.
        """
        dir_path = self._dir_list.pop(0)    # fetch the first entry of the dir list
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
                        dir_info['size'] += float(stat.st_size)
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
            key 'size' - float, sum of file sizes
            key 'incomplete' - bool, flag to indicate incomplete size analysis (due to denied access)
            key 'dirs' - dict, container for subdirs' info objects (keys are dir names, values are dir infos)
        """
        dir_info = {'size': 0.0, 'incomplete': False, 'dirs': {}}

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
            self.base_dir_info = dir_info

        else:
            # insert the dir info into the internal info tree
            try:
                # try using the internal cache, which holds the last inserted dir info
                # (works if the analysis (caller) is working down a dir branch)
                # reason is to speed things up
                if not self.__last_path or (len(dir_path) < len(self.__last_path)):
                    # last path is not yet set or current path too short to be a potential subdir
                    # -> cache cannot be used
                    raise ValueError

                if dir_path[:len(self.__last_path)] != self.__last_path:
                    # specified dir path is not a subdir of last path
                    # -> cache cannot be used
                    raise ValueError

                # check if the specified dir is exactly a subdir of the last dir (and not e.g. a sub-subdir)
                path_remainder = dir_path[len(self.__last_path):].split(os.sep)     # split remainder of specified dir
                path_remainder = [p for p in path_remainder if p]   # remove split "artifacts" (from initial or trailing slashes)
                if len(path_remainder) != 1:
                    # TODO: remove this condition? should work without
                    # specified dir is a deeper-level subdir of last path
                    # -> cache cannot be used
                    raise ValueError

                # cache check was positive  ->  use the cache
                dir_list = path_remainder   # use subdir remainder as final dir list
                parent_dir_info = self.__last_info  # use cache's info object as parent object for insertion

                self.__last_counter[1] += 1     # just used for debugging info: count the cache "hits"

            except ValueError:
                # cache "miss"  ->  set insertion start at root of dir-info tree
                dir_list = dir_path[len(self.base_dir):].split(os.sep)   # split path part after base-dir part into dir names
                parent_dir_info = self.base_dir_info     # use base-dir info as starting point for insertion

                self.__last_counter[0] += 1     # just used for debugging info: count the cache "misses"


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
            parent_dir_info.update(dir_info)

            # update cache for poss. next insertion
            self.__last_path = dir_path
            self.__last_info = parent_dir_info

    def _sum_sizes(self, dir_info=None):
        """
        Adds up the sizes of the specified dir-info object.
        Recursively processes all subdir infos.

        @param dir_info - [optional] DirInfo, info object (with complete subdir info),
            if not specified, the class' dir_info attribute will be used
        @retval size - float, summed size of the dir-info object
        """
        if dir_info is None:
            dir_info = self.base_dir_info

        # perform size calculation only if not done yet
        # if not dir_info['sized']:
        size = sum([self._sum_sizes(info) for info in dir_info['dirs'].values()])
        dir_info['size'] += size
        # dir_info['sized'] = True    # set flag that size calculation was done

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
