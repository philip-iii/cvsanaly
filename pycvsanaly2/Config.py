# Copyright (C) 2007  GSyC/LibreSoft
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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
# Authors: Carlos Garcia Campos <carlosgc@gsyc.escet.urjc.es>


class ErrorLoadingConfig(Exception):

    def __init__(self, message=None):
        Exception.__init__(self)

        self.message = message


class Config(object):

    __shared_state = {'debug': False,
                      'quiet': False,
                      'profile': False,
                      'repo_logfile': None,
                      'save_logfile': None,
                      'no_parse': False,
                      'db_driver': 'mysql',
                      'db_user': 'operator',
                      'db_password': None,
                      'db_database': 'cvsanaly',
                      'db_hostname': 'localhost',
                      'extensions': [],
                      'hard_order': False,
                      'branch': None,
                      'low_memory': False,
                      'backout': False,
                      # Metrics extension options
                      'metrics_all': False,
                      'metrics_noerr': False,
                      # Threading options
                      'max_threads': 10,
                      # Content options
                      'no_content': False,
                      # File count extension options
                      'count_types': [],
                      # Regex for matching bug fixes in BugFixMessage
                      'bug_fix_regexes': ["defect(s)?", "patch(ing|es|ed)?",
                                          "bug(s|fix(es)?)?",
                                          "(re)?fix(es|ed|ing|age|\s?up(s)?)?",
                                          "debug(ged)?",
                                          "\#\d+", "back\s?out",
                                          "revert(ing|ed)?"],
                      'bug_fix_regexes_case_sensitive': ["[A-Z]+(-|#)\d+",
                                                         "CVE-\d+-\d+"],
                      # Should merge commits be analyzed.
                      'analyze_merges': False,
                      # Should comments be ignored, when running hunk_blame?
                      'hb_ignore_comments': False,
                     }

    def __init__(self):
        self.__dict__ = self.__shared_state

    def __getattr__(self, attr):
        return self.__dict__[attr]

    def __setattr__(self, attr, value):
        object.__setattr__(self, attr, value)

    def __load_from_file(self, config_file):
        try:
            from types import ModuleType
            config = ModuleType('cvsanaly-config')
            f = open(config_file, 'r')
            exec f in config.__dict__
            f.close()
        except Exception, e:
            raise ErrorLoadingConfig("Error reading config file %s (%s)" % \
                                     (config_file, str(e)))

        try:
            self.debug = config.debug
        except:
            pass
        try:
            self.quiet = config.quiet
        except:
            pass
        try:
            self.profile = config.profile
        except:
            pass
        try:
            self.repo_logfile = config.repo_logfile
        except:
            pass
        try:
            self.save_logfile = config.save_logfile
        except:
            pass
        try:
            self.no_parse = config.no_parse
        except:
            pass
        try:
            self.db_driver = config.db_driver
        except:
            pass
        try:
            self.db_user = config.db_user
        except:
            pass
        try:
            self.db_password = config.db_password
        except:
            pass
        try:
            self.db_database = config.db_database
        except:
            pass
        try:
            self.db_hostname = config.db_hostname
        except:
            pass
        try:
            self.extensions.extend([item for item in config.extensions \
                                    if item not in self.extensions])
        except:
            pass
        try:
            self.count_types.extend([item for item in config.count_types \
                                    if item not in self.count_types])
        except:
            pass
        try:
            self.hard_order = config.hard_order
        except:
            pass
        try:
            self.low_memory = config.low_memory
        except:
            pass
        try:
            self.branch = config.branch
        except:
            pass
        try:
            self.metrics_all = config.metrics_all
        except:
            pass
        try:
            self.metrics_noerr = config.metrics_noerr
        except:
            pass
        try:
            self.max_threads = config.max_threads
        except:
            pass
        try:
            self.bug_fix_regexes = config.bug_fix_regexes
        except:
            pass
        try:
            self.bug_fix_regexes_case_sensitive = \
                config.bug_fix_regexes_case_sensitive
        except:
            pass
        try:
            self.no_content = config.no_content
        except:
            pass

        try:
            self.backout = config.backout
        except:
            pass
        try:
            self.analyze_merges = config.analyze_merges
        except:
            pass
        try:
            self.hb_ignore_comments = config.hb_ignore_comments
        except:
            pass

    def load(self):
        import os
        from utils import cvsanaly_dot_dir, printout

        # First look in /etc
        # FIXME /etc is not portable
        config_file = os.path.join('/etc', 'cvsanaly2')
        if os.path.isfile(config_file):
            self.__load_from_file(config_file)

        # Then look at $HOME
        config_file = os.path.join(cvsanaly_dot_dir(), 'config')
        if os.path.isfile(config_file):
            self.__load_from_file(config_file)
        else:
            # If there's an old file, migrate it
            old_config = os.path.join(os.environ.get('HOME'), '.cvsanaly')
            if os.path.isfile(old_config):
                printout("Old config file found in %s, moving to %s",
                         (old_config, config_file))
                os.rename(old_config, config_file)
                self.__load_from_file(config_file)

    def load_from_file(self, path):
        self.__load_from_file(path)
