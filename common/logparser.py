import os
from common.helpers import path_joiner
from common.config import logs_path

import logging
logger = logging.getLogger(__name__)

class PidException(Exception):
    pass


class WrongFormat(Exception):
    pass


class LogTail(object):
    def __init__(self, filename, offset_name='offset'):
        self.filename = filename
        self.offset_filename = path_joiner(
            logs_path,
            "%s.%s" % (os.path.basename(self.filename), offset_name)
        )
        self._fh = None
        (self._log_inode, self._log_offset) = self._read_log_offset()
        self._read_rotated_log = False

        if self._log_inode != os.stat(self.filename).st_ino or os.stat(self.filename).st_size < self._log_offset:
            if not self._try_open_rotated_log():
                self._log_offset = 0

        if not self._fh:
            self._fh = open(self.filename, mode="r", encoding='utf8', errors='replace')
            self._log_inode = os.stat(self.filename).st_ino
        self._fh.seek(self._log_offset)

    def __del__(self):
        if self._fh:
            self._fh.close()

    def __iter__(self):
        return self

    def _read_log_offset(self):
        try:
            f = open(self.offset_filename, "r")
        except IOError as e:
            return None, 0
        try:
            inode = int(f.readline())
            offset = int(f.readline())
        except ValueError as e:
            return None, 0
        finally:
            f.close()
        return inode, offset

    def _write_log_offset(self):
        try:
            f = open(self.offset_filename, "w")
        except IOError as e:
            logger.info("Unable to write log offset file %s: %s" % (self.offset_filename, e.message))
            return
        try:
            os.fchmod(f.fileno(), 0o600)
            self._log_offset = self._fh.tell()
            f.write("%s\n%s\n" % (self._log_inode, self._log_offset))
        finally:
            f.close()

    def _try_open_rotated_log(self):
        rotated_filename = "%s.1" % self.filename
        if os.path.exists(rotated_filename) and \
                        self._log_inode == os.stat(rotated_filename).st_ino:
            self._fh = open(rotated_filename, mode="r", encoding='utf8', errors='replace')
            self._read_rotated_log = True
            return True
        return False

    def __next__(self):
        line = self._fh.readline()
        if not line:
            if self._read_rotated_log:
                self._fh.close()
                self._read_rotated_log = False
                self._fh = open(self.filename, mode="r", encoding='utf8', errors='replace')
                self._log_offset = 0
                self._log_inode = os.stat(self.filename).st_ino
                line = self._fh.readline()
                if not line:
                    self._write_log_offset()
                    raise StopIteration()
            else:
                self._write_log_offset()
                raise StopIteration()

        return line

    def skip_rest(self):
        self._fh.seek(-1, 2)
        self._fh.readline()
        self._write_log_offset()
