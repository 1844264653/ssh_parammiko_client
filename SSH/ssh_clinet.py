import paramiko
import time
import socket
import atexit
import random
import traceback


RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_INTERVAL_MAX = 10

NETWORK_ERROR = (socket.error, EOFError,
                 paramiko.ssh_exception.NoValidConnectionsError,
                 paramiko.ssh_exception.SSHException)


def retry(fn):
    def _retry(*args, **kwargs):
        retry_times = RETRY_TIMES if RETRY_ENABLED else 1
        for i in range(retry_times):
            try:
                return fn(*args, **kwargs)
            except NETWORK_ERROR as e:
                if kwargs.get("not_retry", None):
                    raise

                if i < retry_times - 1:
                    # LOG.warning("%s failed(%s): %s\n%s" %
                    #             (fn.func_name, type(e), e, traceback.format_exc()))
                    time.sleep(random.randint(1, RETRY_INTERVAL_MAX))
                else:
                    raise

    return _retry


class CommandFailedError(RuntimeError):
    pass


class SSH(object):
    DISCONNECT_ERROR = (socket.error, EOFError,
                        paramiko.ssh_exception.NoValidConnectionsError,
                        paramiko.ssh_exception.SSHException, CommandFailedError)

    CACHE_SSH = {}

    @classmethod
    def close_all_ssh(cls):
        for ssh in cls.CACHE_SSH.values():
            ssh.close()

    @retry
    def __init__(self, host, user, password, port=22, refresh=False, not_retry=False):
        if ':' in host:
            host = host.partition(':')[0]

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        if host in self.CACHE_SSH and not refresh:
            self.ssh = self.CACHE_SSH[self.host]
        else:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy())
            self.ssh.connect(self.host, self.port, self.user, self.password)
            self.CACHE_SSH[self.host] = self.ssh

    @retry
    def execute(self, command, timeout=None, real_time_output=False,
                read_error=False, quiet=False, not_retry=False):
        """
        Execute command on remote host

        :param command: the shell command to execute
        :param timeout: timeout to wait command exceuted and return
        :param real_time_output: True for real-time output, suitable for slow command
        :param read_error: True for read result from stderr, such as "wget"
        :param quiet: True for reducing log output
        :param not_retry: True for not retry if execution failed, controlled by execute and @retry
        :return:
        """
        if real_time_output and timeout is None:
            timeout = 180

        try:
            stdin, stdout, stderr = self.ssh.exec_command(command, timeout=timeout)
        except self.DISCONNECT_ERROR as e:
            if not_retry:
                raise
            else:
                # LOG.warning("Execute command(%s) in %s failed(%s): %s, now try again" %
                #             (command, self.host, type(e), e.message))
                self.ssh.close()
                self.ssh.connect(self.host, self.port, self.user, self.password)
                stdin, stdout, stderr = self.ssh.exec_command(command, timeout=timeout)

        output = stderr if read_error else stdout
        if real_time_output:
            old_line = ''
            while True:
                line = output.readline()
                if len(line) == 0:
                    errmsg = stderr.read()
                    status = stdout.channel.recv_exit_status()
                    if status is 0:
                        if errmsg and not quiet:
                            # LOG.info("stderr: %s" % errmsg)
                        return
                    # else:
                    #     raise CommandFailedError("Execute command(%(cmd)s) failed: ")

# todo 