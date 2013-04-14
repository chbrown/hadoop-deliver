#!/usr/bin/env python
import os
import stat
import pwd
# import sys
import argparse
import socket
import paramiko
import time
import betterwalk
from glob import glob

import logging
logging.basicConfig(level=logging.DEBUG)

here = os.path.dirname(os.path.abspath(__file__))
package_dir = os.path.dirname(here)
logging.debug('here=%s package_dir=%s' % (here, package_dir))

known_hosts_path = os.path.expanduser('~/.ssh/known_hosts')
# private_key_path = os.path.expanduser('~/.ssh/id_rsa')

HADOOP_HOME = '/etc/hadoop'


def collapse_path(*parts):
    flat = os.path.join(*parts)
    return os.path.normpath(os.path.expanduser(flat))


def getquadmode(mode):
    user = (mode & stat.S_IRWXU) >> 6
    group = (mode & stat.S_IRWXG) >> 3
    others = (mode & stat.S_IRWXO)
    return '0%d%d%d' % (user, group, others)


class Server(object):
    def __init__(self, hostname):
        self.hostname = hostname

        # with paramiko.SSHClient() as ssh:
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.load_host_keys(known_hosts_path)
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logging.debug('SSHClient connecting to %s' % hostname)
        self.ssh_client.connect(hostname)

        self.transport = self.ssh_client.get_transport()
        self.session = self.transport.open_session()
        self.session.set_combine_stderr(True)
        # self.session.get_pty()
        self.session.invoke_shell()

        logging.debug('Connected to %s:%d.' % self.transport.getpeername())  # e.g., ('69.164.206.80', 22)

    def recvall(self):
        for i in range(10):  # 1s timeout
            if self.session.recv_ready():
                return self.session.recv(1024*1024)
            time.sleep(0.1)
        return ''

    def send(self, command):
        self.session.send(command.strip() + '\n')

    def communicate(self, command):
        self.send(command)
        return self.recvall()

    def write_file(self, filepath, string):
        logging.debug('Writing %d characters to file: %s' % (len(string), filepath))
        session = self.transport.open_session()
        session.set_combine_stderr(True)
        session.invoke_shell()
        session.send('cat > "%s" < /dev/stdin\n' % filepath)
        session.send(string + '\n')
        session.shutdown_write()
        if session.recv_ready():
            return session.recv(1024*1024)
        return ''

    def put(self, sftp, local_path, remote_path, mode=None, buffer_length=32768):
        file_local = file(local_path, 'rb')
        file_remote = sftp.file(remote_path, 'wb')
        if mode:
            file_remote.chmod(mode)
        file_remote.set_pipelined(True)
        try:
            while True:
                data = file_local.read(buffer_length)
                if len(data) == 0:
                    break
                file_remote.write(data)
        finally:
            file_remote.close()

    def copy_tree(self, local, remote):
        cwd = os.getcwd()
        os.chdir(local)
        sftp = paramiko.SFTPClient.from_transport(self.transport)
        for base, dirs, dir_states, files, file_stats in betterwalk.walk_stat('.', fields=['st_mode']):
            for dirname in dirs:
                dirpath = collapse_path(remote, base, dirname)
                try:
                    sftp.mkdir(dirpath, 0774)
                except IOError, exc:
                    logging.warning('Directory already exists: %s (%s)' % (dirpath, exc))
            for filename, st in zip(files, file_stats):
                local_filepath = collapse_path(base, filename)
                remote_filepath = collapse_path(remote, base, filename)
                self.put(sftp, local_filepath, remote_filepath, st.st_mode)
                # self.communicate('chmod %s "%s"' % (getquadmode(local_mode), remote_filepath))
        # return to the working directory we were in before.
        os.chdir(cwd)

    # _, stdout, _ = ssh.exec_command('env')
    # print stdout.read()


def main():
    parser = argparse.ArgumentParser(description='Deliver a Hadoop install to a cluster.')
    parser.add_argument('--namenode', help='Defaults to `hostname -s` on this machine.')
    parser.add_argument('--jobnode', help='Defaults to namenode.')
    parser.add_argument('--slaves', nargs='*')
    parser.add_argument('--user', default=pwd.getpwuid(os.getuid()).pw_name)
    parser.add_argument('--group', default='admin')
    parser.add_argument('--hadoop')
    parser.add_argument('--setup', action='store_true')
    opts = parser.parse_args()

    hadoop = os.path.expanduser(opts.hadoop)
    namenode = opts.namenode or socket.gethostname().split('.')[0]
    jobnode = opts.jobnode or namenode

    datadir = '/mnt/hadoop_test'
    if opts.setup:
        servers = set([namenode, jobnode] + opts.slaves)
        setup_filestructure(servers, opts.user, opts.group, hadoop, datadir)
    write_templates(namenode, jobnode, opts.slaves, opts.user, opts.group, hadoop, datadir)


def put():
    parser = argparse.ArgumentParser(description='Put a file to HDFS.')
    parser.add_argument('filenames', nargs='+')
    opts = parser.parse_args()

    for filename in opts.filenames:
        cmd = 'hadoop fs -put %s %s' % (filename, filename)
        os.system(cmd)

    os.system('hadoop fs -ls')


def dismantle(namenode, jobnode, slaves):
    # hosts = set([namenode, jobnode] + slaves)
    stop_all_sh = os.path.join(HADOOP_HOME, 'bin/stop-all.sh')
    #ssh -Y $slavehost "rm -rf /hadoop/$USER"
    #rm -rf /hadoop/$USER
    # revert the masters and slaves files to default
    # echo 'localhost' > $HADOOP_CONF_DIR/masters
    # echo 'localhost' > $HADOOP_CONF_DIR/slaves


def setup_filestructure(servers, user, group, hadoop, datadir):
    # ensure these dirs exist:
    directories = [HADOOP_HOME, os.path.join(HADOOP_HOME, 'conf'), os.path.join(HADOOP_HOME, 'logs'), datadir]

    for hostname in servers:
        logging.info('Constructing host: %s (as user:group -> %s:%s) ' % (hostname, user, group))
        server = Server(hostname)
        login_message = server.recvall()
        logging.debug(login_message)
        for directory in directories:
            logging.info('Ensuring directory exists: %s' % directory)
            mkdir_output = server.communicate('sudo mkdir -p "%s"' % directory)
            chown_output = server.communicate('sudo chown -R %s:%s %s' % (user, group, directory))
            logging.debug('Result: %s %s' % (mkdir_output, chown_output))

        server.copy_tree(hadoop, HADOOP_HOME)

def write_templates(namenode, jobnode, slaves, user, group, hadoop, datadir):
    masters = list(set([namenode, jobnode]))

    params = dict(
        hadoop_home=HADOOP_HOME,
        jobnode=jobnode,
        port=8021,
        namenode=namenode,
        map_tasks_max=8,
        reduce_tasks_max=8,
        task_xmx='3g',
        datadir=datadir,
        hadoop_heapsize=2000,
        master_list='\n'.join(masters),
        slave_list='\n'.join(slaves)
    )
    logging.debug('Interpolation parameters: %s' % str(params))

    for hostname in masters + slaves:
        server = Server(hostname)

        os.chdir(package_dir)
        for conf_filename in glob('conf/*'):
            logging.debug('Writing config from template: %s' % conf_filename)
            template = open(conf_filename).read()
            try:
                template = template % params
            except TypeError, exc:
                logging.warning('Templating error (%s): %s' % (exc, template[:1000]))
            filepath = os.path.join(HADOOP_HOME, conf_filename)
            write_result = server.write_file(filepath, template)
            logging.debug('Writing to %s: %s' % (filepath, write_result))

    # $HADOOP_HOME/bin/hadoop namenode -format
    # start_all_sh = os.path.join(HADOOP_HOME, 'bin/start-all.sh')
    # server.send('sh %s' % start_all_sh)
