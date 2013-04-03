#!/usr/bin/env python
import os
# import sys
import argparse
import socket
import paramiko
import time
from glob import glob

import logging
logging.basicConfig(level=logging.DEBUG)

# import sys, os, time, paramiko
known_hosts_path = os.path.expanduser('~/.ssh/known_hosts')
# private_key_path = os.path.expanduser('~/.ssh/id_rsa')

HADOOP_HOME = '/etc/hadoop'
# HADOOP_CONF = os.path.join(HADOOP_HOME, 'conf')


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
        # self.session.set_combine_stderr(True)
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
        session = self.transport.open_session()
        session.invoke_shell()
        session.send('cat > "%s" < /dev/stdin\n' % filepath)
        session.send(string + '\n')
        session.shutdown_write()


    # _, stdout, _ = ssh.exec_command('env')
    # print stdout.read()


def main():
    parser = argparse.ArgumentParser(description='Deliver a Hadoop install to a cluster.')
    parser.add_argument('--namenode', help='Defaults to `hostname -s` on this machine.')
    parser.add_argument('--jobnode', help='Defaults to namenode.')
    parser.add_argument('--slaves', nargs='*')
    parser.add_argument('--user', default=os.getlogin())
    opts = parser.parse_args()

    namenode = opts.namenode or socket.gethostname().split('.')[0]
    jobnode = opts.jobnode or namenode

    construct(namenode, jobnode, opts.slaves, opts.user)


def put():
    parser = argparse.ArgumentParser(description='Put a file to HDFS.')
    parser.add_argument('filenames', nargs='+')
    opts = parser.parse_args()

    for filename in opts.filenames:
        cmd = 'hadoop fs -put %s %s' % (filename, filename)
        os.system(cmd)

    os.system('hadoop fs -ls')


def dismantle(namenode, jobnode, slaves, user):
    # hosts = set([namenode, jobnode] + slaves)
    stop_all_sh = os.path.join(HADOOP_HOME, 'bin/stop-all.sh')
    #ssh -Y $slavehost "rm -rf /hadoop/$USER"
    #rm -rf /hadoop/$USER
    # revert the masters and slaves files to default
    # echo 'localhost' > $HADOOP_CONF_DIR/masters
    # echo 'localhost' > $HADOOP_CONF_DIR/slaves


def write_templates(server, params):
    for conf_filename in glob('conf/*'):
        logging.debug('Writing config from template: %s' % conf_filename)
        template = open(conf_filename).read()
        server.write_file(os.path.join(HADOOP_HOME, conf_filename), template % params)


def construct(namenode, jobnode, slaves, user):
    masters = list(set([namenode, jobnode]))

    # ensure these dirs exist:
    datadir = '/mnt/hadoop_test'
    directories = [HADOOP_HOME, os.path.join(HADOOP_HOME, 'conf'), os.path.join(HADOOP_HOME, 'logs'), datadir]

    # render mapred-site.xml.template -> mapred-site.xml
    params = dict(
        hadoop_home=HADOOP_HOME,
        jobnode=jobnode,
        port=8021,
        namenode=namenode,
        map_tasks_max=8,
        reduce_tasks_max=8,
        task_xmx='3g',
        # user=user,
        datadir=datadir,
        hadoop_heapsize=6000
    )
    logging.debug('Interpolation parameters: %s' % str(params))

    for hostname in masters + slaves:
        logging.info('Constructing host: %s' % hostname)
        server = Server(hostname)
        for directory in directories:
            logging.info('Ensuring directory exists: %s' % directory)
            server.communicate('mkdir -p "%s"' % directory)

        write_templates(server, params)
        server.write_file(os.path.join(HADOOP_HOME, 'conf/masters'), '\n'.join(masters))
        server.write_file(os.path.join(HADOOP_HOME, 'conf/slaves'), '\n'.join(slaves))


    # $HADOOP_HOME/bin/hadoop namenode -format
    # start_all_sh = os.path.join(HADOOP_HOME, 'bin/start-all.sh')
    # server.send('sh %s' % start_all_sh)

# fern = Server('fern')
# fern.write_file('/tmp/voila-hadoop-deliver', 'April 3rd, 2013, you\'re right.')
