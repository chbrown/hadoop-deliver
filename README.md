# Some commands

fern = Server('fern')
fern.write_file('/tmp/voila-hadoop-deliver', 'April 3rd, 2013, you\'re right.')
print '-------'
print fern.recvall()
print '-------'*5
print fern.communicate('whoami')
