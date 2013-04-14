# Some commands

    fern = Server('fern')
    fern.write_file('/tmp/voila-hadoop-deliver', 'April 3rd, 2013, you\'re right.')
    print '-------'
    print fern.recvall()
    print '-------'*5
    print fern.communicate('whoami')

    fern = Server('fern')
    fern.write_tree('/Users/chbrown/Desktop/muolou', '/tmp/testtree')

## Installation

You'll need to install a custom betterwalk, first:

    pip install -e git://github.com/chbrown/betterwalk.git#egg=betterwalk
