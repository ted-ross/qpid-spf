[NAME]

qpidd \- the Qpid AMQP Message Broker Daemon

[SYNOPSIS]

qpidd [-p port] [--config config_file] [--data-dir directory]

[DESCRIPTION]

An AMQP message broker daemon that stores, routes and forwards
messages using the Advanced Message Queueing Protocol (AMQP).

[OPTIONS]

The options below are built-in to qpidd. Installing add-on modules provides additional options. To see the full set of options available type "qpidd --help"

Options may be specified via command line, environment variable or configuration file. See FILES and ENVIRONMENT below for details.

[FILES]
.I /etc/qpidd.conf
.RS
Default configuration file.
.RE

Configuration file settings are over-ridden by command line or environment variable settings. '--config <file>' or 'export QPID_CONFIG=<file>' specifies an alternate file.

Each line is a name=value pair. Blank lines and lines beginning with # are ignored. For example:

  # My qpidd configuration file.
  port=6000
  max-connections=10
  log-to-file=/tmp/qpidd.log

[ENVIRONMENT]
.I QPID_<option>
.RS
There is an environment variable for each option.
.RE

The environment variable is the option name in uppercase, prefixed with QPID_ and '.' or '-' are replaced with '_'. Environment settings are over-ridden by command line settings. For example:

  export QPID_PORT=6000
  export QPID_MAX_CONNECTIONS=10
  export QPID_LOG_TO_FILE=/tmp/qpidd.log

[AUTHOR]

The Apache Qpid Project, dev@qpid.apache.org

[REPORTING BUGS]

Please report bugs to users@qpid.apache.org
