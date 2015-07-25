#! /usr/bin/python

# alias dl='docker ps -l -q'
# alias dal='docker exec -i -t `dl` bash'

import sys
import subprocess
import argparse
import os
import json

description = """Manage a cluster of docker containers."""

directory = os.path.abspath( os.path.dirname( __file__ ) )
config_file = os.path.join( directory  , "cluster" )

def parse_cmd( argv ):
    parser = argparse.ArgumentParser( description = description , formatter_class = argparse.ArgumentDefaultsHelpFormatter )
    subparsers = parser.add_subparsers( title = "commands" , dest = "command" )
    
    init_parser = subparsers.add_parser( "init" , help="Init the cluster" )
    init_parser.add_argument( "image" , help="docker image" , type= str )
    init_parser.add_argument( "--name" , help="image name" , type=str , default="node" )
    init_parser.add_argument( "--host" , help="host name" , type=str , default="node" )
    init_parser.add_argument( "--number" , help="number of nodes" , type=int , default=4 )

    run_parser = subparsers.add_parser( "run" , help = "Run the cluster." )

    start_parser = subparsers.add_parser( "start" , help = "Start the cluster." )

    stop_parser = subparsers.add_parser( "stop" , help = "Stop the cluster." )

    rm_parser = subparsers.add_parser( "rm" , help = "Remove the cluster." )

    status_parser = subparsers.add_parser( "status" , help="Get status of the cluster." )
    status_parser.add_argument( "--verbose" , "-v" , help="Verbose output" , action='store_true' )

    copy_parser = subparsers.add_parser( "cp" , help="Copies files to all nodes." )
    copy_parser.add_argument( "-r" , help="copy recursively" , action='store_true' )
    copy_parser.add_argument( "source" , help="Source file or files." , nargs='+' )
    copy_parser.add_argument( "target" , help="Target file of directory." )

    cmd_parser = subparsers.add_parser( "cmd" , help="Execute command on all nodes." )

    args = parser.parse_args( argv[1:] )
    return args

def run_cmd( cmd , no_err = False ):
    # print cmd
    process = None
    if no_err:
        FNULL = open( os.devnull, 'w' )
        process = subprocess.Popen( cmd  , shell=True , stdout=subprocess.PIPE , stderr=FNULL )
    else: 
        process = subprocess.Popen( cmd  , shell=True , stdout=subprocess.PIPE )
    out , err = process.communicate()
    return process.returncode , out

def call_cmd( cmd ):
    return subprocess.call( cmd , shell=True )

def error( msg ):
    print "Error: " + msg
    exit( -1 )

def get_config():
    if os.path.isfile( config_file ):
        try:
            return json.load( open( config_file , "r" ) )    
        except Exception as e:
            error( "Could not read config file:" + str( e ) )
    else:
        error( "No config file found in current directory." )

def get_default_config():
    config = {}
    config[ "name" ] = "node"
    config[ "host" ] = "host"
    config[ "number" ] = 4
    return config

def create_config( config ):
    if os.path.isfile( config_file ):
        error( "Config already exist in current directory." )
    json.dump( config , open( config_file , "w" ) , indent = 2 )

def get_number_string( i , n ):
    if ( n < 0 ) or ( i < 0 ) or ( i > n ):
        raise Exception( "n (" + str( n ) + ") must be positive" )
    if n < 10:
        return "%01d" % i
    elif n < 100:
        return "%02d" % i
    elif n < 1000:
        return "%03d" % i
    elif n < 10000:
        return "%04d" % i
    else:
        return str( i )


def get_nodes( config ):
    n = config[ "number" ]
    nodes = []
    for i in range( 1 , n + 1 ):
        name = config[ "name" ] + get_number_string( i , n )
        host = config[ "host" ] + get_number_string( i , n )
        nodes.append( [ name , host ] )
    return nodes




image = "karsten/flink-base"

def get_ip( node ):
    cmd = "docker inspect --format '{{ .NetworkSettings.IPAddress }}' " + node[0]
    ret = run_cmd( cmd )
    if ret[0]:
        error( "Could no get ip from container " + node[0] )
    return ret[1][:-1]

def get_host_ip():
    cmd = "/sbin/ifconfig docker0 | grep \"inet addr\" | awk -F: '{print $2}' | awk '{print $1}'"
    ret = run_cmd( cmd )
    if ret[0]:
        error( "Could not get host ip address." )
    return ret[1][:-1]

def write_hosts( ips ):
    marker = "# cluster"
    with open( '/etc/hosts' , 'r' ) as f:
        s = f.read()
    pos = s.find( marker )
    if pos == -1:
        s += "\n\n" + marker + "\n"
    else:
        s = s[:pos] + marker + "\n"
    for ip in ips:
        s += ip[1] + " " + ip[0] + "\n"
    with open('/tmp/hosts', 'w') as outf:
        outf.write(s)
    os.system( "sudo mv /tmp/hosts /etc/hosts" )


def run():
    config = get_config()
    nodes = get_nodes( config )
    hostIp = get_host_ip()
    ips = []
    for node in nodes:
        cmd = "docker run -d -h " + node[1] + " --dns=" + hostIp + " --name " + node[0] + " " + config[ "image" ]
        ret = run_cmd( cmd )
        id = ret[1][:-1]
        if ret[0]:
            error( "Could not create container " + node[0] )
        ip = get_ip( node )
        ips.append( [ node[1] , ip ] )
        print "Created container " + node[0] + ", id = " + id + ", ip = " + ip
    
    write_hosts( ips )
    with open( "hosts" , "w" ) as f:
        for ip in ips:
            f.write( ip[1] + " " + ip[0] + "\n" )
        f.close()
    # print "To enable dns run"
    # print "sudo dnsmasq -q -h -H `pwd`/hosts"


def start():
    config = get_config()
    nodes = get_nodes( config )
    for node in nodes:
        cmd = "docker start " + node[0]
        ret = run_cmd( cmd )
        if ret[0]:
            error( "Could not start container " + node[0] )
        print "Starting container " + node[0]


def stop():
    config = get_config()
    nodes = get_nodes( config )
    for node in nodes:
        cmd = "docker stop " + node[0]
        ret = run_cmd( cmd )
        if ret[0]:
            error( "Could not stop container " + node[0] )
        print "Stoping container " + node[0]


def rm():
    stop()
    config = get_config()
    nodes = get_nodes( config )
    for node in nodes:
        cmd = "docker rm  " + node[0]
        ret = run_cmd( cmd )
        if ret[0]:
            error( "Could not remove container " + node[0] )
        print "Removing container " + node[0]

def get_rt_status( node ):
    cmd = "docker inspect --format '{{ .State.Running }}' " + node[0]
    ret = run_cmd( cmd , True )
    status = ret[1][:-1]
    if ret[0]:
        return "no container"
    if status == 'true':
        return "running"
    else:
        return "stopped"

def status( args ):
    config = get_config()
    nodes = get_nodes( config )
    if args.verbose:
        status = {}
        status[ "configuration" ] = config
        status[ "cluster" ] = []
        for node in nodes:
            n = {}
            n[ "name" ] = node[0]
            n[ "host" ] = node[1]
            n[ "status" ] = get_rt_status( node )
            if n[ "status" ] != "no container":
                n[ "ip" ] = get_ip( node )
            status[ "cluster" ].append( n )
        print json.dumps( status , indent = 2 )
    else:
        for node in nodes:
            state = get_rt_status( node )
            print node[0] + " : " + state


def init( args ):
    config = get_default_config()
    config[ "name" ] = args.name
    config[ "image" ] = args.image
    config[ "number" ] = args.number
    config[ "host" ] = args.host
    create_config( config )

def copy( args ):
    config = get_config()
    nodes = get_nodes( config )
    for node in nodes:
        cmd = "scp"
        if args.r: cmd += " -r"
        for s in args.source:
            cmd += " " + s
        cmd += " " + node[1] + ":" + args.target
        print cmd
        call_cmd( cmd )

def cmd( args ):
    raise Exception( "cmd not implemented yet." )

def main( argv ):
    
    args = parse_cmd( argv )
        
    if args.command == "run":
        run()
    if args.command == "start":
        start()
    elif args.command == "stop":
        stop()
    elif args.command == "rm":
        rm()
    elif args.command == "status":
        status( args )
    elif args.command == "init":
        init( args )
    elif args.command == "cp":
        copy( args )
    elif args.command == "cmd":
        cmd( args )



if __name__ == "__main__" :
    main( sys.argv )
