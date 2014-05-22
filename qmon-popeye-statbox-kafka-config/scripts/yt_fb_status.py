#!/usr/bin/python
from __future__ import print_function
from __future__ import division
import sys
import socket
import httplib
import ast


dc_env_2_yt_fb = {
    ('iva', 'production'): ['smith.yt.yandex.net'],
    ('fol', 'production'): ['kant.yt.yandex.net'],
    ('myt', 'testing'): ['quine.yt.yandex.net'],
    ('sas', 'production'): ['plato.yt.yandex.net']
}

kafka_hosts = '/etc/yandex/statbox-kafka/kafka-hosts'
env_file = '/etc/yandex/environment.type'

error_limit = 0.2  # if ratio of failing hosts > error_limit  =>  error
timeout = 5  # timeout to wait http response


def terminate(message=''):
    print('2; {}'.format(message), file=sys.stderr)
    sys.exit(1)

try:
    with open(env_file, 'r') as f:
        my_env = f.read().strip()
except IOError:
    terminate('Missing {}. Can\'t determine environment.'.format(env_file))


host_to_dc = dict()
try:
    with open(kafka_hosts, 'r') as f:
        for line in f:
            host, dc_id, datacenter = line.split()
            host_to_dc[host] = datacenter
except IOError:
    terminate('Missing {}. Can\'t determine hosts belonging to each datacenter.'.format(kafka_hosts))


my_host = socket.getfqdn()
print("My host is {}".format(my_host))
my_dc = host_to_dc.get(my_host, None)

if my_dc is None:
    terminate("Hostname ({}) is not in /etc/yandex/statbox-kafka/kafka-hosts".format(my_host))

print('My datacenter is {}'.format(my_dc))


all_errors = {}
statuses = {
    'Warning': 1,
    'Error': 1 << 1
}
overall_status = 0


class FastboneProblem(Exception):
    pass


def print_and_raise(error):
    print(" {}".format(error))
    raise FastboneProblem(error)


def connect_and_get_response(host, port, path, timeout=timeout):
    """May throw FastboneProblem"""

    print('Connecting to {}:{}{}'.format(host, port,path))
    try:
        connection = httplib.HTTPConnection(host, port, timeout=timeout)
        connection.request('GET', path)
        connection.sock.settimeout(timeout)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        if response.status == 200:
            print(' OK')
        else:
            print_and_raise('Connection to {}:{}{} returned code {}'.format(host, port, path, response.status))
        return data
    except socket.timeout:
        print_and_raise("Connection to {}:{}{} timeouted".format(host, port, path))
    except socket.error as e:
        print_and_raise("Socket error while connecting to {}:{}{} [{}]".format(host, port, path, e))
    except FastboneProblem:
        raise  # has already been printed
    except Exception as e:
        print_and_raise("Problem connecting to {}:{}{} [{!r}]".format(host, port, path, e))


for fb_hosts_address in dc_env_2_yt_fb.get((my_dc, my_env), []):
    errors_list = []
    try:
        data = connect_and_get_response(fb_hosts_address, 80, '/hosts/fb')
        fb_hosts = ast.literal_eval(data)
        print('Hosts are: {}'.format(fb_hosts))
        if not fb_hosts:
            print_and_raise('{}:80/hosts/fb returned empty hosts list: {}'.format(fb_hosts_address, fb_hosts))
    except FastboneProblem as e:
        overall_status |= statuses['Error']
        errors_list.append(e.message)
    else:
        for fb_host in fb_hosts:
            try:
                connect_and_get_response(fb_host, 80, '/hosts/fb')
            except FastboneProblem as e:
                errors_list.append(e.message)
        if errors_list:
            if len(errors_list) / len(fb_hosts) > error_limit:
                overall_status |= statuses['Error']
            else:
                overall_status |= statuses['Warning']
    if errors_list:
        all_errors[fb_hosts_address] = errors_list



if overall_status & statuses['Error']:
    print('2; {}'.format(all_errors), file=sys.stderr)
elif overall_status & statuses['Warning']:
    print('1; {}'.format(all_errors), file=sys.stderr)
else:
    print('0; OK', file=sys.stderr)

