#!/usr/bin/python

from __future__ import print_function
import rubrik_cdm
import sys
import getpass
import getopt
import datetime
import pytz
import time
import urllib3
urllib3.disable_warnings()
import threading
try:
    import queue
except ImportError:
    import Queue as queue
from random import randrange

def usage():
    print("Usage goes here!")
    exit(0)

def dprint(message):
    if DEBUG:
        print(message)

def python_input(message):
    if int(sys.version[0]) > 2:
        val = input(message)
    else:
        val = raw_input(message)
    return(val)

def get_bytes (size, unit):
    if unit in ('g', 'G'):
        size = size * 1024 * 1024 * 1024
    elif unit in ('t', 'T'):
        size = size * 1024 * 1024 * 1024 * 1024
    elif unit in ('m', 'M'):
        size = size * 1024 * 1024
    else:
        sys.stderr.write("Acceptable units are 'M', 'G' or 'T' case insensitive\n")
        sys.exit (1)
    return (size)

def get_size_from_bytes(bytes):
    if bytes >= 1024 * 1024 * 1024 * 1024:
        size = str(int(bytes/1024/1024/1024/1024)) + "TB"
    elif bytes >= 1024 * 1024 * 1024:
        size = str(int(bytes/1024/1024/1024)) + "GB"
    elif bytes >= 1024 * 1024:
        size = str(int(bytes/1024/1024)) + "MB"
    else:
        size = str(bytes) + " Bytes"
    return(size)

def get_rubrik_nodes(rubrik, user, password, token):
    node_list = []
    cluster_network = rubrik.get('internal', '/cluster/me/network_interface')
    for n in cluster_network['data']:
        if n['interfaceType'] == "Management":
            if token:
                try:
                    rbk_session = rubrik_cdm.Connect(n['ipAddresses'][0], api_token=token)
                except Exception as e:
                    sys.stderr.write("Error on " + n['ipAddresses'][0] + ": " + str(e) + ".  Skipping\n")
                    continue
            else:
                try:
                    rbk_session = rubrik_cdm.Connect(n['ipAddresses'][0], user, password)
                except Exception as e:
                    sys.stderr.write("Error on " + n['ipAddresses'][0] + ": " + str(e) + ".  Skipping\n")
                    continue
            try:
                node_list.append({'session': rbk_session, 'name': n['nodeName']})
            except KeyError:
                node_list.append({'session': rbk_session, 'name': n['node']})
    return(node_list)

def walk_tree (rubrik, id, delim, path, parent):
    offset = 0
    done = False
    file_count = 0
    file_list = []
    while not done:
        job_ptr = randrange(len(rubrik_cluster))
        params = {"path": path, "offset": offset}
        if offset == 0:
            if VERBOSE:
                print("Starting job " + path + " on " + rubrik_cluster[job_ptr]['name'])
            else:
                print (' . ', end='')
        rbk_walk = rubrik_cluster[job_ptr]['session'].get('v1', '/fileset/snapshot/' + str(id) + '/browse', params=params, timeout=timeout)
        file_count = 0
        for dir_ent in rbk_walk['data']:
            offset += 1
            if dir_ent == parent:
                return
            if dir_ent['fileMode'] == "file":
                file_count += 1
                file_list.append((path + delim + dir_ent['filename'], dir_ent['size']))
            elif dir_ent['fileMode'] == "directory" or dir_ent['fileMode'] == "drive":
                if dir_ent['fileMode'] == "drive":
                    new_path = dir_ent['filename']
                elif delim == "/":
                    if path == "/":
                        new_path = "/" + dir_ent['path']
                    else:
                        new_path = path + "/" + dir_ent['path']
                else:
                    if path == "\\":
                        new_path = "\\" + dir_ent['path']
                    else:
                        new_path = path + "\\" + dir_ent['path']
#                files_to_restore = walk_tree(rubrik, id, inc_date, delim, new_path, dir_ent, files_to_restore)
                job_queue.put(threading.Thread(name=new_path, target=walk_tree, args=(rubrik, id, delim, new_path, dir_ent)))
        if not rbk_walk['hasMore']:
            done = True
        print("DIR: " + path + " // FILES: " + str(file_count))
        if file_count > 0:
            dir_list.append(path)
            sample = randrange(len(file_list))
            file_samples.append(file_list[sample])
            if file_count > 10:
                sample = randrange(len(file_list))
                file_samples.append(file_list[sample])
            if file_count > 20:
                sample = randrange(len(file_list))
                file_samples.append(file_list[sample])
        else:
            empty_dir.append(path)



if __name__ == "__main__":
    backup = ""
    rubrik = ""
    user = ""
    password = ""
    fileset = ""
    date = ""
    latest = False
    share_id = ""
    snap_list = []
    restore_location = ""
    restore_share_id = ""
    restore_host_id = ""
    initial_path = ""
    token = ""
    DEBUG = False
    VERBOSE = False
    NAS = False
    timeout = 360
    rubrik_cluster = []
    job_queue = queue.Queue()
    dir_list = []
    empty_dir = []
    file_samples = []
    max_threads = 0
    thread_factor = 10
    max_size = 0
    num_files = 10

    optlist, args = getopt.getopt(sys.argv[1:], 'hDvlc:t:b:f:d:m:M:s:n:', ['--help', '--DEBUG', '--verbose', '--latest',
                                                                       '--creds=', '--token=', '--backup=', '--fileset=',
                                                                       '--date=', '--max_threads=', '--thread_factor=',
                                                                       '--size=', '--number_files='])
    for opt, a in optlist:
        if opt in ('-h', '--help'):
            usage()
        if opt in ('-D', '--DEBUG'):
            DEBUG = True
            VERBOSE = True
        if opt in ('-v', '--verbose'):
            VERBOSE = True
        if opt in ('-l', '--latest'):
            latest = True
        if opt in ('-c', '--creds'):
            (user, password) = a.split(':')
        if opt in ('-t', '--token'):
            token = a
        if opt in ('-b', '--backup'):
            backup = a
        if opt in ('-f', '--fileset'):
            fileset = a
        if opt in ("-d", "--date"):
            date = a
            date_dt = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
            date_dt_s = datetime.datetime.strftime(date_dt, "%Y-%m-%d %H:%M:%S")
        if opt in ('-m', '--max_threads'):
            max_threads = int(a)
        if opt in ('-M', '--thread_factor'):
            thread_factor = int(a)
        if opt in ('-s', '--size'):
            size_s = a
            unit = size_s[-1]
            max_size = int(size_s[:-1])
            max_size = get_bytes(max_size, unit)
        if opt in ('-n', '--number_files'):
            num_files = int(a)

    try:
        rubrik_node = args[0]
    except:
        usage()
    if not backup:
        backup = python_input("Backup (host:share | host): ")
    if ':' in backup:
        NAS = True
    if NAS:
        (host, share) = backup.split(':')
    else:
        host = backup
    if not fileset:
        fileset = python_input("Fileset: ")
    if not token:
        if not user:
            user = python_input("User: ")
        if not password:
            password = getpass.getpass("Password: ")
    if NAS:
        host, share = backup.split(":")
        if share.startswith("/"):
            delim = "/"
        else:
            delim = "\\"
        initial_path = delim
    if token:
        rubrik = rubrik_cdm.Connect(rubrik_node, api_token=token)
    else:
        rubrik = rubrik_cdm.Connect(rubrik_node, user, password)
    rubrik_config = rubrik.get('v1', '/cluster/me', timeout=timeout)
    rubrik_tz = rubrik_config['timezone']['timezone']
    local_zone = pytz.timezone(rubrik_tz)
    utc_zone = pytz.timezone('utc')
    rubrik_cluster = get_rubrik_nodes(rubrik, user, password, token)
    if max_threads == 0:
        max_threads = thread_factor * len(rubrik_cluster)
    print("Using up to " + str(max_threads) + " threads across " + str(len(rubrik_cluster)) + " nodes.")
    if NAS:
        hs_data = rubrik.get('internal', '/host/share', timeout=timeout)
        for x in hs_data['data']:
            if x['hostname'] == host and x['exportPoint'] == share:
                share_id = x['id']
                break
        if share_id == "":
            sys.stderr.write("Share not found\n")
            exit(2)
        fs_data = rubrik.get('v1', str("/fileset?share_id=" + share_id + "&name=" + fileset), timeout=timeout)
    else:
        hs_data = rubrik.get('v1', '/host?name=' + host, timeout=timeout)
        share_id = str(hs_data['data'][0]['id'])
        os_type = str(hs_data['data'][0]['operatingSystemType'])
        dprint("OS_TYPE: " + os_type)
        if os_type == "Windows":
            delim = "\\"
        else:
            delim = "/"
        initial_path = "/"
        if share_id == "":
            sys.stderr.write("Host not found\n")
            exit(2)
        fs_data = rubrik.get('v1', '/fileset?host_id=' + share_id, timeout=timeout)
    fs_id = ""
    for fs in fs_data['data']:
        if fs['name'] == fileset:
            fs_id = fs['id']
            break
    dprint(fs_id)
    snap_data = rubrik.get('v1', str("/fileset/" + fs_id), timeout=timeout)
    for snap in snap_data['snapshots']:
        s_time = snap['date']
        s_id = snap['id']
        s_time = s_time[:-5]
        snap_dt = datetime.datetime.strptime(s_time, '%Y-%m-%dT%H:%M:%S')
        snap_dt = pytz.utc.localize(snap_dt).astimezone(local_zone)
        snap_dt_s = snap_dt.strftime('%Y-%m-%d %H:%M:%S')
        snap_list.append((s_id, snap_dt_s))
    if latest:
        start_index = len(snap_list)-1
        start_id = snap_list[-1][0]
    elif date:
        dprint("TDATE: " + date_dt_s)
        for i, s in enumerate(snap_list):
            dprint(str(i) + ": " + s[1])
            if date_dt_s == s[1]:
                dprint("MATCH!")
                start_index = i
                start_id = snap_list[i][0]
    else:
        for i, snap in enumerate(snap_list):
            print(str(i) + ": " + snap[1] + "  [" + snap[0] + "]")
        valid = False
        while not valid:
            start_index = python_input("Select Backup: ")
            try:
                start_id = snap_list[int(start_index)][0]
            except (IndexError, TypeError, ValueError) as e:
                print("Invalid Index: " + str(e))
                continue
            valid = True
    valid = False
    print("Backup: " + snap_list[int(start_index)][1] + " [" + start_id + "]")
    if not latest and not date:
        go_s = python_input("Is this correct? (y/n): ")
        if not go_s.startswith('Y') and not go_s.startswith('y'):
            exit (0)
    current_index = int(start_index)
    print("Scanning for Sample Files...")
    threading.Thread(name='root_walk', target=walk_tree, args=(rubrik, snap_list[current_index][0], delim, initial_path, {})).start()
    print("Waiting for jobs to queue")
    time.sleep(20)
    print("PPQ: " + str(job_queue.empty()) + '// AC: ' + str(threading.activeCount()))
    first = True
    while first or not job_queue.empty() or threading.activeCount() > 1:
        first = False
        if threading.activeCount()-1 < max_threads and not job_queue.empty():
            job = job_queue.get()
            print("\nQueue: " + str(job_queue.qsize()))
            print("Running Threads: " + str(threading.activeCount()-1))
            job.start()
        elif not job_queue.empty():
            time.sleep(10)
            print("\nQueue: " + str(job_queue.qsize()))
            print("Running Threads: " + str(threading.activeCount()-1))
        else:
            print("\nWaiting on " + str(threading.activeCount()-1) + " jobs to finish.")
            time.sleep(10)
        print("PQ: " + str(job_queue.empty()) + '// AC: ' + str(threading.activeCount()))
#    print(dir_list)
#    print(empty_dir)
    dprint(file_samples)
    print("Selecting Files....")
    files_selected = []
    selected_size = 0
    done = False
    while not done:
        pick = randrange(len(file_samples))
        if max_size:
            if selected_size + file_samples[pick][1] > max_size:
                done = True
        if file_samples[pick][0] in files_selected:
            continue
        files_selected.append(file_samples[pick][0])
        selected_size += file_samples[pick][1]
        if len(files_selected) == num_files:
            done = True
    print("Selected " + str(len(files_selected)) + " files totaling " + get_size_from_bytes(selected_size))




