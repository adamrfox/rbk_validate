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
import json
urllib3.disable_warnings()
import threading
try:
    import queue
except ImportError:
    import Queue as queue
from random import randrange
from pprint import pprint
from os.path import exists as file_exists
import string

class AtomicCounter:

    def __init__(self, initial=0):
        """Initialize a new atomic counter to given initial value (default 0)."""
        self.value = initial
        self._lock = threading.Lock()

    def increment(self, num=1):
        """Atomically increment the counter by num (default 1) and return the
        new value.
        """
        with self._lock:
            self.value += num
            return self.value

def usage():
    sys.stderr.write("Usage: rbk_validate.py [-hDvl] [-c creds] [-t token] [-b backup] [-f fileset] [-d date] [-m max_threads [-M thread_factor] [-s size] [-n num_files] [-j job_types] [-S sampling] [-x excludes] restore_location rubrik\n")
    sys.stderr.write("-h | --help : Prints this message\n")
    sys.stderr.write("-D | --DEBUG : DEBUG mode.  Used for troubleshooting\n")
    sys.stderr.write("-v | --verbose : Verbose output\n")
    sys.stderr.write("-l | --latest : Use the latest backup\n")
    sys.stderr.write("-c | --creds : Rubrik Credentials\n")
    sys.stderr.write("-t | --token : Rubrik API token\n")
    sys.stderr.write("-b | --backup : Backup to use.  host:share for NAS, host for physical\n")
    sys.stderr.write("-f | --fileset : Specify a fileset name\n")
    sys.stderr.write("-d | --date : Specify a backup by date\n")
    sys.stderr.write("-m | --max_threads : Maximum number of threads.  [Def: 10x the number of nodes]\n")
    sys.stderr.write("-M | --thread_factor : Specify threads per node [Def: 10]\n")
    sys.stderr.write("-s | --size : Total space for restored files [Def: None]\n")
    sys.stderr.write("-n | --num_files : Total number of files (raw number or %) [Def: 10 files]\n")
    sys.stderr.write("-j | --jobs : Specify which jobs to run [restore, patch].  [Def: restore]\n")
    sys.stderr.write("-S | --sampling : Specify the type of sampling [default, none, max, #] [Def: default]\n")
    sys.stderr.write("-x | --exclude : Specify a list of paths to exclude (comma separated)\n")
    sys.stderr.write("restore_location : Specify where to write restored files (host:share:path | host:path)\n")
    sys.stderr.write("rubrik : Name or IP of a Rubrik node\n\n")d
    exit(0)


def dprint(message):
    if DEBUG:
        dfh = open(debug_log, 'a')
        dfh.write(message + "\n")
        dfh.close()
    return ()

def vprint(message):
    if VERBOSE:
        print(message)

def python_input(message):
    if int(sys.version[0]) > 2:
        val = input(message)
    else:
        val = raw_input(message)
    return (val)

def get_bytes(size, unit):
    if unit in ('g', 'G'):
        size = size * 1024 * 1024 * 1024
    elif unit in ('t', 'T'):
        size = size * 1024 * 1024 * 1024 * 1024
    elif unit in ('m', 'M'):
        size = size * 1024 * 1024
    else:
        sys.stderr.write("Acceptable units are 'M', 'G' or 'T' case insensitive\n")
        sys.exit(1)
    return (size)

def get_size_from_bytes(bytes):
    if bytes >= 1024 * 1024 * 1024 * 1024:
        size = str(int(bytes / 1024 / 1024 / 1024 / 1024)) + " TB"
    elif bytes >= 1024 * 1024 * 1024:
        size = str(int(bytes / 1024 / 1024 / 1024)) + " GB"
    elif bytes >= 1024 * 1024:
        size = str(int(bytes / 1024 / 1024)) + " MB"
    else:
        size = str(bytes) + " Bytes"
    return (size)

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
    return (node_list)

def get_sample(file_list, count):
    samples = []
    for i in range(count):
        sample = randrange(len(file_list))
        samples.append(file_list[sample])
        i += 1
    return(samples)

def check_root_vm_path(job_ptr, id, path):
    vp_browse = rubrik_cluster[job_ptr]['session'].get('internal', '/browse?path=' + path + "&offset=0&snapshot_id=" + str(id),
                                                       timeout=timeout)
#    print("VP_BROWSE: " + path)
#    if path == "/boot":
#        pprint(vp_browse)
    for be in vp_browse['data']:
        if not be['filename'][0] in string.punctuation:
            file_check = path + "/" + be['filename']
#            print("RCHECKING " + file_check)
            f_cur = ""
            f_check_done = False
            while not f_check_done:
#                print("F_CHECK_OUTER")
                if f_cur:
                    f_search = rubrik_cluster[job_ptr]['session'].get('internal', '/search?managed_id=' + vm_id + "&query_string="
                                                              + file_check + "&cursor=" + f_cur, timeout=timeout)
                else:
                    f_search = rubrik_cluster[job_ptr]['session'].get('internal',
                                                                      '/search?managed_id=' + vm_id + "&query_string="
                                                                      + file_check, timeout=timeout)
#                pprint(f_search)
                for sr in f_search['data']:
#                    print("SRP: " + sr['path'] + " // " + path)
#                    print("SRF: " + sr['filename'] + " // " + be['filename'])
                    if sr['path'] == path + '/' + be['filename']:
#                        print('RFOUND ' + sr['path'] + '/' + sr['filename'])
                        for v in sr['fileVersions']:
                            if v['snapshotId'] == id:
#                                print("RPATH: " + path + " :: True")
#                                pprint(sr)
#                                pprint(v)
#                                time.sleep(10)
                                print("RPATH: True")
                                return (True)
                if f_search['hasMore']:
                    f_cur = check_dir['nextCursor']
                else:
#                    print("RPATH2 FALSE: " + path)
                    return(False)

        else:
            continue
#    print("RPATH: " + path + " :: False")
    return(False)

def check_vm_path(job_ptr, id, path):
    pf = path.split('/')
    file_name = pf.pop(-1)
    path_name = '/'.join(pf)
    path_name += "/" + file_name
    check_done = False
    cursor = ""
    while not check_done:
        if len(pf) == 1:
#            print("CALLING CRVP: " + path + '/' + path_name)
            return(check_root_vm_path(job_ptr, id, path))
        if cursor:
            check_dir = rubrik_cluster[job_ptr]['session'].get('internal', '/search?managed_id=' + vm_id + '&query_string=' + path_name +
                                                           '&cursor=' + cursor, timeout=timeout)
        else:
            check_dir = rubrik_cluster[job_ptr]['session'].get('internal', '/search?managed_id=' + vm_id + '&query_string=' + path_name, timeout=timeout)
        for d in check_dir['data']:
            if d['path'] == path_name and d['filename'] == file_name:
#                print('FOUND ' + d['path'] + '/' + d['filename'])
                for v in d['fileVersions']:
                    if v['snapshotId'] == id and v['fileMode'] == "directory":
#                        pprint(d)
#                        pprint(v)
#                        time.sleep(10)
                        return(True)
        if check_dir['hasMore']:
            cursor = check_dir['nextCursor']
        else:
            check_done = True
    return(False)

def walk_tree(rubrik, id, delim, path, parent, exclude_paths):
    offset = 0
    done = False
    file_count = 0
    file_list = []
    sampling_mult = 1
    if SAMPLING.isdigit():
        sampling_mult = int(SAMPLING)
    if path.startswith(tuple(exclude_paths)):
        return
    while not done:
        job_ptr = randrange(len(rubrik_cluster))
        params = {"path": path, "offset": offset}
        if offset == 0:
            if VERBOSE:
                print("Starting job " + path + " on " + rubrik_cluster[job_ptr]['name'])
            else:
                print(' . ', end='')
        if not NAS:
            rbk_walk = rubrik_cluster[job_ptr]['session'].get('v1', '/fileset/snapshot/' + str(id) + '/browse',
                                                          params=params, timeout=timeout)
        else:
            rbk_walk = rubrik_cluster[job_ptr]['session'].get('internal', '/browse?path=' + path + '&offset=' +
                                                              str(offset) + "&snapshot_id=" + str(id))
        file_count = 0
        for dir_ent in rbk_walk['data']:
            offset += 1
            if dir_ent == parent:
                return
            if dir_ent['fileMode'] == "file":
                file_count += 1
                if path == delim:
                    file_list.append((path + dir_ent['filename'], dir_ent['size']))
                else:
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
                if VMWARE:
                    if not check_root_vm_path(job_ptr, id, new_path):
                        exclude_paths.append(new_path)
                        dprint("EXCLUDING: " + new_path)
                        continue
                job_queue.put(
                    threading.Thread(name=new_path, target=walk_tree, args=(rubrik, id, delim, new_path, dir_ent, exclude_paths)))
        if not rbk_walk['hasMore']:
            done = True
        dprint("DIR: " + path + " // FILES: " + str(file_count))
        total_file_count.increment(file_count)
        if SAMPLING != "none":
            if file_count > 0 :
                sample_list = []
                if SAMPLING == "max" or file_count <= 10:
                    sample_list = get_sample(file_list, 1)
                elif file_count < 50:
                    sample_list = get_sample(file_list, 2 * sampling_mult)
                elif file_count < 100:
                    sample_list = get_sample(file_list, 5 * sampling_mult)
                else:
                    sample_list = get_sample(file_list, 10 * sampling_mult)
                for f in sample_list:
                    file_samples.append(f)
            else:
                empty_dir.append(path)
        else:
            for f in file_list:
                file_samples.append(f)

def get_folder_path(v):
    vfp = []
    for fp in v['folderPath']:
        if fp['name'] == "vm":
            continue
        vfp.append(fp['name'])
    return(vfp)

def find_vm_with_path(vm_data):
    v_id = ""
    delim = ""
    for v in vm_data['data']:
        vm_folder_path = get_folder_path(v)
        dprint("VP: " + str(vm_folder_path) + " // HF: " + str(hf))
        if vm_folder_path == hf:
            v_id = v['id']
            if 'Windows' in v['guestOsName']:
                delim = "\\"
            else:
                delim = "/"
            break
    return(v_id, delim)

def run_patch_verify(rubrik, object, snap):
    payload = {'objectId': object, 'snapshotIdsOpt': [snap]}
    dprint("VERIFY_PAYLOAD:")
    dprint(str(payload))
    verify_job = rubrik.post('v1', '/backup/verify', payload, timeout=timeout)
    dprint("VERIFY:")
    dprint(str(verify_job))
    done = False
    first = True
    print("Patch Validation:")
    while not done:
        restore_job_status = rubrik.get('v1', '/backup/verify/' + verify_job['id'], timeout=timeout)
        job_status = restore_job_status['status']
        if job_status in ['RUNNING', 'QUEUED', 'ACQUIRING', 'FINISHING']:
            progress = int(restore_job_status['progress'])
            if first:
                print("Progress: " + str(progress) + "%", end='')
                sys.stdout.flush()
                first = False
            else:
                for x in range(0, digits + 1):
                    print('\b', end='')
                print(str(progress) + "%", end='')
                sys.stdout.flush()
            if progress < 10:
                digits = 1
            elif progress < 100:
                digits = 2
            else:
                digits = 3
            time.sleep(5)
        elif job_status in ['SUCCEEDED', 'FAILED']:
            return(job_status)
        elif job_status == "TO_CANCEL" or 'endTime' in job_status:
            sys.stderr.write("\nJob ended with status: " + job_status + "\n")
            exit(1)
        else:
            return(job_status)

def debug_restore(payload, id):
    deb_res_payload = {}
    if not VMWARE:
        if NAS:
            deb_res_payload['shareId'] = restore_share_id
            deb_res_payload['hostId'] = restore_host_id
        else:
            deb_res_payload['hostId'] = restore_host_id
    else:
        if host != restore_host:
            deb_res_payload['destObjId'] = restore_host_id
        deb_res_payload['username'] = ""
        deb_res_payload['password'] = ""
        deb_res_payload['domainName'] = ""
        deb_res_payload['shouldSaveCredentials'] = False
        deb_res_payload['shouldUseAgent'] = True
    payload['ignoreErrors'] = False
    for fr in payload['restoreConfig']:
        if host == restore_host or VMWARE:
            deb_res_payload['restoreConfig'] = [{'path': fr['path'], 'restorePath': fr['restorePath']}]
        elif not VMWARE:
            deb_res_payload['exportPathPairs'] = [{'path': fr['path'], 'restorePath': fr['restorePath']}]
        dprint("DEB_RES: " + str(deb_res_payload))
        print("ID=" + str(id))
        print("Restoring " + fr['path'])
        if host == restore_host:
            if not VMWARE:
                first_path = fr['path']
                deb_restore = rubrik.post('internal', '/fileset/snapshot/' + str(id) + '/restore_files',
                                             deb_res_payload, timeout=timeout)
            else:
                uri = "/vmware/vm/snapshot/" + str(id) + "/restore_files"
                deb_restore = rubrik.post('internal', uri,
                                             deb_res_payload, timeout=timeout)
        else:
            if not VMWARE:
                first_path = fr['path']
                deb_restore = rubrik.post('internal', "/fileset/snapshot/" + str(id) + "/export_files",
                                             deb_res_payload, timeout=timeout)
            else:
                deb_restore = rubrik.post('internal', '/vmware/vm/snapshot/' + str(id) + '/restore_files',
                                             deb_res_payload, timeout=timeout)
        job_status_url = str(deb_restore['links'][0]['href']).split('/')
        job_status_path = "/" + "/".join(job_status_url[5:])
        done = False
        first = True
        while not done:
            restore_job_status = rubrik.get('v1', job_status_path)
            job_status = restore_job_status['status']
            if job_status in ['RUNNING', 'QUEUED', 'ACQUIRING', 'FINISHING']:
                progress = int(restore_job_status['progress'])
                if first:
                    print("Progress: " + str(progress) + "%", end='')
                    sys.stdout.flush()
                    first = False
                else:
                    for x in range(0, digits + 1):
                        print('\b', end='')
                    print(str(progress) + "%", end='')
                    sys.stdout.flush()
                if progress < 10:
                    digits = 1
                elif progress < 100:
                    digits = 2
                else:
                    digits = 3
                time.sleep(5)
            elif job_status in ['SUCCEEDED', 'FAILED']:
                print("\nDone")
                done = True
            elif job_status == "TO_CANCEL" or 'endTime' in job_status:
                sys.stderr.write("\nJob ended with status: " + job_status + "\n")
                exit(1)
            else:
                print("\nStatus: " + job_status)
        if not VMWARE:
            object_ids = share_id + ',' + fs_id
        else:
            object_ids = vm_id
        events = rubrik.get('v1', '/event/latest?limit=50&event_type=Recovery&object_ids=' + str(object_ids),
                            timeout=timeout)
        ev_series_id = ""
        ev_status = ""
        ev_reason = ""
        if not VMWARE:
            dprint("FIRST_PATH: " + first_path)
            for ev in events['data']:
                ev_data = ev['latestEvent']['eventInfo']
                ev_message = json.loads(ev_data)
                ev_s = ev_message['message']
                evf = ev_s.split("'")
                ev_path = evf[1]
                if ev_path == first_path:
                    ev_status = ev['latestEvent']['eventStatus']
                    try:
                        ev_reason = ev_message['cause']['reason']
                    except KeyError:
                        pass
                    break
        else:
            ev_status = events['data'][0]['latestEvent']['eventStatus']
            try:
                ev_reason = events['data'][0]['cause']['reason']
            except KeyError:
                pass
        print("RESTORE VALIDATION: " + ev_status, end='')
        if ev_reason:
            print(" : " + ev_reason)
        else:
            print('\n')
        time.sleep(15)
    exit(0)

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
    restore_share = ""
    restore_host = ""
    restore_path = ""
    initial_path = ""
    payload = {}
    token = ""
    DEBUG = False
    VERBOSE = False
    NAS = False
    VMWARE = False
    SAMPLING = "default"
    VALIDATION = "restore"
    INJECT_FAILURE = False
    timeout = 600
    rubrik_cluster = []
    job_queue = queue.Queue()
    dir_list = []
    empty_dir = []
    file_samples = []
    max_threads = 0
    thread_factor = 10
    max_size = 0
    num_files = "10"
    total_file_count = AtomicCounter()
    debug_log = "debug_log.txt"
    vm_folder_path = []
    vm_id = ""
    exclude_paths = []
    first_path = ""
    jobs_types_to_run = ['restore']

    optlist, args = getopt.getopt(sys.argv[1:], 'hDvlc:t:b:f:d:m:M:s:n:S:Fx:j:',
                                  ['--help', '--DEBUG', '--verbose', '--latest',
                                   '--creds=', '--token=', '--backup=', '--fileset=',
                                   '--date=', '--max_threads=', '--thread_factor=',
                                   '--size=', '--number_files=', '--sampling=', '--inject_failure',
                                   '--exclude=','--jobs='])
    for opt, a in optlist:
        if opt in ('-h', '--help'):
            usage()
        if opt in ('-D', '--DEBUG'):
            DEBUG = True
            VERBOSE = True
            dfh = open(debug_log, "w")
            dfh.close()
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
            num_files = a
        if opt in ('-S', '--sampling'):
            SAMPLING = a.lower()
            if SAMPLING not in ('default', 'none', 'max') and not SAMPLING.isdigit():
                sys.stderr.write("Valid sampling values: 'default', 'none', 'max' or a numerical multiplier\n")
                exit(1)
        if opt in ('-F', '--inject_failure'):
            INJECT_FAILURE = True
        if opt in ('-x', '--exclude'):
            exclude_paths = a.split(',')
        if opt in ('-j', '--jobs'):
            jobs_types_to_run = []
            jt_list = a.split(',')
            for j in jt_list:
                if j in ['patch', 'restore']:
                    jobs_types_to_run.append(j)
                else:
                    sys.stderr.write("Job types are 'restore' and 'patch'\n")
                    exit(2)
    try:
        (restore_location, rubrik_node) = args
    except:
        usage()
    if not backup:
        backup = python_input("Backup (host:share | host): ")
    if ':' in backup:
        NAS = True
    if NAS:
        (host, share) = backup.split(':')
        try:
            (restore_host, restore_share, restore_path) = restore_location.split(':')
        except ValueError:
            (restore_host, restore_share) = restore_location.split(':')
    else:
        host = backup
        try:
            (restore_host, restore_path) = restore_location.split(';')
        except ValueError:
            restore_host = restore_location
    if not fileset:
        VMWARE = True
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
        if restore_path and not restore_path.startswith(delim):
            restore_path = delim + restore_path
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
            if x['hostname'] == restore_host and x['exportPoint'] == restore_share:
                restore_share_id = x['id']
            if share_id and restore_share_id:
                break
        if share_id == "":
            sys.stderr.write("Share not found\n")
            exit(2)
        if restore_share_id == "":
            sys.stderr.write("Restore share not found\n")
            exit(2)
        fs_data = rubrik.get('v1', str("/fileset?share_id=" + share_id + "&name=" + fileset), timeout=timeout)
    elif not VMWARE:
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
        hs_data = rubrik.get('v1', '/host?name=' + restore_host, timeout=timeout)
        restore_share_id = str(hs_data['data'][0]['id'])
        if restore_share_id == "":
            sys.stderr.write("Restore host not found\n")
            exit(2)
        restore_os_type = str(hs_data['data'][0]['operatingSystemType'])
        if os_type != restore_os_type:
            sys.stderr.write("OS Type Mismatch\n")
            exit(3)
        fs_data = rubrik.get('v1', '/fileset?host_id=' + share_id, timeout=timeout)
    else:
        vm_has_path = False
        if '/' in host:
            hf = host.split('/')
            host = hf.pop()
            vm_has_path = True
        dprint("VM_HOST = " + host)
        vm_data = rubrik.get('v1', '/vmware/vm?name=' + host, timeout=timeout)
        if vm_data['total'] == 0:
            sys.stderr.write('VM not found\n')
            exit(2)
        if vm_data['total'] > 1:
            if vm_has_path:
                (vm_id, delim) = find_vm_with_path(vm_data)
                if not vm_id:
                    sys.stderr.write("VM host found.\n")
                    exit(2)
            else:
                sys.stderr.write("Multiple VMs found.  Use a folder path to specify the desired VM\n")
                exit(2)
        else:
            if vm_has_path:
                (vm_id, delim) = find_vm_with_path(vm_data)
            else:
                vm_id = vm_data['data'][0]['id']
                if 'Windows' in vm_data['data'][0]['guestOsName']:
                    delim = '\\'
                else:
                    delim = '/'
        initial_path = '/'
        dprint("VM_ID: " + vm_id)
    if not VMWARE:
        h_data = rubrik.get('v1', '/host?name=' + restore_host, timeout=timeout)
        for h in h_data['data']:
            if h['name'] == restore_host:
                restore_host_id = h['id']
                break
    else:
        vm_has_path = False
        if '/' in host:
            hf = host.split('/')
            host = hf.pop()
            vm_has_path = True
        dprint("VM_RES_HOST = " + host)
        vm_data = rubrik.get('v1', '/vmware/vm?name=' + restore_host, timeout=timeout)
        if vm_data['total'] == 0:
            sys.stderr.write('Restore VM not found\n')
            exit(2)
        if vm_data['total'] > 1:
            if vm_has_path:
                vm_id = find_vm_with_path(vm_data)
                if not vm_id:
                    sys.stderr.write("Restore VM host found.\n")
                    exit(2)
            else:
                sys.stderr.write("Multiple Restore VMs found.  Use a folder path to specify the desired VM\n")
                exit(2)
        else:
            if vm_has_path:
                restore_host_id = find_vm_with_path(vm_data)
            else:
                restore_host_id = vm_data['data'][0]['id']
    if not restore_host_id:
        sys.stderr.write("Can't find Restore Host ID\n")
        exit(2)
    if not VMWARE:
        fs_id = ""
        for fs in fs_data['data']:
            if fs['name'] == fileset:
                fs_id = fs['id']
                break
        dprint(fs_id)
        snap_data = rubrik.get('v1', str("/fileset/" + fs_id), timeout=timeout)
#        pprint(snap_data)
    else:
        snap_data = rubrik.get('v1', "/vmware/vm/" + str(vm_id), timeout=timeout)
#        pprint(snap_data)
    for snap in snap_data['snapshots']:
        if VMWARE and snap['indexState'] == 0:
            continue
        s_time = snap['date']
        s_id = snap['id']
        s_time = s_time[:-5]
        snap_dt = datetime.datetime.strptime(s_time, '%Y-%m-%dT%H:%M:%S')
        snap_dt = pytz.utc.localize(snap_dt).astimezone(local_zone)
        snap_dt_s = snap_dt.strftime('%Y-%m-%d %H:%M:%S')
        snap_list.append((s_id, snap_dt_s))
    if latest:
#        start_index = len(snap_list) - 1
#        start_id = snap_list[-1][0]
        for a, snap in reversed(list(enumerate(snap_data['snapshots']))):
            if snap['indexState'] == 0:
                continue
            start_index = a
            start_id = snap_list[a][0]
            break
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
            exit(0)
    current_index = int(start_index)
    if 'patch' in jobs_types_to_run:
        if not VMWARE:
            patch_job = run_patch_verify(rubrik, fs_id, snap_list[current_index][0])
        else:
            patch_job = run_patch_verify(rubrik, vm_id, snap_list[current_index][0])
        if patch_job != "SUCCEEDED":
            sys.stderr.write("\nPatch Verification Failed: " + status + "\n")
            exit(5)
        print("\nPatch Verification Passed!")
    if not 'restore' in jobs_types_to_run:
        exit(0)
    dprint("SNAP_ID: " + str(snap_list[current_index][0]))
    print("Scanning for Sample Files...")
    threading.Thread(name='root_walk', target=walk_tree,
                     args=(rubrik, snap_list[current_index][0], delim, initial_path, {}, exclude_paths)).start()
    print("Waiting for jobs to queue")
    time.sleep(20)
    dprint("PPQ: " + str(job_queue.empty()) + '// AC: ' + str(threading.activeCount()))
    first = True
    while first or not job_queue.empty() or threading.activeCount() > 1:
        first = False
        if threading.activeCount() - 1 < max_threads and not job_queue.empty():
            job = job_queue.get()
            vprint("\nQueue: " + str(job_queue.qsize()))
            vprint("Running Threads: " + str(threading.activeCount() - 1))
            job.start()
        elif not job_queue.empty():
            time.sleep(10)
            print("\nQueue: " + str(job_queue.qsize()))
            print("Running Threads: " + str(threading.activeCount() - 1))
        else:
            print("\nWaiting on " + str(threading.activeCount() - 1) + " jobs to finish.")
            if DEBUG and threading.activeCount() < 35:
                dprint(str(threading.activeCount()) + " jobs:")
                for thread in threading.enumerate():
                    dprint(thread.name)
            time.sleep(10)
        dprint("PQ: " + str(job_queue.empty()) + '// AC: ' + str(threading.activeCount()))
    #    print(dir_list)
    #    print(empty_dir)
    dprint(str(file_samples))
    print("Selecting Files....")
    files_selected = []
    selected_size = 0
    done = False
    if num_files.endswith('%'):
        factor = int(num_files[:-1]) / 100
        max_files = int(total_file_count.value * factor)
    else:
        max_files = int(num_files)
    if max_files > len(file_samples):
        print("Requested number of files larger than file sample.  Restoring " + str(len(file_samples)) + " files.")
        max_files = len(file_samples)
    picked_list = []
    while not done:
        pick = randrange(len(file_samples))
        if pick in picked_list:
            continue
        if max_size:
            if selected_size + file_samples[pick][1] > max_size:
                done = True
        if duplicate_file(file_samples[pick][0], files_selected):
            continue
        fpf = file_samples[pick][0].split(delim)
        fpf.pop(-1)
        rp = delim.join(fpf)
        if not rp:
            rp = delim
        if host == restore_host or VMWARE:
            files_selected.append({'path': str(file_samples[pick][0]), 'restorePath': str(restore_path) + str(rp)})
        else:
            files_selected.append({'srcPath': str(file_samples[pick][0]), 'dstPath': str(restore_path) + str(rp)})
        selected_size += file_samples[pick][1]
        if len(files_selected) == max_files:
            dprint("LEN FS:" + str(len(files_selected)))
            done = True
    if INJECT_FAILURE:
        if not VMWARE:
            if host == restore_host:
                files_selected.append({'path': '/foo/bar/baz', 'restorePath': '/restore/foo/bar'})
            else:
                files_selected.append({'srcPath': '/foo/bar/baz', 'dstPath': '/restore/foo/bar'})
        else:
            if delim == "\\":
                files_selected.append({'path': 'C:\\foo\\bar\\baz', 'restorePath': 'C:\\restore\\foo\\bar'})
            else:
                files_selected.append({'path': '/foo/bar/baz', 'restorePath': '/restore/foo/bar'})

    print("Selected " + str(len(files_selected)) + " files totaling " + get_size_from_bytes(selected_size))
    if host == restore_host or VMWARE:
        payload['restoreConfig'] = files_selected
    elif not VMWARE:
        payload['exportPathPairs'] = files_selected
        if NAS:
            payload['shareId'] = restore_share_id
            payload['hostId'] = restore_host_id
        else:
            payload['hostId'] = restore_host_id
    if VMWARE:
        if host != restore_host:
            payload['destObjId'] = restore_host_id
        payload['username'] = ""
        payload['password'] = ""
        payload['domainName'] = ""
        payload['shouldSaveCredentials'] = False
        payload['shouldUseAgent'] = True
    payload['ignoreErrors'] = False
    dprint("RESTORE PAYLOAD:")
    dprint(str(payload))
    print("Restoring files....")
#    if DEBUG:
#        debug_restore(payload, start_id)
    if host == restore_host:
        if not VMWARE:
            first_path = files_selected[0]['path']
            rubrik_restore = rubrik.post('internal', '/fileset/snapshot/' + str(start_id) + '/restore_files',
                                     payload, timeout=timeout)
        else:
            uri = "/vmware/vm/snapshot/" + str(start_id) + "/restore_files"
            rubrik_restore = rubrik.post('internal', uri,
                                         payload, timeout=timeout)
    else:
        if not VMWARE:
            first_path = files_selected[0]['srcPath']
            rubrik_restore = rubrik.post('internal', "/fileset/snapshot/" + str(start_id) + "/export_files",
                                     payload, timeout=timeout)
        else:
            rubrik_restore = rubrik.post('internal', '/vmware/vm/snapshot/'+ str(start_id) + '/restore_files',
                                         payload, timeout=timeout)
    dprint("RESTORE:")
    dprint(str(rubrik_restore))
    job_status_url = str(rubrik_restore['links'][0]['href']).split('/')
    job_status_path = "/" + "/".join(job_status_url[5:])
    done = False
    first = True
    while not done:
        restore_job_status = rubrik.get('v1', job_status_path)
        job_status = restore_job_status['status']
        if job_status in ['RUNNING', 'QUEUED', 'ACQUIRING', 'FINISHING']:
            progress = int(restore_job_status['progress'])
            if first:
                print("Progress: " + str(progress) + "%", end='')
                sys.stdout.flush()
                first = False
            else:
                for x in range(0,digits+1):
                    print('\b', end='')
                print(str(progress) + "%", end='')
                sys.stdout.flush()
            if progress < 10:
                digits = 1
            elif progress < 100:
                digits = 2
            else:
                digits = 3
            time.sleep(5)
        elif job_status in ['SUCCEEDED', 'FAILED', 'CANCELED']:
            print("\nDone")
            done = True
        elif job_status == "TO_CANCEL" or 'endTime' in job_status:
            sys.stderr.write("\nJob ended with status: " + job_status + "\n")
            exit(1)
        else:
            print("\nStatus: " + job_status)
    if not VMWARE:
        object_ids = share_id + ',' + fs_id
    else:
        object_ids = vm_id
    events = rubrik.get('v1', '/event/latest?limit=50&event_type=Recovery&object_ids='+ str(object_ids), timeout=timeout)
    ev_series_id = ""
    dprint("FIRST FILE: " + first_path)
    ev_status = ""
    ev_reason = ""
    if not VMWARE:
        for ev in events['data']:
            ev_data = ev['latestEvent']['eventInfo']
            ev_message = json.loads(ev_data)
            ev_s = ev_message['message']
            evf = ev_s.split("'")
            ev_path = evf[1]
            if ev_path == first_path:
                ev_status = ev['latestEvent']['eventStatus']
                try:
                    ev_reason = ev_message['cause']['reason']
                except KeyError:
                    pass
                break
    else:
        ev_status = events['data'][0]['latestEvent']['eventStatus']
        try:
            ev_reason = events['data'][0]['cause']['reason']
        except KeyError:
            pass
    print("RESTORE VALIDATION: " + ev_status, end='')
    if ev_reason:
        print(" : " + ev_reason)
    else:
        print('\n')
    if ev_status == "Failure":
        for f, p in enumerate(payload['restoreConfig']):
            if not file_exists(p['restorePath']):
                print(str(f) + p['path'] + " not found")

##TODO VMware Files
##TODO Non-Verbose Output for Scan




