"""SeedBox Download
Downloads all files from a remote SFTP host from a given directory, then removes those files from
the remote.

SCRIPT RULES
 - Never compromise the local systems files.
 - Never remove remote files, unless download is complete and verified

This script is designed to be run on a regular cron cadence. It handles potential overlapping
processes (IE cron evrey 5 minutes, but active downloads exceed that). It does this by checking a
redis server to know if an instance of the script is already "running", via a lock var.
If the lock var is found, the script assumes an instances is currently running and will stop
execution of any download functions. However some notification process will STILL RUN.

Requirements:
    arrow==0.16.0
    pysftp==0.2.9
    redis==3.5.3
    slackclient==2.9.
    pyyaml-5.3.1

Redis Keys
 - ftp-boi-lock
 - ftp-boi-lock-date
 - ftp-boi-last-message-ts
 - ftp-boi-last-message-content

Features to build
    - add config for local redis
    - determine local free space BETTER and STOP DL if it exceeds that
    - ALLOW CLI args such as --delete to remove lock key from redis
    -  Delete from remote  recursively, or some what
    - download files in remote dir that are NOT folders, ie a reademe.txt
    - check download size
        - make sure this doesnt exceed local vol

"""
import argparse
from datetime import timedelta
import os
import shutil
import subprocess

import arrow
import pysftp
import redis
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import yaml


class SeedboxDownload:

    def __init__(self, args):
        self.args = args
        self.downloads = []
        self.to_download = []
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.notifications = []
        self.move_after_download = False

    def run(self, args):
        print("\n[%s] Starting SeedBox Download" % arrow.now())
        self.read_config()
        if self.args.delete:
            delete = self.handle_delete()
            exit(0)

        run_prepped = self.prep_run()
        if not self.check_lock() and run_prepped:
            self.create_lock()
            if self.prep_downloads():
                self.run_downloads()
            self.delete_lock()
            # self.cleanup()
        self.report()
        print('[%s] Finished' % arrow.now())

    def read_config(self):
        """Read a config yaml file, and use those values.
           By default we assume the script exists in the same dir as this script does named simply
           ./config.yaml

           @todo: CLI support
           @note: Future  versions will  allow this to be supplemented via CLI args.
        """
        print("[%s] DEBUG - Using config file: %s" % (arrow.now(), self.args.config))

        if not os.path.exists(self.args.config):
            print("ERROR: cannot find config file: %s" % self.args.config)
            exit()

        with open(self.args.config) as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            config_yaml = yaml.load(file, Loader=yaml.FullLoader)

        if 'local' not in config_yaml:
            return _config_error('local')

        # Locals config
        if config_yaml['local']['downloadPath']:
            self.local_path = config_yaml['local']['downloadPath']
        if config_yaml['local']['downloadTmpPath']:
            self.local_path_tmp = config_yaml['local']['downloadTmpPath']
            self.move_after_download = True
        if config_yaml['local']['lockAgeAlertMin']:
            self.lock_age_alert_min = config_yaml['local']['lockAgeAlertMin']
        if 'instanceName' in config_yaml['local']:
            self.redis_key_base = config_yaml['local']['instanceName']

        self.free_space_required = None
        if 'freeSpaceRequired' in config_yaml['local']:
            self.free_space_required = config_yaml['local']['freeSpaceRequired']

        # Remote config
        if config_yaml['sftp']['host']:
            self.sftp_host = config_yaml['sftp']['host']
        if config_yaml['sftp']['user']:
            self.sftp_user = config_yaml['sftp']['user']
        if config_yaml['sftp']['pass']:
            self.sftp_pass = config_yaml['sftp']['pass']
        if config_yaml['sftp']['path']:
            self.sftp_path = config_yaml['sftp']['path']
        if config_yaml['sftp']['removeDownloadedFiles']:
            self.sftp_remove_files = config_yaml['sftp']['removeDownloadedFiles']

        # Notification config
        if 'enabled' in config_yaml['notifications']:
            self.notifications_enabled = config_yaml['notifications']['enabled']

            if self.notifications_enabled:
                self.slack_enabled = config_yaml['notifications']['slack']['enabled']
                if self.slack_enabled:
                    self.slack_token = config_yaml['notifications']['slack']['token']
                    self.slack_channel = config_yaml['notifications']['slack']['channel']
                    self.slack_user = config_yaml['notifications']['slack']['user']
                    self.slack_repeat_message_min = config_yaml['notifications']['slack']['repeatMsgIntervalMin']
        return True

    def handle_delete(self) -> bool:
        """ """
        lock_name = "%s-lock" % self.redis_key_base
        self.redis.delete(lock_name)
        print("[%s] Deleted lock: %s" % (arrow.now(), lock_name))
        return True

    def prep_run(self) -> bool:
        """ """

        # Check local download path exists and will work
        if not os.path.exists(self.local_path):
            print('[%s] ERROR - Local download path does not exist: "%s"' % (
                arrow.now(),
                self.local_path))
            return False

        if self.move_after_download:
            if not os.path.exists(self.local_path_tmp):
                print('[%s] ERROR - Local download tmp path does not exist: "%s"' % (
                    arrow.now(),
                    self.local_path_tmp))
                return False

        if self.free_space_required:
            free_gigs = self._get_free_local_gigs(self.local_path)
            print('[%s] Free local space:\t\t%s gigs' % (arrow.now(), free_gigs))
            if free_gigs < self.free_space_required:
                msg = "Not enough free space on device"
                print('[%s] ERROR - %s' % (
                    arrow.now(),
                    msg))
                self.notifications.append({
                    "type": "error",
                    "message": msg,
                })
                return False

        print("[%s] Remote Host:\t\t\t%s" % (arrow.now(), self.sftp_host))
        print("[%s] Remote Path:\t\t\t%s" % (arrow.now(), self.sftp_path))
        if self.sftp_remove_files:
            remote_delete_msg = "YES"
        else:
            remote_delete_msg = "NO"
        print("[%s] Remote Delete Files:\t\t%s" % (arrow.now(), remote_delete_msg))

        if not self.move_after_download:
            print("[%s] Local Path:\t\t%s" % (arrow.now(), self.local_path))
        else:
            print("[%s] Local Temp Path:\t\t%s" % (arrow.now(), self.local_path_tmp))
            print("[%s] Local Completed Path:\t%s" % (arrow.now(), self.local_path))
        # @todo: figure this out
        # print("[%s] Local Free Space:\t%s(someting)" % (arrow.now(), 'x'))
        print("[%s] Redis Lock Name:\t\t%s-lock" % (arrow.now(), self.redis_key_base))
        print("\n")

        # print("[%s] Remote Host:]\t%s" % (arrow.now()))
        return True

    def _get_free_local_gigs(self, path) -> float:
        statvfs = os.statvfs(self.local_path)
        free = round((statvfs.f_frsize * statvfs.f_bfree) / 1024 / 1024 / 1024, 2)
        return free

    def check_lock(self) ->  bool:
        """Check if the LOCK_FILE_PATH file exists, if it does, do not run. """
        # print("[%s] Checking for lock" % arrow.now())
        seedbox_lock = self.redis.get('%s-lock' % self.redis_key_base)
        if seedbox_lock:
            seedbox_lock_val = seedbox_lock.decode()
            if seedbox_lock_val == 'True':
                seedbox_lock_val = True
            else:
                seedbox_lock_val = False
        else:
            seedbox_lock_val = False

        if seedbox_lock_val:
            lock_created = self.redis.get('%s-lock-date' % self.redis_key_base)
            if not lock_created:
                return False
            lock_created = lock_created.decode()

            lock_created_val = arrow.get(lock_created)
            delta = arrow.now() - lock_created_val
            delta_hours = round(delta.seconds / 3600, 2)

            delta_alert = arrow.now() - timedelta(minutes=self.lock_age_alert_min)
            alert_time = timedelta(minutes=self.lock_age_alert_min)
            print('[%s] Seedbox lock found: created %s hours ago' % (arrow.now(), delta_hours))


            # if the delta from our lock file age is beyond self.lock_age_alert_min, create a
            # notification
            lock_age_minutes = round(float(self.lock_age_alert_min / 60), 2)
            if lock_created_val < delta_alert:
                msg = "Seedbox lock is more than %s hours old, something may be wrong." % (
                    lock_age_minutes)
                print("[%s] Warning: %s" % (arrow.now(), msg))
                self.notifications.append({
                    "type": "warning",
                    "message": msg,
                    "ignore_str": lock_age_minutes,
                })

        return seedbox_lock_val

    def create_lock(self) -> bool:
        """Create a lock file so only only instance of seedbox-download runs at once. """
        seedbox_lock = self.redis.set('%s-lock' % self.redis_key_base, 'True')
        lock_date = arrow.utcnow().format("YYYY-MM-DD HH:mm:ss")
        seedbox_lock = self.redis.set('%s-lock-date' % self.redis_key_base, lock_date)
        return True

    def prep_downloads(self) -> bool:
        """Connect to remote SFTP host and look for files to download in the directory specified
           by `self.sftp_path`. After downloading each file, try to delete the file from the
           remote.
        """

        print("[%s] Connecting to host: %s" % (arrow.now(), self.sftp_host))
        self.sftp = pysftp.Connection(
            self.sftp_host,
            username=self.sftp_user,
            password=self.sftp_pass)
        with self.sftp.cd(self.sftp_path):
            directory_structure = self.sftp.listdir_attr()

        print("[%s] Looking for files in %s" % (arrow.now(), self.sftp_path))
        if directory_structure:
            print("[%s] Found %s file(s) to download." % (arrow.now(), len(directory_structure)))
        else:
            print("[%s] Nothing on remote file system to download." % arrow.now())
            return True


        # For each file, handle pulling them down and then removing them from the remote
        for attr in directory_structure:
            # Get file info
            filename = attr.filename

            if self._file_if_exists_locally(filename):
                continue

            self.to_download.append(filename)

        if self.to_download:
            return True
        else:
            return False

    def run_downloads(self):
        """ """
        print("[%s] Planning to download:" % arrow.now())
        for filename in self.to_download:
            print('\t"%s"' % filename)
        for filename in self.to_download:
            self.download_entity(filename)
        print("[%s] Downloads successful" % arrow.now())

    def download_entity(self, filename: str) -> bool:
        """Do this better. """
       # Download
        print('[%s] Downloading: "%s"' % (arrow.now(), filename))
        remote_file = os.path.join(self.sftp_path, filename)

        local_path = self.local_path
        if self.move_after_download:
            local_path = self.local_path_tmp

        with self.sftp.cd(self.sftp_path):
            self.sftp.get_r(filename, localdir=local_path)
        print('[%s] Completed Download "%s"' % (arrow.now(), filename))
        self.downloads.append(filename)

        # Move file to completed downloads location if enabled
        if self.move_after_download:
            source = os.path.join(self.local_path_tmp, filename)
            destination = os.path.join(self.local_path, filename)
            dest = shutil.move(source, destination)

        # Delete Remote
        if self.sftp_remove_files:
            self._recursive_delete(filename)

    def delete_lock(self) -> bool:
        """Delete the lock file once the script has completed. """
        seedbox_lock = self.redis.set('%s-lock' % self.redis_key_base, 'False')
        # print("[%s] Deleted lock" % arrow.now())
        return True

    def report(self) -> bool:
        """Report out the downloads performed. """
        print('Report')
        slack_msg = ""
        if self.downloads:
            print('[%s] All downloads completed.' % arrow.now())
            slack_msg += "Downloaded\n"
        for download in self.downloads:
            msg = '[%s] %s' % (arrow.now(), download)
            print(msg)
            slack_msg += " - `%s`" % download + "\n"

        if self.notifications:
            slack_msg += "Issues \n"
            for notification in self.notifications:
                slack_msg += " - %s" % notification['message']

        # If a message worth notifying about has been created, send it.
        if self.notifications_enabled and self.slack_enabled and slack_msg:
            self._send_slack(slack_msg)
        return True

    def _file_if_exists_locally(self, filename: str) -> bool:
        """Check if the remote file already exists in the local download location. """
        local_file_path = os.path.join(self.local_path, filename)
        if os.path.exists(local_file_path):
            msg = '"%s" already exists locally, logging this and skipping.' % filename
            print('[%s] Warning: %s' % (arrow.now(), msg))
            self.notifications.append({'type': 'warning', 'message': msg})
            return True

        if self.move_after_download:
            local_file_path_tmp = os.path.join(self.local_path_tmp, filename)
            if os.path.exists(local_file_path):
                msg = '"%s" already exists locally in tmp, logging this and skipping.' % filename
                print('[%s] Warning: %s' % (arrow.now(), msg))
                self.notifications.append({'type': 'warning', 'message': msg})
                return True

        return False

    def _recursive_delete(self, filename: str):
        """Deletes as far as 2 directories deep from the `filename` var from the remote server.
           Example tree to be removed from the remote.
           IE /downloads/complete/some_movie
           IE /downloads/complete/some_movie/subs
        """
        print('[%s] Starting remote delete for: "%s"' %  (
            arrow.now(),
            os.path.join(self.sftp_path, filename)))
        sub_dirs_to_delete = []
        sub_sub_dirs_to_delete = []
        sub_sub_sub_dirs_to_delete = []

        # First directory
        # ie og dir
        with self.sftp.cd(os.path.join(self.sftp_path, filename)):
            directory_structure = self.sftp.listdir_attr()
            for sub in directory_structure:
                if sub.st_size != 4096:
                    continue
                sub_dirs_to_delete.append(os.path.join(
                    self.sftp_path,
                    filename,
                    sub.filename))

                # Second directory
                with self.sftp.cd(os.path.join(self.sftp_path, filename, sub.filename)):
                    directory_structure = self.sftp.listdir_attr()
                    for sub_sub in directory_structure:
                        if sub_sub.st_size != 4096:
                            continue
                        sub_sub_dirs_to_delete.append(os.path.join(
                            self.sftp_path,
                            filename,
                            sub.filename,
                            sub_sub.filename))

                        # Third Directory
                        with self.sftp.cd(os.path.join(self.sftp_path, filename, sub.filename, sub_sub.filename)):
                            directory_structure = self.sftp.listdir_attr()
                            for sub_sub_sub in directory_structure:
                                if sub_sub_sub.st_size != 4096:
                                    continue
                                sub_sub_sub_dirs_to_delete.append(os.path.join(
                                    self.sftp_path,
                                    filename,
                                    sub.filename,
                                    sub_sub.filename,
                                    sub_sub_sub.filename))


        for sub_sub_sub_dir  in sub_sub_sub_dirs_to_delete:
            cmd = self._get_rm_dir_command(sub_sub_sub_dir, sub_dir=True)
            response = subprocess.check_output(cmd, shell=True).decode()

        for sub_sub_dir  in sub_sub_dirs_to_delete:
            cmd = self._get_rm_dir_command(sub_sub_dir, sub_dir=True)
            response = subprocess.check_output(cmd, shell=True).decode()

        for sub_dir  in sub_dirs_to_delete:
            cmd = self._get_rm_dir_command(sub_dir, sub_dir=True)
            response = subprocess.check_output(cmd, shell=True).decode()

        cmd = self._get_rm_dir_command(filename)
        response = subprocess.check_output(cmd, shell=True).decode()

    def _get_rm_dir_command(self, dir_name: str, sub_dir: bool=False) -> str:
        if sub_dir:
            remote_path = os.path.join(self.sftp_path, dir_name)
        else:
            remote_path = self.sftp_path
        cmd = """
sshpass -p %(password)s sftp %(user)s@%(host)s << !
    cd %(remote_path)s
    rm "%(dir)s"/*
    rmdir "%(dir)s"
    bye
!
        """ % {
            "remote_path": remote_path,
            "dir": dir_name,
            "user": self.sftp_user,
            "password": self.sftp_pass,
            "host": self.sftp_host,
        }
        return cmd

    def _send_slack(self, msg: str) -> bool:
        """Sends a slack message to the peppercon #notifi channel. """
        if not self._slack_evalutate_msg_worthy(msg):
            print("[%s] Decided Slack msg is not worhty of sending, skipping" % arrow.now())
            print('\tWould have sent')
            print(msg)
            return False

        client = WebClient(token=self.slack_token)

        try:
            response = client.chat_postMessage(channel=self.slack_channel, text=msg)
            # assert response["message"]["text"] == msg
            print("Sent slack message")
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
            return False

        self.redis.set('%s-last-message-content' % self.redis_key_base, msg)
        msg_date = arrow.utcnow().format("YYYY-MM-DD HH:mm:ss")
        self.redis.set('%s-last-message-ts' % self.redis_key_base, msg_date)
        return True

    def _slack_evalutate_msg_worthy(self, msg) -> bool:
        """This method evaluates whether or not a message is worthy of being sent to slack.
           Non-worthiness has the following criteria, a message MUST meat all the following
           conditions.
            - redis values for the keys `seedbox-download-last-message-ts` and 
              `seedbox-download-last-message-content` exist.
            - The current message purposed does match the last message.
            - This message has been sent to Slack in the last `self.slack_repeat_message_min` 
              minutes.

           If all those conditions are met, we will NOT send a message to slack.

           Ultimately a message worthy of sending returns True

        """
        
        last_slack_msg_ts = self.redis.get('%s-last-message-ts' % self.redis_key_base)
        last_slack_msg_content = self.redis.get('%s-last-message-content' % self.redis_key_base)

        if not last_slack_msg_ts or not last_slack_msg_content:
            return  True

        last_slack_msg_ts = last_slack_msg_ts.decode()
        last_slack_msg_content = last_slack_msg_content.decode()

        # If either redis var does not exist, leave the func, msg IS worthy of send
        if not last_slack_msg_content and last_slack_msg_ts:
            print('NOT SEND redis keys arent there')
            return False
        
        # If the last message sent, is not identical to the one being sent now, EXIT msg IS worthy
        if last_slack_msg_content != msg:
            return True

        # If the message IS a duplicate of the last, and HAS been sent in minutes, do NOT repeat 
        last_slack_msg_ts = arrow.get(last_slack_msg_ts)
        delta = arrow.now() - timedelta(minutes=self.slack_repeat_message_min)
        if not last_slack_msg_ts < delta:
            print('NOT SENC dupe message but its been %s minutes' % self.slack_repeat_message_min)

            return False

        # Send the slack message, it has passed all our concerns. 
        print('SEND IT to slack')
        return True

    def _config_error(self, missing_tree_peice: str):
        """ """
        print("[%s] ERROR: CONFIG - Config is missing the yaml piece: %s" % (
            arrow.now(),
            missing_tree_peice))


def parse_args():
    """Parse CLI args. """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        default=False,
        help="Config file to use.")
    parser.add_argument(
        "-d",
        "--delete",
        default=False,
        action='store_true',
        help="Delete lock if it exists allowing Ftp Boi to run.")
    parser.add_argument(
        "-v",
        "--version",
        default=False,
        action='store_true',
        help="Print the version information.")

    args = parser.parse_args()

    if not args.config:
        args.config = os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.yaml")

    return args


if __name__ == "__main__":
    args = parse_args()
    SeedboxDownload(args).run({})

# End File: ftp-boi/ftp-boi.py
