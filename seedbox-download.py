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
 - seedbox-download-lock
 - seedbox-download-lock-date
 - seedbox-download-last-message-ts
 - seedbox-download-last-message-content

Features to build
    - dertmine local freespace and STOP DL if it exceeds that
    - ALLOW CLI args such as --delete to remove lock key from redis
    -  Delete from remote  recursively, or some what
    - download files in remote dir that are NOT folders, ie a reademe.txt

    - check download size
        - make sure this doesnt exceed local vol

    - create python constant to decide wheather or not to delete files from remote.
      SFTP_DELETE_REMOTE_COPY

    - determine  external root "file" size

    print("[%s]" % (arrow.now()))

"""
import argparse
from datetime import timedelta
import os
import subprocess

import arrow
import pysftp
import redis
import slack
import yaml


# Slack config
SLACK_ENABLED = False

# Typically  this can remain constant, unless you have a multiple instances of this script
REDIS_KEY_BASE = "seedbox-download-beta"

class SeedboxDownload:

    def __init__(self):
        self.downloads = []
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.notifications = []

    def run(self, args):
        print("\n[%s] Starting SeedBox Download" % arrow.now())
        self.read_config()
        self.prep_run()
        if not self.check_lock():
            self.create_lock()
            self.stage_download()
            self.delete_lock()
        # self.report()
        print('[%s] Finished' % arrow.now())

    def read_config(self):
        """Read a config yaml file, and use those values.
           By default we assume the script exists in the same dir as this script does named simply
           ./config.yaml

           @todo: CLI support
           @note: Future  versions will  allow this to be supplemented via CLI args.
        """
        self.redis_key_base = REDIS_KEY_BASE
        self.slack_enabled = SLACK_ENABLED

        self.config_path = None

        if not self.config_path:
            current_script_dir = os.path.dirname(os.path.realpath(__file__))
            expected_config_loc = os.path.join(current_script_dir, 'config.yaml')
            if not os.path.exists(expected_config_loc):
                print("ERROR: cannot find config file: %s" % expected_config_loc)
                exit()
            self.config_path = expected_config_loc


        print("[%s] DEBUG - Using config file: %s" % (arrow.now(), self.config_path))

        with open(self.config_path) as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            config_yaml = yaml.load(file, Loader=yaml.FullLoader)

        if 'local' not in config_yaml:
            return _config_error('local')

        # Locals !
        if config_yaml['local']['download_path']:
            self.local_path = config_yaml['local']['download_path']
        if config_yaml['local']['lock_age_alert_min']:
            self.lock_age_alert_min = config_yaml['local']['lock_age_alert_min']
        if 'redisKeyBase' in config_yaml['local']:
            self.redis_key_base = config_yaml['local']['instanceName']

        # Remote
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

        if config_yaml['notifications']['enabled']:
            self.notifications_enabled = True

            self.slack_enabled = config_yaml['notifications']['slack']['enabled']
            if self.slack_enabled:
                self.slack_token = config_yaml['notifications']['slack']['token']
                self.slack_channel = config_yaml['notifications']['slack']['channel']
                self.slack_user = config_yaml['notifications']['slack']['user']
                self.slack_repeat_message_min = config_yaml['notifications']['slack']['repeatMsgIntervalMin']
            
    def prep_run(self):
        print("[%s] Remote Host:\t\t%s" % (arrow.now(), self.sftp_host))
        print("[%s] Remote Path:\t\t%s" % (arrow.now(), self.sftp_path))
        if self.sftp_remove_files:
            remote_delete_msg = "YES"
        else:
            remote_delete_msg = "NO"
        print("[%s] Remote Delete Files:\t%s" % (arrow.now(), remote_delete_msg))

        print("[%s] Local Path:\t\t%s" % (arrow.now(), self.local_path))
        # @todo: figure this out
        # print("[%s] Local Free Space:\t%s(someting)" % (arrow.now(), 'x'))
        print("[%s] Redis Lock Name:\t%s-lock" % (arrow.now(), self.redis_key_base))
        print("\n")

        # print("[%s] Remote Host:]\t%s" % (arrow.now()))
        return True

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

    def stage_download(self) -> bool:
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

            self.download_entity(filename)

        if self.downloads:
            print("[%s] Downloads successful" % arrow.now())

        return True

    def download_entity(self, filename: str) -> bool:
        """Do this better. """
       # Download
        print('[%s] Downloading: "%s"' % (arrow.now(), filename))
        remote_file = os.path.join(self.sftp_path, filename)
        with self.sftp.cd(self.sftp_path):
            self.sftp.get_r(filename, localdir=self.local_path)
        print('[%s] Completed Download "%s"' % (arrow.now(), filename))
        self.downloads.append(filename)

        # Delete
        if self.sftp_remove_files:
            self._recursive_delete(filename)

    def delete_lock(self) -> bool:
        """Delete the lock file once the script has completed. """
        seedbox_lock = self.redis.set('%s-lock' % self.redis_key_base, 'False')
        # print("[%s] Deleted lock" % arrow.now())
        return True

    def report(self) -> bool:
        """Report out the downloads performed. """
        slack_msg = ""
        if self.downloads:
            print('[%s] All downloads completed.' % arrow.now())
            slack_msg += "Downloaded\n"
        for download in self.downloads:
            msg = '[%s] %s"' % (arrow.now(), download)
            print(msg)
            slack_msg += " - `%s`" % download + "\n"

        if self.notifications:
            slack_msg += "Issues \n"
            for notification in self.notifications:
                slack_msg += " - %s" % notification['message']

        # If a message worth notifying about has been created, send it.
        if slack_msg and SLACK_ENABLED:
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
        return False

    def _recursive_delete(self, filename: str):
        """Deletes as far as 2 directories deep from the `filename` var from the remote server.
           Example tree to be removed from the remote.
           IE /downloads/complete/some_movie
           IE /downloads/complete/some_movie/subs
        """
        print('[%s] Starting remote delete for: "%s"' %  (arrow.now(), filename))
        sub_dirs_to_delete = []
        sub_sub_dirs_to_delete = []
        sub_sub_sub_dirs_to_delete = []
        with self.sftp.cd(os.path.join(self.sftp_path, filename)):
            directory_structure = self.sftp.listdir_attr()
            for thing in directory_structure:
                if thing.st_size != 4096:
                    continue
                sub_dirs_to_delete.append(os.path.join(
                    self.sftp_path,
                    filename,
                    thing.filename))

                with self.sftp.cd(os.path.join(self.sftp_path, filename, thing.filename)):
                    directory_structure = self.sftp.listdir_attr()
                    for sub_thing in directory_structure:
                        if sub_thing.st_size != 4096:
                            continue
                        sub_sub_dirs_to_delete.append(os.path.join(
                            self.sftp_path,
                            filename,
                            thing.filename,
                            sub_thing.filename))

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
            "password": self.sftp_path,
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

        slack_client = slack.WebClient(token=SLACK_TOKEN)
        print('Sending: %s' % msg)
        slack_client.chat_postMessage(
            username=SLACK_USER,
            channel=SLACK_CHANNEL,
            text=msg
        )
        current_time = str(arrow.now())
        self.redis.set('%s-last-message-ts' % self.redis_key_base, current_time)
        self.redis.set('%s-last-message-content' % self.redis_key_base, msg)
        print("[%s] Sending slack msg" % arrow.now())
        print(msg)
        return True

    def _slack_evalutate_msg_worthy(self, msg) -> bool:
        """This method evaluates whether or not a message is worthy of being sent to slack.
           Non-worthiness has the following criteria, a message MUST meat all the following
           conditions.
            - redis values for the keys `seedbox-download-last-message-ts` and 
              `seedbox-download-last-message-content` exist.
            - The current message purposed does match the last message.
            - This message has been sent to Slack in the last `SLACK_REPEAT_MESSAGE_MIN` minutes.

           If all those conditions are met, we will NOT send a message to slack.

           Ultimately a message worthy of sending returns True

        """
        
        last_slack_msg_ts = self.redis.get('%s-last-message-ts' % self.redis_key_base)
        last_slack_msg_content = self.redis.get('%s-last-message-content' % self.redis_key_base)

        if not last_slack_msg_ts or not last_slack_msg_content:
            return  False

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
        delta = arrow.now() - timedelta(minutes=SLACK_REPEAT_MESSAGE_MIN)
        if not last_slack_msg_ts < delta:
            print('NOT SENC dupe message but its been %s minutes' % SLACK_REPEAT_MESSAGE_MIN)

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
    """ """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--delete",
        default=False,
        action='store_true',
        help="Selects against all running and stopped containers")
    parser.add_argument(
        "-v",
        "--version",
        default=False,
        action='store_true',
        help="Print the version information.")

    args = parser.parse_args()

    import ipdb; ipdb.set_trace()
    return args

if __name__ == "__main__":
    # args = parse_args()
    # exit()
    SeedboxDownload().run({})

# End File: seedbox-download.py
