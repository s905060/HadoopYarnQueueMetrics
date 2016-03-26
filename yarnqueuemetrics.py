#!/usr/bin/env python2.7
# Author Jash Lee s905060@gmail.com

import time
import re
import subprocess


class YarnQueueMetrics():

    def __init__(self):
        self.epoch_time = str(int(time.time()))
        self.fuzzy_time = self.epoch_time[:7]  # minute accuracy window
        self.log_path = "/var/log/hadoop/rm_yarn_metrics.log"
        self.metric_prefix = "hadoop.yarn.FairScheduler"
        self.metric_timestamp = ""

    def is_queue_or_users(self, input):
        """
         * [is_queue_or_users description]
         * @param  {[string]}   input           [description]
         * @return {[string]}   queue_or_user   [description]
        """
        if "User" in input:
            queue_or_user = "User"
        else:
            queue_or_user = "Queue"

        return queue_or_user

    def user_generator(self, queue, user):
        """
         * [user_generator description]
         * @param  {[string]}  queue       [description]
         * @param  {[string]}  user        [description]
         * @return {[string]}  user_queue  [description]
        """
        queue = queue.split("=")
        queue = queue[1].split(".")
        queue = "_".join(queue)
        user_queue = user.split("=")[1] + "." + queue

        return "users." + user_queue

    def queue_generator(self, queue):
        """
         * [queue_generator description]
         * @param  {[string]}  queue   [description]
         * @return {[string]}  queue   [description]
        """
        queue = queue.split("=")
        queue = queue[1].split(".")
        queue = "_".join(queue)

        return "queues." + queue

    def grep_log(self):
        """
         * [grep_log description]
         * @return {[string]} output [description]
        """
        output = subprocess.check_output('sudo grep %s %s 2>/dev/null' % (self.fuzzy_time, self.log_path), shell=True)
        return output

    def stdout_metrics(self, log):
        """
         * [grep_log description]
         * @STDOUT {[string]} STDOUT [description]
        """

        lines = log.splitlines()

        for line in lines:
            line = re.split(r'[;,:\s]\s*', line)  # Magic split
            self.metric_timestamp = str(line[0])
            start_index = ""

            if "QueueMetrics" in line[1]:
                queue_or_user = self.is_queue_or_users(line[3])

                if queue_or_user == "User":
                    queue = self.user_generator(line[2], line[3])
                    start_index = 4

                elif queue_or_user == "Queue":
                    queue = self.queue_generator(line[2])
                    start_index = 3

                for index in xrange(start_index, len(line)):
                    metrics = line[index].split("=")
                    key = metrics[0]
                    value = metrics[1]
                    print self.metric_prefix + '.' + queue + '.' + key + ' ' + value + ' ' + self.metric_timestamp

            else:
                pass

if __name__ == "__main__":
    metric_checker = YarnQueueMetrics()
    metric_checker.stdout_metrics(metric_checker.grep_log())
