#! /usr/bin/env python3

import boto3

from datetime import datetime
import re
from sys import argv

START_OF_PLAY = 8
CLOSE_OF_PLAY = 20


# ====== Rules ====== #

# Rules are functions that take a local date/time and return one of these values
#  - 'start':   Starts the instance
#  - 'stop':    Stops the instance
#  - 'leave':   Leaves the instance unchanged (this is the default)
#  - 'start?'   Starts the instance unless overridden by a subsequent rule
#  - 'stop?'    Stops the instance unless overridden by a subsequent rule
#  - 'leave?'   Leaves the instance unchanged unless overridden by a subsequent rule
#  - '?'        Uses the previous rule unless overridden by a subsequent rule
#  - Any other value:
#               Uses the previous rule and stops processing other rules

RULE_FUNCTIONS = {}

def rule(name):
    def register(func):
        RULE_FUNCTIONS[name.lower()] = func
        return func
    return register

# === AlwaysOn

@rule('AlwaysOn')
def AlwaysOnRule(dt):
    """
    Any instances that are stopped will be started.
    This rule should always be specified last.
    """
    return 'start'

# === OnDemand

@rule('OnDemand')
def OnDemandRule(dt):
    """
    Stops instances at COP. Leaves them as-is otherwise.
    """
    if dt.weekday() < 5 and dt.hour == CLOSE_OF_PLAY:
        return 'stop'
    else:
        return '?'

# === WorkingHours

@rule('WorkingHours')
def WorkingHoursRule(dt):
    """
    Starts instances at start of day and stops them at the end of the day.
    """
    if dt.weekday() < 5:
        if dt.hour == START_OF_PLAY:
            return 'start'
        elif dt.hour == CLOSE_OF_PLAY:
            return 'stop'
    return '?'

# === Manual

@rule('Manual')
def ManualRule(dt):
    return '?'

# === PatchTuesday

@rule('PatchTuesday')
def PatchTuesdayRule(dt):
    """
    Starts the instances and keeps them up during the patch window.
    Shuts them down at the end unless other rules specify otherwise.
    This rule should always be specified first.
    """
    PATCH_START_HOUR = 2
    PATCH_END_HOUR = 7

    # Patch Wednesday is the day after Patch Tuesday
    # so it's the day after the second Tuesday in the month
    is_patch_wednesday = dt.weekday() == 2 and dt.day >= 9 and dt.day <= 15

    if is_patch_wednesday:
        if dt.hour == PATCH_START_HOUR:
            return 'start'
        elif dt.hour == PATCH_END_HOUR:
            return 'stop?'
        elif dt.hour > PATCH_START_HOUR and dt.hour < PATCH_END_HOUR:
            return 'leave'

    return '?'


# ====== get_action ====== #

def get_action(rules, dt):
    if isinstance(rules, str):
        rules = [
            rule.strip()
            for rule in
            re.split('[' + re.escape(',+:/') + ']', rules)
        ]
    result = 'leave'
    for rule in rules:
        rule_func = RULE_FUNCTIONS.get(rule.lower())
        if callable(rule_func):
            action = rule_func(dt)
            if not isinstance(action, str):
                return result
            action = action.lower()
            passthrough = action.endswith('?')
            if passthrough:
                action = action[:-1]
            if action in ['start', 'stop', 'leave']:
                result = action
            if not passthrough:
                return result
    return result


# ====== InstanceControl class ====== #

class InstanceControl(object):

    def __init__(self, region_name, session=None):
        self.session = session or boto3.Session(region_name=region_name)
        self.ec2 = self.session.resource('ec2')
        self.autoscaling = self.session.client('autoscaling')

    def get_autoscaling_groups(self):
        """
        Gets all autoscaling groups
        """
        more = True
        r = self.autoscaling.describe_auto_scaling_groups()
        while more:
            groups = r['AutoScalingGroups']
            more = next_token = r.get('NextToken')
            for g in groups:
                yield g
            if more:
                r = self.autoscaling.describe_auto_scaling_groups(NextToken=next_token)

    def get_autoscaling_groups_for_change(self, action, time):
        for g in self.get_autoscaling_groups():
            tags = g.get('Tags')
            if tags:
                lifecycle = [
                    tag['Value'] for tag in tags
                    if tag['Key'] == 'Lifecycle'
                ]
                rules = lifecycle[0] if lifecycle else ''
                group_action = get_action(rules, time)
                if action == group_action:
                    yield g

    def get_instances(self):
        """
        Gets all instances tagged with Lifecycle:WorkingHours
        """
        return self.ec2.instances.all()

    def get_instances_for_change(self, action, time):
        for i in self.get_instances():
            if i.tags:
                lifecycle = [
                    tag['Value'] for tag in i.tags
                    if tag['Key'] == 'Lifecycle'
                ]
                rules = lifecycle[0] if lifecycle else ''
                group_action = get_action(rules, time)
                if action == group_action:
                    yield i

    def stop_instances(self, dt):
        for group in self.get_autoscaling_groups_for_change('stop', dt):
            group_name = group['AutoScalingGroupName']
            print('Suspending processes on ' + group_name)
            try:
                self.autoscaling.suspend_processes(AutoScalingGroupName=group_name)
            except:
                print('Could not suspend autoscaling group ' + group_name)
                import traceback
                traceback.print_exc()

        for instance in self.get_instances_for_change('stop', dt):
            print('Stopping instance: ' + instance.id)
            try:
                instance.stop()
            except:
                print('Could not stop instance ' + instance.id)
                import traceback
                traceback.print_exc()


    def start_instances(self, dt):
        for instance in self.get_instances_for_change('start', dt):
            print('Starting instance: ' + instance.id)
            try:
                instance.start()
            except:
                print('Could not start instance ' + instance.id)
                import traceback
                traceback.print_exc()

        for group in self.get_autoscaling_groups_for_change('start', dt):
            group_name = group['AutoScalingGroupName']
            print('Resuming processes on ' + group_name)
            try:
                self.autoscaling.resume_processes(AutoScalingGroupName=group_name)
            except:
                print('Could not resume autoscaling group ' + group_name)
                import traceback
                traceback.print_exc()


# ====== Control function to be called by AWS Lambda ====== #

def instance_control(event, context):
    """
    This function will be called by AWS Lambda. It either starts or stops all
    the instances whose names start with one of the default prefixes.abs

    Environments are brought up if the lambda is called in the morning; down if
    they are called in the afternoon or evening.
    """

    import os
    import pytz

    ic = InstanceControl(os.environ['AWS_REGION'])
    now = datetime.utcnow()
    tz = pytz.timezone('Europe/London')
    now = tz.fromutc(datetime.utcnow())
    ic.start_instances(now)
    ic.stop_instances(now)

if __name__ == '__main__':
    if len(argv) == 2:
        ic = InstanceControl(boto3.session.Session().region_name)
        command = argv[1]
        if command == 'start':
            ic.start_instances()
            exit(0)
        elif command == 'stop':
            ic.stop_instances()
            exit(0)

    print('Usage: stop.py (start|stop)')
    exit(1)
