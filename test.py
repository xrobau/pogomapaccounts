import os
import uuid
from datetime import *
from peewee import *
from flask import Flask
from playhouse.flask_utils import FlaskDB
from playhouse.shortcuts import model_to_dict

import logging
logger = logging.getLogger('peewee')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

APP_DIR = os.path.dirname(os.path.realpath(__file__))
DATABASE = 'sqliteext:///%s' % os.path.join(APP_DIR, 'test.db')
DEBUG = False

app = Flask(__name__)
app.config.from_object(__name__)

flaskDb = FlaskDB()
flaskDb.init_app(app)

class Account(flaskDb.Model):
    username = CharField(primary_key = True)
    password = CharField()
    auth_service = CharField()
    last_fail_time = DateTimeField(default = datetime(2010,01,01))
    last_used_time = DateTimeField(default = datetime(2010,01,01))
    pause_until = DateTimeField(default = datetime(2010,01,01))
    needs_captcha = BooleanField(default = False)
    is_banned = BooleanField(default = False)
    uuid = CharField()
    atomic = CharField()

    def ping(self, force=False):
        if not force and self.is_banned:
            raise TypeError('Tried to ping an account that is currently banned')
        if not force and self.needs_captcha:
            raise TypeError('Tried to ping an account that is waiting on a captcha')
        self.update(last_used_time = datetime.now()).where(Account.username == self.username).execute()

    def block(self, banned=False, captcha=False):
        if banned:
            self.is_banned = True
        elif captcha:
            self.needs_captcha = True
        else:
            raise RuntimeError('Tried to block with no reason')

        self.update(last_fail_time = datetime.now()).where(Account.username == self.username).execute()

    def claim(self):
        # Ping ourselves to update our timestamp
        self.ping(force=True)
        # Now check that no other thread has grabbed this account by trying to do an atomic update
        newuuid = uuid.uuid4()
        row = self.update(atomic = newuuid).where(Account.atomic == self.atomic).execute()
        if row != 1:
            return False
        return True

try:
    Account.create_table()
except:
    pass


class Accounts:

    # Number of times to retry a claim.
    retries = 5
    
    @staticmethod
    def add_account(username, password, auth_service, force="no"):
        if force != "no":
            d = Account.delete().where(Account.username == username).execute()

        Account.create(username=username, password=password, auth_service=auth_service, uuid=uuid.uuid4(), atomic=uuid.uuid4())

    @staticmethod
    def del_all():
        Account.delete().execute();

    @staticmethod
    def get_all():
        return Account.select()

    @staticmethod
    def __get_query():
        glaretime = datetime.now()-timedelta(minutes = 2)
        return Account.select().where(
                Account.last_used_time < glaretime,
                Account.pause_until < datetime.now(),
                Account.is_banned == 0,
                Account.needs_captcha == 0
                )

    @staticmethod
    def get_random():
        try:
            for i in range(Accounts.retries):
                if (type(Account._meta.database).__name__ == "MySQLDatabase"):
                    a = Accounts.__get_query().order_by(fn.Rand()).get()
                else:
                    a = Accounts.__get_query().order_by(fn.Random()).get()

            if a.claim():
                return a
        except:
            pass

        return False

    @staticmethod
    def get_least_used():
        try:
            for i in range(Accounts.retries):
                a = Accounts.__get_query().order_by(Account.last_used_time).limit(1).get()
            if a.claim():
                return a
        except:
            pass

        return False

    @staticmethod
    def get_most_used():
        try:
            for i in range(Accounts.retries):
                a = Accounts.__get_query().order_by(Account.last_used_time.desc()).limit(1).get()
            if a.claim():
                return a
        except:
            pass

        return False

    @staticmethod
    def captchas_needed():
        try:
            return len(Account.select(Account.needs_captcha).where(Account.needs_captcha == True))
        except:
            return 0



a = Accounts()

a.del_all()

a.add_account("fred", "abc", "no", "yes")
#a.add_account("fred2", "abc", "no", "yes")
#a.add_account("fred3", "abc", "no", "yes")
#a.add_account("fred4", "abc", "no", "yes")
#a.add_account("fred5", "abc", "no", "yes")
#a.add_account("fred6", "abc", "no", "yes")
#
account = a.get_random()
account.ping()
print account.username
del account
account = a.get_random()
# Note it fails here due to being return false instead of an object
account.ping()
print account.username
del account
account = a.get_random()
account.ping()
print account.username
exit
"""
del account
account = a.get_random()
account.ping()
print account.username
del account
account = a.get_random()
account.ping()
print account.username
del account
account = a.get_random()
account.ping()
print account.username
del account
account = a.get_random()
account.ping()
print account.username
del account
account = a.get_random()
account.ping()
print account.username
del account

exit
account = a.get_random()
account.block(captcha=True)
account.claim()
exit

#print a.captchas_needed()
#Account.update(needs_captcha = True).where(Account.username == "fred").execute()
#print a.captchas_needed()
#exit

x = a.get_all()

for z in x:
#    print model_to_dict(z)
    print z.atomic
"""
