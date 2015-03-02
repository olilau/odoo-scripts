#! /usr/bin/env python

import os
import sys
import xmlrpclib
import argparse
from getpass import getpass
from textwrap import dedent
import logging
from collections import Counter
from distutils.version import LooseVersion as version
import psycopg2
from psycopg2.psycopg1 import cursor as psycopg1cursor

__version__ = '0.2'

ERROR_ODOO_VERSION_NOT_FOUND = 2
ERROR_NO_FILESTORE_PATH = 3
ERROR_FILESTORE_PATH_DOES_NOT_EXIST = 4
ERROR_DOCUMENT_STORAGE_ALREADY_EXIST = 5
ERROR_DOCUMENT_NOT_INSTALLED = 6
ERROR_ATTACHMENTS_ALREADY_IN_A_FILESTORE = 7
ERROR_USER_NOT_ADMIN = 8
ERROR_NO_DNS = 9

LOG_FMT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FMT)

from argparse import RawTextHelpFormatter
parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
parser.add_argument(
    'dbname', metavar='DBNAME',
    help="Odoo database to connect",
    action='store')

group_connection = parser.add_argument_group("Connection parameters")
group_connection.add_argument(
    '-u', '--user', help="Odoo user (default: %(default)s)",
    action='store', default='admin')

only_one_of = group_connection.add_mutually_exclusive_group()
only_one_of.add_argument(
    '--password', help="Odoo password (default: %(default)s)",
    action='store', default='admin')
only_one_of.add_argument(
    '--ask-password', help="Ask for the Odoo password (default: %(default)s)",
    action='store_true', default=False)

group_connection.add_argument(
    '--host', help="Odoo server host (default: %(default)s)",
    action='store', default='localhost')
group_connection.add_argument(
    '-p', '--port', help="Odoo server port (default: %(default)s)",
    action='store', default=8069, type=int)
group_connection.add_argument(
    '--protocol', help="Odoo server protocol (default: %(default)s)",
    action='store', default='http')

parser.add_argument(
    '--filestore-path',
    help="Odoo server filestore path. Only required for 6.0 "
         "and 6.1 Odoo version (default: %(default)s)",
    action='store', default=False)
parser.add_argument(
    '--install-document-module',
    help="Installs the 'document' module. Only required for 6.0 "
         "and 6.1 Odoo version (default: %(default)s)",
    action='store_true', default=False)

parser.add_argument(
    '--manual-attachment-conversion',
    help=("Manually convert the attachments instead of executing "
          "the _attach_parent_id method from document/document.py\nYou'll "
          "need to comment the call to this method in "
          "document/document_data.xml:\n"
          "  <!--<function model=\"ir.attachment\"\n"
          "                name=\"_attach_parent_id\"/>-->\n"
          "This option is only available for the 6.0 version\n(it "
          "needs to be analyzed for other versions)\nThis option is "
          "usefull if you have a huge number of attachments (less memory "
          "consumption) but your attachments will be unavailable during "
          "this time"),
    action='store_true', default=False)
parser.add_argument(
    '--dsn',
    help=("DSN used to connect to the database (default: %(default)s). "
          "Example: dbname=yup port=5432 host=localhost"),
    action='store', default=False)

parser.add_argument(
    '-q', '--quiet', help="Quiet output (only errors are reported)",
    dest="quiet", action='append_const', const=1)
parser.add_argument(
    '--version',
    action='version', version='%(prog)s {}'.format(__version__))

args = parser.parse_args()


class loggingcursor(psycopg1cursor):
    def execute(self, query, vars=None):
        q = dedent(self.mogrify(*[query, vars]))
        logging.info(q.lstrip())
        try:
            return super(loggingcursor, self).execute(query, vars)
        except psycopg2.Error as e:
            if hasattr(e, 'diag'):
                msg = "{} - {}".format(e.diag.severity, e.diag.message_primary)
            else:
                msg = "ERROR - {}".format(e)
            logging.error(msg)
            raise


class DocumentMover(object):
    def __init__(self, args):
        self.args = args
        self.dbname = self.args.dbname
        self.verbose = not self.args.quiet

        self.sock = None
        self.uid = None
        self.pwd = None

    def move_using_config_parameter(self):
        # for Odoo 7.0 and 8.0
        value_ids = self.execute(
            'ir.config_parameter', 'search',
            [('key', '=', 'ir_attachment.location')], 0, False, 'id')

        if not value_ids:
            self.execute(
                'ir.config_parameter', 'create',
                {'key': 'ir_attachment.location', 'value': 'file:filestore'})
        else:
            self.execute(
                'ir.config_parameter', 'write', value_ids,
                {'key': 'ir_attachment.location', 'value': 'file:filestore'})
        self.log('Begin moving attachments')
        attachment_ids = self.execute(
            'ir.attachment', 'search', [])
        total = len(attachment_ids)
        c = 0
        for attachment_id in list(attachment_ids):
            c += 1
            try:
                attachments = self.execute(
                    'ir.attachment', 'read',
                    [attachment_id], ['id', 'name', 'datas'])
                attachment = attachments[0]
                msg = ("Moving attachment (id={}) {}/{} (status: "
                       "{{}}): {}").format(
                    attachment['id'], c, total, attachment['name'])
                self.execute(
                    'ir.attachment', 'write',
                    attachment['id'], {'datas': attachment['datas']})
                status = 'ok'
                self.log(msg.format(status))
            except xmlrpclib.Fault as e:
                self.log(e, True)
                attachment_ids.remove(attachment_id)
                status = 'fail'
                self.log(msg.format(status))
            except Exception as e:
                msg = 'Error when trying to move attachment with id={}'.format(
                    attachment['id'])
                self.log(msg.format(status))

        self.log('Deleting attachments from database')
        self.execute(
            'ir.attachment', 'write', attachment_ids,
            {'db_datas': False})

    def move_using_document_storage(self):
        # for Odoo 6.0 and 6.1
        self.pre_move_checks()

        # read old db storage id for reuse in for-loop
        old_storage_id = self.execute(
            'document.storage', 'search',
            [('type', '=', 'db')])[0]
        # see if a 'filestore' storage exists already:
        fs_storage_ids = self.execute(
            'document.storage', 'search',
            [
                ('type', '=', 'filestore'),
                ('path', '=', self.args.filestore_path)])
        if len(fs_storage_ids):
            # if so, use it:
            new_storage_id = fs_storage_ids[0]
        else:
            # otherwise, create it:
            new_storage_id = self.execute(
                'document.storage', 'create',
                {
                    'name': 'File Storage',
                    'type': 'filestore',
                    'path': self.args.filestore_path})

        # set storage to 'filestore'
        dir_ids = self.execute(
            'document.directory', 'search',
            [])

        self.log('Begin moving attachments')
        # Only work on those that haven't been converted yet
        attachment_ids = self.execute(
            'ir.attachment', 'search',
            [('db_datas', '!=', 'False')])

        all_attachment_ids = self.execute(
            'ir.attachment', 'search',
            [])
        unique_names = set([x['name'] for x in self.execute(
            'ir.attachment', 'read',
            all_attachment_ids, ['name']
            )])
        needs_rename = len(unique_names) != len(all_attachment_ids)
        total = len(attachment_ids)
        counter = 0
        for attachment_id in attachment_ids:
            counter += 1

            # set storage to 'db' - makes the script resumable
            attachment = self.execute(
                'ir.attachment', 'read',
                attachment_id, ['parent_id'])

            dir_id = attachment['parent_id'][0]
            # reset attachment to DB storage for reading
            self.execute(
                'document.directory', 'write',
                [dir_id], {'storage_id': old_storage_id})

            # load attachment
            attachment = self.execute(
                'ir.attachment', 'read',
                attachment_id, ['datas', 'parent_id', 'name'])

            data = attachment['datas']


            # set storage to 'filestore'
            self.execute(
                'document.directory', 'write',
                [dir_id], {'storage_id': new_storage_id})

            # write attachment -> will save it in filestore
            vals = {
                'datas': data,
                'db_datas': False
            }
            if needs_rename and not attachment['name'].startswith('attachment %d' % (attachment['id'], )):
                vals.update({
                    'name': 'attachment %d - %s' % (attachment_id, attachment['name'])
                    })
            res = self.execute(
                'ir.attachment', 'write',
                [attachment_id], vals)
            status = 'ok' if res else 'fail'
            msg = ("Moving attachment (id={}) {}/{} (status: "
                   "{{}})").format(
                attachment_id, counter, total)
            self.log(msg.format(status))



    def pre_move_checks(self):
        """preliminary checks"""
        # check args:
        if self.args.manual_attachment_conversion:
            if not self.args.dsn:
                msg = ("You must supply a DSN if you want to use the "
                       "'manual-attachment-conversion' option.")
                self.log(msg, out_to_err=True)
                sys.exit(ERROR_NO_DNS)

        # other checks:
        self.check_document_module_is_installed()
        self.install_document_module_if_needed()
        self.check_filestore_path()

    def check_document_module_is_installed(self):
        """check that 'document' module is installed"""
        document_module_ids = self.execute(
            'ir.module.module', 'search',
            [('name', '=', 'document')])
        document_module = self.execute(
            'ir.module.module', 'read',
            document_module_ids, ['state'])
        if document_module[0]['state'] != 'installed' \
                and not self.args.install_document_module:
            msg = ("For Odoo version 6.0 and 6.1, the 'document' module need "
                   "to be installed. This script can do it for you if you "
                   "specify the --install-document-module command line option")
            self.log(msg, out_to_err=True)
            sys.exit(ERROR_DOCUMENT_NOT_INSTALLED)

    def check_filestore_path(self):
        """for v6.0 and v6.1, we need the filestore path"""
        if not self.args.filestore_path:
            msg = ("For Odoo version 6.0 and 6.1, you need to specify "
                   "the --filestore-path command line option")
            self.log(msg, out_to_err=True)
            sys.exit(ERROR_NO_FILESTORE_PATH)

        # the filestore path needs to exist:
        if not os.path.isdir(self.args.filestore_path):
            msg = "filestore path '{}' does not exist.".format(
                self.args.filestore_path)
            self.log(msg, out_to_err=True)
            sys.exit(ERROR_FILESTORE_PATH_DOES_NOT_EXIST)


    def install_document_module_if_needed(self):
        if self.args.install_document_module:
            document_module_ids = self.execute(
                'ir.module.module', 'search',
                [('name', '=', 'document')])
            document_module = self.execute(
                'ir.module.module', 'read',
                document_module_ids, ['state'])
            if document_module[0]['state'] == 'installed':
                msg = ("'document' module already installed. Skipping its "
                       "installation (you supplied the "
                       "--install-document-module command line option)")
                self.log(msg)
            else:
                msg = "Installing the 'document' module"
                self.log(msg)
                mod_ids = self.execute(
                    'ir.module.module', 'search',
                    [('name', '=', 'document')])
                # install modules
                self.execute(
                    'ir.module.module', 'button_install',
                    mod_ids)

                upgrade_id = self.execute(
                    'base.module.upgrade', 'create',
                    {})
                self.execute(
                    'base.module.upgrade', 'upgrade_module',
                    [upgrade_id], {})
                self.log("done")
            if self.args.manual_attachment_conversion:
                self.manual_attachment_conversion()

    def manual_attachment_conversion(self):
        cnx = psycopg2.connect(self.args.dsn)
        cursorclass = loggingcursor # if self.args.verbose else psycopg1cursor
        cr = cnx.cursor(cursor_factory=cursorclass)

        cr.execute("""
            SELECT count(*)
            FROM ir_attachment
            WHERE parent_id IS not null""")
        if cr.fetchone()[0]:
            msg = ("SKipping the manual conversion: it seems "
                   "it's already done !!!")
            self.log(msg, out_to_err=True)
            return

        cr.execute("""
            SELECT res_id
            FROM ir_model_data
            WHERE
                model = 'document.directory' AND
                name = 'dir_root'""")
        res = cr.fetchone()
        parent_id = res[0]

        cr.execute("""
            SELECT count(*)
            FROM ir_attachment
            WHERE parent_id IS null AND db_datas IS not NULL""")
        total = cr.fetchone()[0]

        # batch by 1000
        #batch = int(total/20) or total
        batch = 1000
        for i in range(1, total+1, batch):
            cr.execute("""
              UPDATE ir_attachment
              SET
                parent_id = %s,
                db_datas = decode(encode(db_datas,'escape'), 'base64')
              WHERE
                parent_id IS NULL AND
                id BETWEEN %s AND %s""",
              [parent_id, i, i+batch-1])

        cr.execute("""
          UPDATE ir_attachment
          SET
            parent_id = %s
          WHERE
            parent_id IS NULL""",
          [parent_id])

        cr.execute("""
            ALTER TABLE ir_attachment ALTER parent_id SET NOT NULL""")

        cr.execute("""
            SELECT count(*)
            FROM ir_attachment
            WHERE
                file_size=0 AND
                db_datas IS NOT NULL""")

        total = cr.fetchone()[0]
        cr.execute("""
            SELECT id, db_datas
            FROM ir_attachment
            WHERE
                file_size=0 AND
                db_datas IS NOT NULL""")

        c = 0
        print_at = int(total / 20) or 1
        for attachment in cr.fetchall():
            c += 1
            if not c % print_at:
                self.log(
                    ("attachments converted: {}/{} ({})\n".format(
                        c, total, float(c/total*100))))
            f_size = len(attachment[1])
            cr.execute("""
                UPDATE ir_attachment
                SET file_size=%s
                WHERE id=%s""",
                (f_size, attachment[0]))

    def run(self):
        # ask or get password from command line
        self.pwd = self.args.password if self.args.password else getpass()

        self.sock = self.connect()

        # get Odoo version:
        odoo_version = self.get_odoo_version()

        if version(odoo_version) >= '6.0' and version(odoo_version) < '7.0':
            self.move_using_document_storage()
        elif version(odoo_version) >= '7.0':
            self.move_using_config_parameter()
        else:
            msg = ("Moving attachments in Odoo version '{}' is not yet "
                   "implemented").format(odoo_version)
            raise Exception(msg)

    def execute(self, *args, **kwargs):
        return self.sock.execute(
            self.dbname, self.uid, self.pwd, *args, **kwargs)

    def log(self, msg, out_to_err=False):
        if out_to_err:
            logging.error(unicode(msg))
        else:
            logging.info(unicode(msg))

    def connect(self):
        # Get the uid
        url = '{protocol}://{host}:{port}/{placeholder}'.format(
            protocol=self.args.protocol, host=self.args.host,
            port=self.args.port, placeholder='{}')
        sock_common = xmlrpclib.ServerProxy(url.format('xmlrpc/common'))
        self.uid = sock_common.login(self.dbname, self.args.user, self.pwd)
        self.sock = xmlrpclib.ServerProxy(url.format('xmlrpc/object'))
        if self.uid != 1:
            msg = ("You need to execute this script using the 'admin' user "
                   "(id=1) other users can have access restriction on "
                   "attachment or other objects and this can result in "
                   "an incomplete transfert")
            self.log(msg, out_to_err=True)
            sys.exit(ERROR_USER_NOT_ADMIN)

        return self.sock

    def get_odoo_version(self):
        installed_module_ids = self.execute(
            'ir.module.module', 'search',
            [('state', '=', 'installed')], 0, False, 'id')

        modules = self.execute(
            'ir.module.module', 'read',
            installed_module_ids, ['latest_version'])

        get_major_minor = lambda ver: '.'.join(ver.split('.', 2)[0:2])
        versions = Counter(
            [get_major_minor(m['latest_version']) for m in modules])
        odoo_version = versions.most_common()[0][0] if versions else None
        if not odoo_version:
            msg = "Error: could not determine Odoo version. Exiting"
            self.log(msg, out_to_err=True)
            sys.exit(ERROR_ODOO_VERSION_NOT_FOUND)

        return odoo_version

"""
begin;
update ir_attachment set description = coalesce(res_model, '') || ' with id=' || res_id::text || ' does not exist. Attachment has been kept.

', res_model = null, res_id = null where store_fname is null;
select id, store_fname, parent_id, char_length(db_datas::text), res_model, res_id, description from ir_attachment order by id;
commit;
"""

if __name__ == '__main__':
    app = DocumentMover(args)
    app.run()

