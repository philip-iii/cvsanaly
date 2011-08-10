from pycvsanaly2.extensions import Extension, register_extension, \
        ExtensionRunError
from pycvsanaly2.Database import SqliteDatabase, MysqlDatabase, \
        statement, execute_statement, get_repo_id
from pycvsanaly2.utils import printdbg, printerr, printout, \
        get_repo_uri

class FilePatches(Extension):
    def __prepare_table(self, connection):
        cursor = connection.cursor()

        if isinstance(self.db, SqliteDatabase):
            import sqlite3.dbapi2

            try:
                cursor.execute("""ALTER TABLE actions
                    ADD patch varchar""")
            except sqlite3.dbapi2.OperationalError:
                # It's OK if the column already exists
                pass
            except:
                raise
            finally:
                cursor.close()

        elif isinstance(self.db, MysqlDatabase):
            import MySQLdb

            # I commented out foreign key constraints because
            # cvsanaly uses MyISAM, which doesn't enforce them.
            # MySQL was giving errno:150 when trying to create with
            # them anyway
            try:
                cursor.execute("""ALTER TABLE scmlog
                    ADD patch varchar(255)""")
            except MySQLdb.OperationalError, e:
                if e.args[0] == 1060:
                    # It's OK if the column already exists
                    pass
                else:
                    raise
            except:
                raise
            finally:
                cursor.close()

        connection.commit()
        cursor.close()
        
    def run(self, repo, uri, db):
        self.db = db
        connection = self.db.connect()
        read_cursor = connection.cursor()
        write_cursor = connection.cursor()

        # Try to get the repository and get its ID from the database
        try:
            repo_uri = get_repo_uri(uri, repo)
            repo_id = get_repo_id(repo_uri, read_cursor, db)

        except NotImplementedError:
            raise ExtensionRunError( \
                    "FilePatches extension is not supported for %s repos" % \
                    (repo.get_type()))
        except Exception, e:
            raise ExtensionRunError( \
                    "Error creating repository %s. Exception: %s" % \
                    (repo.get_uri(), str(e)))


register_extension("FilePatches", FilePatches)
