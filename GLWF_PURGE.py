import datetime
import logging
import cx_Oracle
import os

from util import send_email_notification

# Database connection parameters
#DB_USER = 'Glwfread'
#DB_PASSWORD = 'Read_2025_Jun_Gl'
#DB_DSN = 'USDDCODB101.CORP.INTRANET:1521/glwfe2e'

#Database connection parameters
DB_USER = 'GLWF'
DB_PASSWORD = 'Owner_2025_Nov_Gl'
DB_DSN = 'usddcodb101.CORP.INTRANET:1521/GLWFST1'

# Extract to variables
billing_app_id = 'PPP'
company_owner_id = '4'
#request_id = 'REQ123456789'
#date='2018-10-06'
def connect_to_oracle():
    try:
        connection = cx_Oracle.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=DB_DSN
        )
        print(f"Connected to Oracle Database: {os.path.abspath(__file__)}")
        return connection
    except cx_Oracle.DatabaseError as e:
        print(f"Database connection error for {os.path.abspath(__file__)}: {e}")
        return None
    
log_file_path = os.path.abspath(f"Delete_script_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f')}.log")
print('\nlog file path:\n' + log_file_path)

start_time = datetime.datetime.now()
print(f'Script started at: {start_time}')
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s -> %(message)s', datefmt='%Y-%m-%dT%I:%M:%S')
logging.info(f'Script started at: {start_time}')


queries = [
   # f"Delete from GL_DETAIL_GLWF where APPLICATION_ID ='{billing_app_id}' and COMPANY_OWNER_ID = '{company_owner_id}' and REQUEST_ID='{request_id}' ",
    f"Delete from GLWF.SERV_REQ_DET_GLWF_BKP12DEC where BILLING_APPLICATION_ID= '{billing_app_id}' and COMPANY_OWNER_ID = '{company_owner_id}'  "]
print("queries defined successfully")


def execute_delete_queries(queries, batch_size=5000):
    connection = None
    curs = None
    total_rows_deleted = 0
    max_total_delete = 10000  # Set a maximum limit to avoid infinite loops
    try:
        connection = connect_to_oracle()
        if connection is None:
            logging.error("Failed to connect to the database.")
            return False, 0

        for i, base_query in enumerate(queries, 1):
            if total_rows_deleted >= max_total_delete:
                print(f"Reached maximum total delete limit of {max_total_delete}. Stopping further deletions.")
                break
            while True:
                # Add ROWNUM condition for batch delete
                if "where" in base_query.lower():
                    batch_query = base_query.rstrip() + f" AND ROWNUM <= {batch_size}"
                else:
                    batch_query = base_query.rstrip() + f" WHERE ROWNUM <= {batch_size}"

                print(f"Executing query {i}/{len(queries)}: {batch_query}")
                curs = connection.cursor()
                curs.arraysize = 5000
                curs.prefetchrows = 5000
                try:
                    curs.execute(batch_query)
                    deleted = curs.rowcount
                    if deleted > 0:
                        connection.commit()
                        total_rows_deleted += deleted
                        print(f"Deleted {deleted} records, committed!")
                    else:
                        break  # No more rows to delete
                except cx_Oracle.DatabaseError as e:
                    error, = e.args
                    print(f"   ❌ Query {i} failed: {error.message}")
                    logging.error(f'Query {i} Oracle-Error-Message: {error.message}')
                    break
                except Exception as e:
                    print(f"   ❌ Query {i} failed: {e}")
                    logging.error(f'Query {i} unexpected error: {e}')
                    break
                finally:
                    curs.close()
            print(f"Total deleted for query {i}: {total_rows_deleted}")
       
        return True, total_rows_deleted
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        logging.error(f'Oracle-Error-Message: {error.message}')
        return False, 0
    except Exception as e:
        logging.error(f'An unexpected error occurred: {e}')
        return False, 0
    finally:
        if connection:
            connection.close()
            print("Database connection closed")

# Execute the deletion
success, total_deleted = execute_delete_queries(queries,batch_size=5000)

send_email_notification({
    'status': 'SUCCESS' if total_deleted > 0 else 'NO_DATA',
    'script_start_time': start_time,
    'script_end_time': datetime.datetime.now(),
    'total_deleted': total_deleted  # Use total_deleted, not row_count
}, __file__)

print(f"Purge SUMMARY:")
print(f"Total records deleted: {total_deleted:,}")
end_time = datetime.datetime.now()
print(f'Script ended at: {end_time}')
logging.info(f'Script ended at: {end_time}')
time_taken = end_time - start_time
print(f'Total time taken: {time_taken}')
logging.info(f'Total time taken: {time_taken}')
