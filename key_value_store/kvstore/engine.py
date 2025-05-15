#  delete_key , compact_table, set_key, get_key, 
"""
DONE:

list_namespaces ,
namespace_exists , 
create_namespace , 
use_namespace, 
current_namespace,
table_exists , 
create_table ,
list_tables ,
flush_table,

"""

import os
import json
import time

class KVEngine:

    def __init__(self, db_path):
        self.db_path = db_path
        self.current_namespace = None
        self.current_table = None

    def current_namespace(self):
        """Get the current namespace."""
        return self.current_namespace

    def list_namespaces(self):
        """List all namespaces in the database directory."""
        list_ns = []
        for ns in os.listdir(self.db_path):
            if os.path.isdir(os.path.join(self.db_path, ns)):
                list_ns.append(ns)
        return list_ns
    
    def create_namespace(self,ns):
        """Create a new namespace in the database directory."""
        os.mkdir(f'{self.db_path}\\{ns}')
        return f"[OK] Created namespace: {ns}"    

    def namespace_exists(self,ns):
        """Check if a namespace exists in the database directory."""
        for files in os.listdir(self.db_path):
            if os.path.isdir(os.path.join(self.db_path, files)):
                if files == ns:
                    return True
        return False
    
    def use_namespace(self, ns):
        if self.namespace_exists(ns):
            self.current_namespace = ns
            return f"[OK] Using namespace: {ns}"
        else:
            return f"[ERROR] Namespace {ns} does not exist."

    def list_tables(self, ns):
        """List all tables in the current namespace."""
    
        tables = []
        namespace_dir = os.path.join(self.db_path, ns)
        for tbl in os.listdir(namespace_dir):
            if os.path.isdir(os.path.join(namespace_dir, tbl)):
                tables.append(tbl)
        return tables
    
    def table_exists(self, tbl):
        """Check if a table exists in the current namespace."""
        namespace_dir = os.path.join(self.db_path, self.current_namespace)
        for files in os.listdir(namespace_dir):
            if os.path.isdir(os.path.join(namespace_dir, files)):
                if files == tbl:
                    return True
        return False

    def current_table(self,tbl):
        """Get the current table."""
        self.current_table = tbl


    def create_table(self, tbl):
        """Create a new table in the current namespace."""
        namespace_dir = os.path.join(self.db_path, self.current_namespace)
        os.mkdir(os.path.join(namespace_dir, tbl))

    def flush_table(self, tbl):
        # Define the directory for the namespace and ensure it exists
        namespace_dir = self.db_path + "\\" + self.current_namespace + "\\" + self.current_table

        # Get the list of files in the namespace directory
        files = [f for f in os.listdir(namespace_dir) if os.path.isfile(os.path.join(namespace_dir, f))]
        
        # Sort files by last modified time (most recent first)
        files.sort(key=lambda x: os.path.getmtime(os.path.join(namespace_dir, x)), reverse=True)

        if files:
            last_file_path = os.path.join(namespace_dir, files[0])
            file_size = os.path.getsize(last_file_path)
            if file_size < 5 * 1024:
                try:
                    # Read existing data from the file
                    with open(last_file_path, 'r') as f:
                        existing_data = json.load(f)
                    # Extend the existing array with new data
                    existing_data.extend(tbl)
                    # Write the updated array back to the file
                    with open(last_file_path, 'w') as f:
                        json.dump(existing_data, f, indent=4)
                    print(f"[OK] Updated {files[0]}")
                    return
                except json.JSONDecodeError:
                    # Handle case where file content is invalid (optional: log error, create new file)
                    print(f"[Error] Invalid JSON in {files[0]}, creating new file.")
        
        # If no suitable file found, create a new one
        timestamp = int(time.time())
        new_filename = f"{self.current_table}_{timestamp}.json"
        new_file_path = os.path.join(namespace_dir, new_filename)
        with open(new_file_path, 'w') as f:
            json.dump(tbl, f,indent=4)
        print(f"[OK] Created new file: {new_filename}")


    



# test the function
db = KVEngine("D:\\ITI\\27 HBase\\Introduction to NoSQL Systems\\Labs\\database")
# print(db.list_namespaces())
# print(db.create_namespace("test2"))
print(db.use_namespace("iti"))
# print(db.list_tables("iti"))
# print(db.table_exists("courses"))
print(db.create_table("students"))