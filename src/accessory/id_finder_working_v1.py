# Internal
import time
import traceback

from pymssql import Cursor

from src.models.connector import SqlServerConnection, ConnectionType

import datetime as dt
import pymssql
import polars as pl

# # Initialize sql connection
# conn_config = SqlServerConnection(ConnectionType.NewSkies)
# connection = pymssql.connect(f"{conn_config.uri + ':' + conn_config.port}", conn_config.user,
#                                            conn_config.pwd, conn_config.database)
#


class IdLookUp:
    # Helper functions ---------------------------------------------------------------------------------------------- //
    # Get the min and max of the ID column from the table
    @staticmethod
    def get_min_max_id(target_cursor: Cursor, target_table: str, target_id_col: str):
        query = f"SELECT MIN({target_id_col}), MAX({target_id_col}) FROM {target_table}"
        target_cursor.execute(query)
        min_id, max_id = target_cursor.fetchone()
        print(f"min_id: {min_id}, max_id: {max_id}")
        return min_id, max_id

    @staticmethod
    # Get the time target for a specific ID
    def get_time_by_id(target_cursor: Cursor, target_id: str, target_table: str, target_id_col: str,
                       target_time_col: str):
        query = f"SELECT {target_id_col}, {target_time_col} FROM {target_table} WHERE {target_id_col} = %s"
        target_cursor.execute(query, str(target_id))
        result = target_cursor.fetchone()
        print(result)
        return result[0], result[1] if result else None

    # Find id based on time
    def find_id_based_on_time(self, target_cursor: Cursor, target_time, target_table, target_id_col, target_time_col,
                              direction="before"):
        min_id, max_id = self.get_min_max_id(target_cursor, target_table, target_id_col)
        current_id = (min_id + max_id) // 2  # Set Initial ID
        closest_id = None
        closest_time = None
        closest_diff = None
        search_step = (max_id - min_id) // 4  # Initial search step
        counter = 0

        while min_id <= max_id:
            counter += 1
            print("current_iter: ", counter, "id: ", current_id, " search_step: ", search_step)
            _, logged_time = self.get_time_by_id(target_cursor, current_id, target_table, target_id_col,
                                                          target_time_col)
            time.sleep(2)

            if logged_time is None:
                print(f"ID {current_id} does not exist. Adjusting search range...")
                # current_id = closest_id
                if current_id < min_id:
                    current_id = min_id
                elif current_id > max_id:
                    current_id = max_id
                continue

            # Time difference
            time_diff = abs((logged_time - target_time)).total_seconds()

            # Check if this is the closest match
            if closest_diff is None or time_diff < closest_diff:
                if (direction == 'before' and logged_time <= target_time) \
                        or (direction == 'after' and logged_time >= target_time):
                    closest_diff = time_diff
                    closest_id = current_id
                    closest_time = logged_time

            # Adjust the search direction based on the comparison
            if logged_time < target_time:
                if direction == 'before':
                    closest_id = current_id
                    closest_time = logged_time
                min_id = current_id + 1  # Move search upwards
                current_id += search_step
            elif logged_time > target_time:
                if direction == 'after':
                    closest_id = current_id
                    closest_time = logged_time
                max_id = current_id - 1  # Move search downwards
                current_id -= search_step
            else:
                return current_id, logged_time  # Exact match

            # Reduce Search step as we get close to the target
            search_step = max(1, search_step // 2)

        return closest_id, closest_time


# cursor = connection.cursor(as_dict=False)
#
#
# # Feature Declaration
# target_date = dt.datetime(year=2024, month=10, day=3)
#
# table_name = "ANAJQDW01.AnalyticsDW.Treatment"
# id_column = "TreatmentID"
# time_column = "LoggedUTC"
#
# # Run id search
# try:
#     id = find_id_based_on_time(cursor, target_date, table_name, id_column, time_column)
# except Exception as err:
#     # Check error and get me my return
#     print(traceback.format_exc())
#     # Clean up connection to prevent rate limit
#     cursor.close()
#     connection.close()
#
#
# # Close Cursor
# cursor.close()
# # Close Connection
# connection.close()
